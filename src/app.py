import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Response
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate
from src import election

Base.metadata.create_all(bind=engine)


# ---------------------------------------------------------------------------
# Lifespan: start the heartbeat / election background thread on startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=election.heartbeat_check, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Health (modificado para push de intento)
# ---------------------------------------------------------------------------

@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}


# ---------------------------------------------------------------------------
# Node registry CRUD
# ---------------------------------------------------------------------------

@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Election endpoints  (Bully protocol messages)
# ---------------------------------------------------------------------------

class ElectionMessage(BaseModel):
    sender_id: int


class CoordinatorMessage(BaseModel):
    leader_id: int


@app.post("/election")
def receive_election(msg: ElectionMessage):
    """
    Receive an ELECTION message from a lower-ID node.
    Returning 200 acts as the OK acknowledgement defined by the Bully protocol.
    We then start our own election in the background.
    """
    election.handle_election_message(msg.sender_id)
    return {"ok": True, "node_id": election.NODE_ID}


@app.post("/coordinator")
def receive_coordinator(msg: CoordinatorMessage):
    """
    Receive a COORDINATOR message — update local leader state.
    """
    election.leader_id = msg.leader_id
    election.election_in_progress = False
    return {"ok": True, "leader_id": election.leader_id}


@app.get("/leader")
def get_leader():
    """Return the currently known leader and this node's own ID."""
    return {
        "node_id": election.NODE_ID,
        "leader_id": election.leader_id,
        "is_leader": election.leader_id == election.NODE_ID,
    }