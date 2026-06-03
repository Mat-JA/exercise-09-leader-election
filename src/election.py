"""
Bully Algorithm — Leader Election
----------------------------------
Each node has a numeric NODE_ID (1, 2, 3).
The node with the highest ID that is alive becomes the leader.

Protocol messages (HTTP POST):
  POST /election      — "I'm starting an election; are you alive?"
  POST /coordinator   — "I won; I'm the new leader."

Flow:
  1. A node that suspects the leader is dead calls start_election().
  2. It sends ELECTION to every peer with a higher ID.
  3. If any higher peer replies OK (HTTP 200) it means a stronger node is
     taking over → wait for a COORDINATOR message.
  4. If nobody replies within ELECTION_TIMEOUT the caller is the highest
     alive node → it calls declare_victory().
  5. declare_victory() sets itself as leader and broadcasts COORDINATOR to
     all peers so they update their local state.
  6. heartbeat_check() runs in a background thread: every HEARTBEAT_INTERVAL
     seconds it pings the current leader; if the ping fails it resets the
     leader and starts a new election.
"""

import os
import threading
import time
from urllib.parse import urlparse

import requests

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
NODE_ID: int = int(os.environ.get("NODE_ID", "1"))
_peers_raw: str = os.environ.get("PEERS", "")
PEERS: list[str] = [p.strip() for p in _peers_raw.split(",") if p.strip()]

ELECTION_TIMEOUT: float = 3.0   # seconds to wait for OK replies
HEARTBEAT_INTERVAL: float = 5.0  # seconds between leader pings

# ---------------------------------------------------------------------------
# Shared mutable state  (protected by _lock where needed)
# ---------------------------------------------------------------------------
leader_id: int | None = None
election_in_progress: bool = False
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _peer_node_id(url: str) -> int:
    """Extract the numeric ID from a peer URL (e.g. http://node-2:8080 → 2)."""
    hostname = urlparse(url).hostname  # 'node-2'
    return int(hostname.split("-")[-1])


def _higher_peers() -> list[str]:
    """Return URLs of peers whose ID is strictly greater than ours."""
    return [p for p in PEERS if _peer_node_id(p) > NODE_ID]


def _post(url: str, payload: dict, timeout: float = 2.0) -> bool:
    """Send a POST and return True on success, False on any error."""
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        return r.status_code == 200
    except requests.RequestException:
        return False


# ---------------------------------------------------------------------------
# Public API (called by FastAPI endpoints and by the background thread)
# ---------------------------------------------------------------------------

def start_election() -> None:
    """
    Initiate a Bully election.

    Sends ELECTION to every peer with a higher ID.
    - If at least one replies OK → a stronger node is alive; wait for its
      COORDINATOR announcement (reset flag after a grace period).
    - If none reply → we are the strongest alive node; declare victory.
    """
    global election_in_progress

    with _lock:
        if election_in_progress:
            return
        election_in_progress = True

    higher = _higher_peers()
    got_ok = False

    for peer in higher:
        ok = _post(f"{peer}/election", {"sender_id": NODE_ID},
                   timeout=ELECTION_TIMEOUT)
        if ok:
            got_ok = True

    if not got_ok:
        # No higher node responded — we win.
        declare_victory()
    else:
        # A higher node is alive and will declare itself leader.
        # Reset our flag after a reasonable wait so we can react if it
        # never sends COORDINATOR (e.g. it also crashed right after).
        def _reset_flag() -> None:
            global election_in_progress
            time.sleep(ELECTION_TIMEOUT * 3)
            with _lock:
                election_in_progress = False

        threading.Thread(target=_reset_flag, daemon=True).start()


def handle_election_message(sender_id: int) -> None:
    """
    React to an ELECTION message received from a lower-ID node.

    The HTTP endpoint already returns 200 (OK) before calling this function,
    which satisfies the protocol handshake.  Here we kick off our own
    election in the background so the highest surviving node eventually wins.
    """
    # Start our own election asynchronously (start_election is idempotent
    # while election_in_progress is True).
    threading.Thread(target=start_election, daemon=True).start()


def declare_victory() -> None:
    """
    Announce that this node is the new leader.

    Sets the local leader state and broadcasts COORDINATOR to every peer.
    """
    global leader_id, election_in_progress

    with _lock:
        leader_id = NODE_ID
        election_in_progress = False

    for peer in PEERS:
        _post(f"{peer}/coordinator", {"leader_id": NODE_ID})


def heartbeat_check() -> None:
    """
    Background loop: ping the current leader periodically.

    Runs forever in a daemon thread.  If the leader does not respond,
    resets the leader state and starts a new election.
    """
    global leader_id

    # Brief initial delay so all nodes finish booting before the first
    # election fires.
    time.sleep(ELECTION_TIMEOUT)
    start_election()   # Elect a leader on startup.

    while True:
        time.sleep(HEARTBEAT_INTERVAL)

        current_leader = leader_id

        if current_leader is None:
            # No known leader — start an election.
            threading.Thread(target=start_election, daemon=True).start()
            continue

        if current_leader == NODE_ID:
            # We are the leader; nothing to check.
            continue

        # Find the URL for the current leader.
        leader_url: str | None = next(
            (p for p in PEERS if _peer_node_id(p) == current_leader), None
        )
        if leader_url is None:
            continue

        # Ping the leader's health endpoint.
        try:
            r = requests.get(f"{leader_url}/health", timeout=2.0)
            alive = r.status_code == 200
        except requests.RequestException:
            alive = False

        if not alive:
            with _lock:
                leader_id = None
            threading.Thread(target=start_election, daemon=True).start()