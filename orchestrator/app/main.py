import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from redis.asyncio import Redis


REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
COUNTER_KEY = "ui:counter"
MODE_KEY = "ui:mode"
NOTES_KEY = "ui:notes"
EVENTS_KEY = "ui:events"


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class NotePayload(BaseModel):
    text: str


class WebSocketHub:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._clients.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(websocket)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        stale: list[WebSocket] = []
        async with self._lock:
            for client in self._clients:
                try:
                    await client.send_json(payload)
                except Exception:
                    stale.append(client)
            for client in stale:
                self._clients.discard(client)


class RedisStore:
    def __init__(self) -> None:
        self.redis: Redis | None = None

    async def connect(self) -> None:
        while True:
            try:
                self.redis = Redis.from_url(REDIS_URL, decode_responses=True)
                await self.redis.ping()
                await self._seed_defaults()
                return
            except Exception as exc:
                print(f"Redis connection failed: {exc}. Retrying in 3s.")
                await asyncio.sleep(3)

    async def close(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()

    async def _seed_defaults(self) -> None:
        assert self.redis is not None
        pipe = self.redis.pipeline()
        pipe.setnx(COUNTER_KEY, 0)
        pipe.setnx(MODE_KEY, "idle")
        await pipe.execute()

    async def snapshot(self) -> dict[str, Any]:
        assert self.redis is not None
        pipe = self.redis.pipeline()
        pipe.get(COUNTER_KEY)
        pipe.get(MODE_KEY)
        pipe.lrange(NOTES_KEY, 0, 19)
        pipe.lrange(EVENTS_KEY, 0, 19)
        counter, mode, notes, events = await pipe.execute()
        parsed_events = [json.loads(event) for event in events]
        return {
            "counter": int(counter or 0),
            "mode": mode or "idle",
            "notes": [json.loads(note) for note in notes],
            "events": parsed_events,
        }

    async def increment_counter(self) -> dict[str, Any]:
        assert self.redis is not None
        value = await self.redis.incr(COUNTER_KEY)
        event = {
            "type": "counter_incremented",
            "timestamp": utc_now(),
            "message": f"Counter incremented to {value}.",
        }
        await self._record_event(event)
        return await self.snapshot()

    async def toggle_mode(self) -> dict[str, Any]:
        assert self.redis is not None
        current = await self.redis.get(MODE_KEY)
        next_mode = "armed" if current != "armed" else "idle"
        await self.redis.set(MODE_KEY, next_mode)
        event = {
            "type": "mode_toggled",
            "timestamp": utc_now(),
            "message": f"Mode switched to {next_mode}.",
        }
        await self._record_event(event)
        return await self.snapshot()

    async def save_note(self, text: str) -> dict[str, Any]:
        assert self.redis is not None
        clean_text = text.strip()
        if not clean_text:
            raise ValueError("Note text cannot be empty.")
        note = {
            "text": clean_text,
            "timestamp": utc_now(),
        }
        await self.redis.lpush(NOTES_KEY, json.dumps(note))
        await self.redis.ltrim(NOTES_KEY, 0, 19)
        event = {
            "type": "note_saved",
            "timestamp": utc_now(),
            "message": f"Saved note: {clean_text}",
        }
        await self._record_event(event)
        return await self.snapshot()

    async def reset(self) -> dict[str, Any]:
        assert self.redis is not None
        await self.redis.delete(COUNTER_KEY, MODE_KEY, NOTES_KEY, EVENTS_KEY)
        await self._seed_defaults()
        event = {
            "type": "store_reset",
            "timestamp": utc_now(),
            "message": "Redis-backed UI state was reset.",
        }
        await self._record_event(event)
        return await self.snapshot()

    async def _record_event(self, event: dict[str, Any]) -> None:
        assert self.redis is not None
        await self.redis.lpush(EVENTS_KEY, json.dumps(event))
        await self.redis.ltrim(EVENTS_KEY, 0, 19)


hub = WebSocketHub()
store = RedisStore()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await store.connect()
    yield
    await store.close()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Redis Button Board</title>
    <style>
      :root {
        --bg: #f3efe7;
        --panel: rgba(255, 253, 247, 0.92);
        --ink: #1f1f1f;
        --muted: #626262;
        --accent: #bf5b2c;
        --accent-soft: #fde1d3;
        --border: rgba(31, 31, 31, 0.12);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, #f8dcb6 0, transparent 26%),
          radial-gradient(circle at bottom right, #d6e8d3 0, transparent 28%),
          linear-gradient(135deg, #f7f2e9 0%, #ebe4db 100%);
      }
      main {
        width: min(1120px, calc(100vw - 32px));
        margin: 32px auto;
        display: grid;
        gap: 18px;
      }
      .hero, .panel {
        background: var(--panel);
        backdrop-filter: blur(12px);
        border: 1px solid var(--border);
        border-radius: 24px;
        box-shadow: 0 18px 48px rgba(31, 31, 31, 0.08);
      }
      .hero {
        padding: 28px;
        display: grid;
        gap: 18px;
      }
      .hero-top {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: flex-start;
        flex-wrap: wrap;
      }
      h1 {
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        font-size: clamp(2rem, 3vw, 3.3rem);
        line-height: 0.95;
      }
      p {
        margin: 0;
        color: var(--muted);
      }
      .hero-grid, .content {
        display: grid;
        gap: 18px;
      }
      .hero-grid {
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      }
      .content {
        grid-template-columns: 1.1fr 0.9fr;
      }
      .metric, .panel {
        padding: 20px;
      }
      .metric {
        border-radius: 20px;
        background: white;
        border: 1px solid var(--border);
      }
      .metric span {
        display: block;
        color: var(--muted);
        font-size: 0.85rem;
        margin-bottom: 6px;
      }
      .metric strong {
        font-size: 1.6rem;
      }
      .panel h2 {
        margin: 0 0 14px;
        font-size: 1rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }
      .action-grid, .list-grid {
        display: grid;
        gap: 12px;
      }
      .action-grid {
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      }
      .button {
        border: 0;
        border-radius: 18px;
        padding: 16px;
        background: var(--accent);
        color: white;
        cursor: pointer;
        font: inherit;
        font-weight: 700;
        box-shadow: 0 10px 24px rgba(191, 91, 44, 0.24);
      }
      .button.secondary {
        background: #234d44;
        box-shadow: 0 10px 24px rgba(35, 77, 68, 0.22);
      }
      .button.ghost {
        background: #ffffff;
        color: var(--ink);
        border: 1px solid var(--border);
        box-shadow: none;
      }
      textarea {
        width: 100%;
        min-height: 110px;
        resize: vertical;
        border-radius: 18px;
        border: 1px solid var(--border);
        padding: 14px;
        font: inherit;
        background: #fff;
      }
      .list-card {
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 14px;
        background: white;
      }
      .list-card code {
        font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
        font-size: 0.83rem;
      }
      .placeholder {
        padding: 22px;
        border: 1px dashed var(--border);
        border-radius: 18px;
        color: var(--muted);
        text-align: center;
      }
      @media (max-width: 860px) {
        .content { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <div class="hero-top">
          <div>
            <h1>Redis Button Board</h1>
            <p>The web UI lives in the orchestrator container. Every action writes state into Redis on the second container.</p>
          </div>
        </div>
        <div class="hero-grid">
          <article class="metric">
            <span>Data Store</span>
            <strong>Redis</strong>
          </article>
          <article class="metric">
            <span>Realtime Channel</span>
            <strong>WebSocket</strong>
          </article>
          <article class="metric">
            <span>External Network</span>
            <strong>dokploy-network</strong>
          </article>
        </div>
      </section>
      <section class="content">
        <section class="panel">
          <h2>Actions</h2>
          <div class="action-grid">
            <button id="increment-button" class="button">Increment Counter</button>
            <button id="toggle-button" class="button secondary">Toggle Mode</button>
            <button id="reset-button" class="button ghost">Reset Store</button>
          </div>
          <div style="margin-top: 14px;">
            <textarea id="note-text" placeholder="Write a note that will be stored in Redis..."></textarea>
          </div>
          <div style="margin-top: 14px;">
            <button id="save-note-button" class="button">Save Note</button>
          </div>
        </section>
        <section class="panel">
          <h2>Current State</h2>
          <div class="list-grid">
            <article class="list-card">
              <strong>Counter</strong>
              <p id="counter-value">0</p>
            </article>
            <article class="list-card">
              <strong>Mode</strong>
              <p id="mode-value">idle</p>
            </article>
          </div>
        </section>
        <section class="panel">
          <h2>Stored Notes</h2>
          <div id="notes" class="list-grid"></div>
        </section>
        <section class="panel">
          <h2>Activity Log</h2>
          <div id="events" class="list-grid"></div>
        </section>
      </section>
    </main>
    <script>
      const incrementButton = document.getElementById("increment-button");
      const toggleButton = document.getElementById("toggle-button");
      const resetButton = document.getElementById("reset-button");
      const saveNoteButton = document.getElementById("save-note-button");
      const noteText = document.getElementById("note-text");
      const counterValue = document.getElementById("counter-value");
      const modeValue = document.getElementById("mode-value");
      const notesEl = document.getElementById("notes");
      const eventsEl = document.getElementById("events");

      let pingTimer = null;

      function escapeHtml(value) {
        return String(value)
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }

      function fmt(ts) {
        return new Date(ts).toLocaleString();
      }

      function render(snapshot) {
        counterValue.textContent = snapshot.counter;
        modeValue.textContent = snapshot.mode;

        if (!snapshot.notes.length) {
          notesEl.innerHTML = '<div class="placeholder">No notes stored in Redis yet.</div>';
        } else {
          notesEl.innerHTML = snapshot.notes.map((note) => `
            <article class="list-card">
              <p>${escapeHtml(note.text)}</p>
              <code>${fmt(note.timestamp)}</code>
            </article>
          `).join("");
        }

        if (!snapshot.events.length) {
          eventsEl.innerHTML = '<div class="placeholder">No Redis write activity yet.</div>';
        } else {
          eventsEl.innerHTML = snapshot.events.map((event) => `
            <article class="list-card">
              <strong>${escapeHtml(event.type)}</strong>
              <p>${escapeHtml(event.message)}</p>
              <code>${fmt(event.timestamp)}</code>
            </article>
          `).join("");
        }
      }

      async function fetchState() {
        const response = await fetch("/api/state");
        render(await response.json());
      }

      async function postJson(url, payload) {
        const response = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        if (!response.ok) {
          const error = await response.json();
          window.alert(error.detail || "Request failed.");
        }
      }

      async function post(url) {
        const response = await fetch(url, { method: "POST" });
        if (!response.ok) {
          const error = await response.json();
          window.alert(error.detail || "Request failed.");
        }
      }

      function connect() {
        const protocol = location.protocol === "https:" ? "wss" : "ws";
        const socket = new WebSocket(`${protocol}://${location.host}/ws`);
        socket.addEventListener("open", () => {
          if (pingTimer) {
            window.clearInterval(pingTimer);
          }
          pingTimer = window.setInterval(() => {
            if (socket.readyState === WebSocket.OPEN) {
              socket.send("ping");
            }
          }, 20000);
        });
        socket.addEventListener("message", (raw) => {
          const data = JSON.parse(raw.data);
          if (data.type === "snapshot") {
            render(data.payload);
          }
        });
        socket.addEventListener("close", () => {
          if (pingTimer) {
            window.clearInterval(pingTimer);
            pingTimer = null;
          }
          window.setTimeout(connect, 1500);
        });
      }

      incrementButton.addEventListener("click", () => post("/api/actions/increment"));
      toggleButton.addEventListener("click", () => post("/api/actions/toggle"));
      resetButton.addEventListener("click", () => post("/api/actions/reset"));
      saveNoteButton.addEventListener("click", async () => {
        await postJson("/api/notes", { text: noteText.value });
        noteText.value = "";
      });

      fetchState();
      connect();
    </script>
  </body>
</html>
"""


@app.get("/api/state")
async def get_state() -> JSONResponse:
    return JSONResponse(await store.snapshot())


@app.post("/api/actions/increment")
async def increment_counter() -> JSONResponse:
    snapshot = await store.increment_counter()
    await hub.broadcast({"type": "snapshot", "payload": snapshot})
    return JSONResponse(snapshot)


@app.post("/api/actions/toggle")
async def toggle_mode() -> JSONResponse:
    snapshot = await store.toggle_mode()
    await hub.broadcast({"type": "snapshot", "payload": snapshot})
    return JSONResponse(snapshot)


@app.post("/api/actions/reset")
async def reset_store() -> JSONResponse:
    snapshot = await store.reset()
    await hub.broadcast({"type": "snapshot", "payload": snapshot})
    return JSONResponse(snapshot)


@app.post("/api/notes")
async def save_note(payload: NotePayload) -> JSONResponse:
    try:
        snapshot = await store.save_note(payload.text)
    except ValueError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    await hub.broadcast({"type": "snapshot", "payload": snapshot})
    return JSONResponse(snapshot)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await hub.connect(websocket)
    await websocket.send_json({"type": "snapshot", "payload": await store.snapshot()})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await hub.disconnect(websocket)
