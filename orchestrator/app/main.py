import asyncio
import json
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import aio_pika
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse


RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
START_QUEUE = os.getenv("START_QUEUE", "stage1")
PROGRESS_QUEUE = os.getenv("PROGRESS_QUEUE", "progress")
REPORT_QUEUE = os.getenv("REPORT_QUEUE", "orchestrator_reports")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class RunStore:
    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}
        self._recent_events: deque[dict[str, Any]] = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def register_start(self, trace_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = {
            "type": "run_started",
            "trace_id": trace_id,
            "timestamp": utc_now(),
            "message": "Orchestrator queued the initial message.",
            "payload": payload,
        }
        async with self._lock:
            self._runs[trace_id] = {
                "trace_id": trace_id,
                "status": "queued",
                "created_at": event["timestamp"],
                "completed_at": None,
                "hops": [],
                "events": [event],
                "final_report": None,
                "message": payload.get("message"),
            }
            self._recent_events.appendleft(event)
            return event

    async def record_progress(self, event: dict[str, Any]) -> None:
        trace_id = event["trace_id"]
        async with self._lock:
            run = self._runs.setdefault(
                trace_id,
                {
                    "trace_id": trace_id,
                    "status": "in_progress",
                    "created_at": event["timestamp"],
                    "completed_at": None,
                    "hops": [],
                    "events": [],
                    "final_report": None,
                    "message": None,
                },
            )
            run["status"] = "in_progress"
            hop = {
                "worker": event["worker"],
                "hostname": event["hostname"],
                "timestamp": event["timestamp"],
            }
            run["hops"].append(hop)
            run["events"].append(event)
            self._recent_events.appendleft(event)

    async def record_report(self, event: dict[str, Any]) -> None:
        trace_id = event["trace_id"]
        async with self._lock:
            run = self._runs.setdefault(
                trace_id,
                {
                    "trace_id": trace_id,
                    "status": "completed",
                    "created_at": event["timestamp"],
                    "completed_at": event["timestamp"],
                    "hops": [],
                    "events": [],
                    "final_report": None,
                    "message": None,
                },
            )
            run["status"] = "completed"
            run["completed_at"] = event["timestamp"]
            run["final_report"] = event
            run["events"].append(event)
            self._recent_events.appendleft(event)

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            runs = sorted(
                self._runs.values(),
                key=lambda item: item["created_at"],
                reverse=True,
            )
            return {
                "runs": runs,
                "events": list(self._recent_events),
            }


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


class RabbitBridge:
    def __init__(self, store: RunStore, hub: WebSocketHub) -> None:
        self.store = store
        self.hub = hub
        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._consumer_tasks: list[asyncio.Task[Any]] = []

    async def connect(self) -> None:
        while True:
            try:
                self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=25)
                await self.channel.declare_queue(START_QUEUE, durable=True)
                await self.channel.declare_queue(PROGRESS_QUEUE, durable=True)
                await self.channel.declare_queue(REPORT_QUEUE, durable=True)
                self._consumer_tasks = [
                    asyncio.create_task(self._consume(PROGRESS_QUEUE, self._handle_progress)),
                    asyncio.create_task(self._consume(REPORT_QUEUE, self._handle_report)),
                ]
                return
            except Exception as exc:
                print(f"RabbitMQ connection failed: {exc}. Retrying in 3s.")
                await asyncio.sleep(3)

    async def close(self) -> None:
        for task in self._consumer_tasks:
            task.cancel()
        if self.connection:
            await self.connection.close()

    async def _consume(self, queue_name: str, handler) -> None:
        assert self.channel is not None
        queue = await self.channel.declare_queue(queue_name, durable=True)
        async with queue.iterator() as queue_iter:
            async for incoming in queue_iter:
                async with incoming.process():
                    payload = json.loads(incoming.body.decode())
                    await handler(payload)

    async def _handle_progress(self, payload: dict[str, Any]) -> None:
        await self.store.record_progress(payload)
        await self.hub.broadcast({"type": "progress", "event": payload})

    async def _handle_report(self, payload: dict[str, Any]) -> None:
        await self.store.record_report(payload)
        await self.hub.broadcast({"type": "report", "event": payload})

    async def publish_start(self, payload: dict[str, Any]) -> None:
        assert self.channel is not None
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )
        await self.channel.default_exchange.publish(message, routing_key=START_QUEUE)


store = RunStore()
hub = WebSocketHub()
bridge = RabbitBridge(store=store, hub=hub)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await bridge.connect()
    yield
    await bridge.close()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Message Flow Orchestrator</title>
    <style>
      :root {
        --bg: #f4efe7;
        --panel: rgba(255, 251, 245, 0.92);
        --ink: #202020;
        --muted: #5b5b5b;
        --accent: #0d6b5f;
        --accent-soft: #d5efe4;
        --border: rgba(32, 32, 32, 0.12);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, #f6d8b8 0, transparent 28%),
          radial-gradient(circle at bottom right, #bddfd3 0, transparent 30%),
          linear-gradient(135deg, #f8f3ea 0%, #ece6dd 100%);
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
        border-radius: 22px;
        box-shadow: 0 18px 48px rgba(32, 32, 32, 0.08);
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
        align-items: center;
        flex-wrap: wrap;
      }
      h1 {
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        font-size: clamp(2rem, 3vw, 3.25rem);
        line-height: 0.95;
      }
      p {
        margin: 0;
        color: var(--muted);
      }
      button {
        border: 0;
        border-radius: 999px;
        padding: 14px 20px;
        font: inherit;
        font-weight: 700;
        cursor: pointer;
        background: var(--accent);
        color: white;
        transition: transform 0.18s ease, box-shadow 0.18s ease;
        box-shadow: 0 10px 24px rgba(13, 107, 95, 0.25);
      }
      button:hover { transform: translateY(-1px); }
      .hero-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 12px;
      }
      .metric {
        padding: 14px 16px;
        border-radius: 18px;
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
        font-size: 1.45rem;
      }
      .content {
        display: grid;
        grid-template-columns: 1.1fr 0.9fr;
        gap: 18px;
      }
      .panel {
        padding: 20px;
      }
      .panel h2 {
        margin: 0 0 14px;
        font-size: 1rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }
      .run-list, .event-list {
        display: grid;
        gap: 12px;
      }
      .run-card, .event-card {
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 14px;
        background: white;
      }
      .run-header, .event-header {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: baseline;
        flex-wrap: wrap;
      }
      .pill {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 12px;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--accent);
        font-size: 0.82rem;
        font-weight: 700;
      }
      .hops {
        margin-top: 12px;
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
      }
      .hop {
        min-width: 120px;
        border-radius: 14px;
        padding: 10px 12px;
        background: #f7f7f7;
      }
      .hop strong {
        display: block;
        margin-bottom: 4px;
      }
      .hop code, .run-card code, .event-card code {
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
            <h1>Internal Container Message Relay</h1>
            <p>Worker 1 -> Worker 2 -> Worker 3 -> Orchestrator, all over RabbitMQ on the internal Docker network.</p>
          </div>
          <button id="start-run">Start New Run</button>
        </div>
        <div class="hero-grid">
          <article class="metric">
            <span>Broker</span>
            <strong>RabbitMQ</strong>
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
          <h2>Runs</h2>
          <div id="runs" class="run-list"></div>
        </section>
        <section class="panel">
          <h2>Live Event Stream</h2>
          <div id="events" class="event-list"></div>
        </section>
      </section>
    </main>
    <script>
      const runsEl = document.getElementById("runs");
      const eventsEl = document.getElementById("events");
      const startButton = document.getElementById("start-run");

      const state = { runs: [], events: [] };
      let pingTimer = null;

      function fmt(ts) {
        return new Date(ts).toLocaleString();
      }

      function escapeHtml(value) {
        return String(value)
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }

      function renderRuns() {
        if (!state.runs.length) {
          runsEl.innerHTML = '<div class="placeholder">No relay has been started yet.</div>';
          return;
        }
        runsEl.innerHTML = state.runs.map((run) => {
          const hops = run.hops.map((hop) => `
            <div class="hop">
              <strong>${escapeHtml(hop.worker)}</strong>
              <code>${escapeHtml(hop.hostname)}</code>
              <div>${fmt(hop.timestamp)}</div>
            </div>
          `).join("");
          const finalReport = run.final_report
            ? `<p><strong>Final path:</strong> <code>${escapeHtml(run.final_report.path.join(" -> "))}</code></p>`
            : '<p><strong>Final path:</strong> waiting for worker3 report</p>';
          return `
            <article class="run-card">
              <div class="run-header">
                <div>
                  <strong>${escapeHtml(run.trace_id)}</strong>
                  <p>${escapeHtml(run.message || "No message body")}</p>
                </div>
                <span class="pill">${escapeHtml(run.status)}</span>
              </div>
              <p>Started ${fmt(run.created_at)}</p>
              ${finalReport}
              <div class="hops">${hops || '<div class="placeholder">Waiting for workers...</div>'}</div>
            </article>
          `;
        }).join("");
      }

      function renderEvents() {
        if (!state.events.length) {
          eventsEl.innerHTML = '<div class="placeholder">Waiting for broker events.</div>';
          return;
        }
        eventsEl.innerHTML = state.events.map((event) => {
          const workerInfo = event.worker
            ? `<p><strong>${escapeHtml(event.worker)}</strong> on <code>${escapeHtml(event.hostname)}</code></p>`
            : "";
          const pathInfo = event.path
            ? `<p><code>${escapeHtml(event.path.join(" -> "))}</code></p>`
            : "";
          return `
            <article class="event-card">
              <div class="event-header">
                <strong>${escapeHtml(event.type)}</strong>
                <span>${fmt(event.timestamp)}</span>
              </div>
              <p>Trace <code>${escapeHtml(event.trace_id)}</code></p>
              ${workerInfo}
              ${pathInfo}
              <p>${escapeHtml(event.message)}</p>
            </article>
          `;
        }).join("");
      }

      function applySnapshot(snapshot) {
        state.runs = snapshot.runs || [];
        state.events = snapshot.events || [];
        renderRuns();
        renderEvents();
      }

      function upsertRunFromEvent(event) {
        const existing = state.runs.find((run) => run.trace_id === event.trace_id);
        if (!existing) {
          state.runs.unshift({
            trace_id: event.trace_id,
            status: event.type === "run_started" ? "queued" : (event.type === "final_report" ? "completed" : "in_progress"),
            created_at: event.timestamp,
            completed_at: event.type === "final_report" ? event.timestamp : null,
            hops: [],
            events: [],
            final_report: null,
            message: event.original_message || event.payload?.message || null,
          });
        }
        const run = state.runs.find((item) => item.trace_id === event.trace_id);
        if (event.type === "worker_progress") {
          run.status = "in_progress";
          run.hops.push({
            worker: event.worker,
            hostname: event.hostname,
            timestamp: event.timestamp,
          });
        }
        if (event.type === "final_report") {
          run.status = "completed";
          run.completed_at = event.timestamp;
          run.final_report = event;
        }
        if (event.type === "run_started") {
          run.status = "queued";
          run.message = event.payload?.message || run.message;
        }
      }

      async function loadSnapshot() {
        const response = await fetch("/api/state");
        const snapshot = await response.json();
        applySnapshot(snapshot);
      }

      async function startRun() {
        startButton.disabled = true;
        try {
          await fetch("/api/start", { method: "POST" });
        } finally {
          window.setTimeout(() => {
            startButton.disabled = false;
          }, 500);
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
            applySnapshot(data.payload);
            return;
          }
          if (data.event) {
            state.events.unshift(data.event);
            state.events = state.events.slice(0, 100);
            upsertRunFromEvent(data.event);
            renderRuns();
            renderEvents();
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

      startButton.addEventListener("click", startRun);

      loadSnapshot().then(() => {
        renderRuns();
        renderEvents();
        if (!state.runs.length) {
          startRun();
        }
      });
      connect();
    </script>
  </body>
</html>
"""


@app.get("/api/state")
async def get_state() -> JSONResponse:
    return JSONResponse(await store.snapshot())


@app.post("/api/start")
async def start_run() -> JSONResponse:
    trace_id = uuid4().hex[:12]
    payload = {
        "trace_id": trace_id,
        "message": "Relay this packet across the internal Docker network.",
        "path": [],
        "created_at": utc_now(),
        "origin": "orchestrator",
    }
    event = await store.register_start(trace_id, payload)
    await hub.broadcast({"type": "run_started", "event": event})
    await bridge.publish_start(payload)
    return JSONResponse({"status": "queued", "trace_id": trace_id})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await hub.connect(websocket)
    await websocket.send_json({"type": "snapshot", "payload": await store.snapshot()})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await hub.disconnect(websocket)
