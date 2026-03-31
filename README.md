# Docker Compose Relay Demo

This project starts:

- `rabbitmq` on an internal Docker network
- `worker1`, `worker2`, and `worker3` on the same internal network
- `orchestrator`, which joins both the internal network and `dokploy-network`

The flow is:

1. The orchestrator publishes a message to `stage1`.
2. `worker1` appends its hostname and forwards to `stage2`.
3. `worker2` appends its hostname and forwards to `stage3`.
4. `worker3` appends its hostname and sends the final report back to the orchestrator.
5. The orchestrator pushes progress to the browser over WebSockets.

## Run

If `dokploy-network` does not already exist locally:

```bash
docker network create dokploy-network
```

Start the stack:

```bash
docker compose up --build
```

Open [http://localhost:8080](http://localhost:8080).
