# Redis Button Board

This project now starts only two containers:

- `redis` on an internal Docker network
- `orchestrator`, which joins both the internal network and `dokploy-network`

The web UI runs from the orchestrator container. Clicking UI buttons writes state into Redis on the other container:

1. `Increment Counter` updates a Redis counter.
2. `Toggle Mode` flips a Redis-backed mode value.
3. `Save Note` stores a note in a Redis list.
4. `Reset Store` clears the Redis-backed UI state.

The orchestrator pushes updated state to connected browsers over WebSockets so every open page reflects the current Redis contents.

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
