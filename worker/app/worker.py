import asyncio
import json
import os
import socket
from datetime import UTC, datetime

import aio_pika


RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
WORKER_NAME = os.getenv("WORKER_NAME", "worker")
INPUT_QUEUE = os.getenv("INPUT_QUEUE", "stage1")
OUTPUT_QUEUE = os.getenv("OUTPUT_QUEUE", "")
PROGRESS_QUEUE = os.getenv("PROGRESS_QUEUE", "progress")
REPORT_QUEUE = os.getenv("REPORT_QUEUE", "orchestrator_reports")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


async def connect() -> tuple[aio_pika.RobustConnection, aio_pika.abc.AbstractRobustChannel]:
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            return connection, channel
        except Exception as exc:
            print(f"{WORKER_NAME}: unable to connect to RabbitMQ: {exc}. Retrying in 3s.")
            await asyncio.sleep(3)


async def publish(channel: aio_pika.abc.AbstractRobustChannel, routing_key: str, payload: dict) -> None:
    message = aio_pika.Message(
        body=json.dumps(payload).encode(),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await channel.default_exchange.publish(message, routing_key=routing_key)


async def main() -> None:
    hostname = socket.gethostname()
    connection, channel = await connect()

    await channel.declare_queue(INPUT_QUEUE, durable=True)
    await channel.declare_queue(PROGRESS_QUEUE, durable=True)
    await channel.declare_queue(REPORT_QUEUE, durable=True)
    if OUTPUT_QUEUE:
        await channel.declare_queue(OUTPUT_QUEUE, durable=True)

    queue = await channel.declare_queue(INPUT_QUEUE, durable=True)
    print(f"{WORKER_NAME}: consuming from {INPUT_QUEUE} as {hostname}")
    await publish(
        channel,
        PROGRESS_QUEUE,
        {
            "type": "worker_ready",
            "trace_id": f"{WORKER_NAME}-ready",
            "worker": WORKER_NAME,
            "hostname": hostname,
            "timestamp": utc_now(),
            "path": [],
            "message": f"{WORKER_NAME} is connected to RabbitMQ and waiting on {INPUT_QUEUE}.",
        },
    )

    async with queue.iterator() as queue_iter:
        async for incoming in queue_iter:
            async with incoming.process():
                payload = json.loads(incoming.body.decode())
                print(f"{WORKER_NAME}: received trace {payload.get('trace_id')} from {INPUT_QUEUE}")
                payload.setdefault("path", [])
                payload["path"].append(hostname)
                payload["last_worker"] = WORKER_NAME
                payload["updated_at"] = utc_now()

                progress_event = {
                    "type": "worker_progress",
                    "trace_id": payload["trace_id"],
                    "worker": WORKER_NAME,
                    "hostname": hostname,
                    "timestamp": utc_now(),
                    "path": list(payload["path"]),
                    "message": f"{WORKER_NAME} processed the message and appended its hostname.",
                    "original_message": payload.get("message"),
                }
                await publish(channel, PROGRESS_QUEUE, progress_event)

                if OUTPUT_QUEUE:
                    await publish(channel, OUTPUT_QUEUE, payload)
                    print(f"{WORKER_NAME}: forwarded trace {payload['trace_id']} to {OUTPUT_QUEUE}")
                else:
                    final_report = {
                        "type": "final_report",
                        "trace_id": payload["trace_id"],
                        "worker": WORKER_NAME,
                        "hostname": hostname,
                        "timestamp": utc_now(),
                        "path": list(payload["path"]),
                        "message": "worker3 delivered the final report to the orchestrator.",
                        "original_message": payload.get("message"),
                    }
                    await publish(channel, REPORT_QUEUE, final_report)
                    print(f"{WORKER_NAME}: reported completion for trace {payload['trace_id']}")

    await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
