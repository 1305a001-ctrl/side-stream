"""Service entry point — runs signal_pusher + trigger_engine + FastAPI together."""
from __future__ import annotations

import asyncio
import logging
import signal as os_signal

import uvicorn

from side_stream import signal_pusher, trigger_engine
from side_stream.api.app import app as fastapi_app
from side_stream.settings import settings

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("side_stream.main")


async def main_async() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info("main.starting brand=%s port=%d", settings.brand_name, settings.http_port)

    config = uvicorn.Config(
        fastapi_app,
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )
    server = uvicorn.Server(config)

    tasks = [
        signal_pusher.run(stop),
        trigger_engine.run(stop),
        server.serve(),
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("main.stopped")


def main() -> int:
    asyncio.run(main_async())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
