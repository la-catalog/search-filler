import asyncio
import os

from aio_pika import Connection, connect
from page_infra.options import get_marketplace_infra
from structlog.stdlib import get_logger

from filler import Filler

filler = Filler(mongo_url=os.environ["MONGO_URL"])
logger = get_logger().bind(deployment="search-fill")


async def fill_queue(connection: Connection, marketplace: str) -> None:
    infra = get_marketplace_infra(marketplace=marketplace, logger=logger)
    channel = await connection.channel()

    async with channel:
        channel.publisher_confirms()
        exchange = channel.default_exchange
        queue = await channel.declare_queue(
            name=infra.search_queue, durable=True, arguments={"x-max-priority": 10}
        )

        await filler.fill(exchange=exchange, marketplace=marketplace)


async def main():
    await filler.setup()

    connection = await connect(os.environ["RABBIT_URL"])

    async with connection:
        await asyncio.gather(
            fill_queue(connection=connection, marketplace="mercado_livre"),
            fill_queue(connection=connection, marketplace="rihappy"),
        )


asyncio.run(main())
