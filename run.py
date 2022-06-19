import asyncio
import os

from aio_pika import connect

from filler import Filler

filler = Filler(mongo_url=os.environ["MONGO_URL"])


async def main():
    await filler.setup()

    connection = await connect(os.environ["RABBIT_URL"])

    async with connection:
        channel = await connection.channel()

        async with channel:
            await asyncio.gather(
                filler.fill(channel=channel, marketplace="mercado_livre"),
                filler.fill(channel=channel, marketplace="rihappy"),
            )


asyncio.run(main())
