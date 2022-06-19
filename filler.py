from aio_pika.channel import Channel
from page_infra import Infra
from structlog.stdlib import BoundLogger, get_logger


class Filler:
    def __init__(self, mongo_url: str, logger: BoundLogger = get_logger()):
        self._mongo_url = mongo_url
        self._infra = Infra(mongo_url=mongo_url, logger=logger)

    async def setup(self):
        await self._infra.setup_search_database()

    async def fill(self, channel: Channel, marketplace: str):
        cursor = await self._infra.get_queries(marketplace=marketplace)

        async for doc in cursor:
            print(doc)
