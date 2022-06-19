from aio_pika.exchange import Exchange
from aio_pika.message import Message
from page_infra import Infra
from rabbit_models.search_scraper import Body
from structlog.stdlib import BoundLogger, get_logger


class Filler:
    def __init__(self, mongo_url: str, logger: BoundLogger = get_logger()):
        self._mongo_url = mongo_url
        self._infra = Infra(mongo_url=mongo_url, logger=logger)

    async def setup(self):
        await self._infra.setup_search_database()

    async def fill(self, exchange: Exchange, marketplace: str):
        messages = []
        cursor = await self._infra.get_queries(marketplace=marketplace)

        async for doc in cursor:
            Body()
            print(doc)

        exchange.publish()
