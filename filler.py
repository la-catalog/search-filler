from aio_pika.abc import DeliveryMode
from aio_pika.exchange import Exchange
from aio_pika.message import Message
from page_infra import Infra
from rabbit_models.search_scraper import Body, Metadata
from structlog.stdlib import BoundLogger, get_logger
from url_builder import Builder


class Filler:
    def __init__(self, mongo_url: str, logger: BoundLogger = get_logger()):
        self._builder = Builder(logger=logger)
        self._infra = Infra(mongo_url=mongo_url, logger=logger)

    async def setup(self):
        await self._infra.setup_search_database()

    async def fill(self, exchange: Exchange, queue: str, marketplace: str):
        messages: list[str] = []
        cursor = await self._infra.get_queries(marketplace=marketplace)

        async for doc in cursor:
            url = self._builder.build_query_url(
                query=doc["query"],
                marketplace=marketplace,
            )

            body = Body(
                url=url,
                marketplace=marketplace,
                metadata=Metadata(query=doc["query"], source="FILLER"),
            )

            messages.append(body.json())

            if len(messages) >= 1000:
                await self._publish(exchange=exchange, queue=queue, messages=messages)
                messages.clear()

        if messages:
            await self._publish(exchange=exchange, queue=queue, messages=messages)

    async def _publish(
        self, exchange: Exchange, queue: str, messages: list[str]
    ) -> None:
        for m in messages:
            m = Message(
                body=m.encode("UTF-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
            )

            await exchange.publish(message=m, routing_key=queue)
