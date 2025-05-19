from typing import Optional

import aio_pika
import json

class RabbitMQProducer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None

        self.exchange_name = "message_exchange"
        self.routing_key = "message_routing_key"
        self.queue_name = "message_queue"

    async def connect(self):
        if self.is_connection_not_valid():
            self.connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
            self.channel = await self.connection.channel()

            self.exchange = await self.channel.declare_exchange(
                name=self.exchange_name,
                type=aio_pika.ExchangeType.DIRECT,
                durable=True
            )

            # declare queue
            self.queue = await self.channel.declare_queue(
                name=self.queue_name,
                durable=True,
            )

            # exchange queue binding
            await self.queue.bind(
                exchange=self.exchange,
                routing_key=self.routing_key,
            )

    async def publish_message(self, message_type: str, message_content: Optional[str] = None):
        try:
            if self.is_connection_not_valid():
                await self.connect()

            message = {
                "type": message_type,
                "content": message_content or "",
            }

            # publish message
            await self.exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type='application/json',
                ),
                routing_key=self.routing_key,
            )

            return True

        except Exception as e:
            print(f"publish_message: {e}")
            return False

    async def close(self):
        if not self.is_connection_not_valid():
            await self.connection.close()

    def is_connection_not_valid(self):
        return self.connection is None or (hasattr(self.connection, 'is_closed') and self.connection.is_closed)