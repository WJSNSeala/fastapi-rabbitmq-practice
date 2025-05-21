import aio_pika
import json
import asyncio

from constants import QUEUE_NAME
from database import save_message
from message_types import MessageType


class RabbitMQConsumer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.queue_name = QUEUE_NAME


    async def connect(self):
        retry_count = 0
        max_retries = 5

        while retry_count < max_retries:
            try:
                # rabbitmq연결
                self.connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")

                # channel
                self.channel = await self.connection.channel()

                # queue 선언, producer와 같은 큐를 사용하기
                self.queue = await self.channel.declare_queue(name=self.queue_name, durable=True)

                await self.channel.set_qos(prefetch_count=1)

                print("Connected to RabbitMQ")
                return True
            except Exception as e:
                retry_count += 1
                print(f"Connection attempt {retry_count} failed: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

        print("Failed to connect to RabbitMQ after multiple attempts")
        return False

    async def handle_message(self, message_type: MessageType, content: str):
        """
        메시지 타입에 따라 적절한 처리를 수행하는 핸들러 메서드 (Python 3.10+)
        """
        print(f"Processing message: Type={message_type}, Content={content}")

        match message_type:
            case MessageType.NOTIFICATION:
                # 알림 처리 로직
                print(f"Sending notification: {content}")
                await asyncio.sleep(0.5)  # 알림 발송 시뮬레이션

            case MessageType.EMAIL:
                # 이메일 처리 로직
                print(f"Sending email: {content}")
                await asyncio.sleep(1)  # 이메일 발송 시뮬레이션

            case MessageType.LOG:
                # 로그 처리 로직
                print(f"Logging: {content}")
                # 로그 기록은 빠르게 처리 가능하므로 지연 없음

            case MessageType.REPORT:
                # 보고서 생성 로직
                print(f"Generating report: {content}")
                await asyncio.sleep(3)  # 보고서 생성 시뮬레이션

            case _:
                # 기본 케이스 (알 수 없는 타입)
                print(f"Unknown message type: {message_type}")

        # 메시지 처리 완료 알림
        print(f"Message processing completed: {message_type}")



    async def process_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                body = message.body.decode()

                # json parsing
                message_data = json.loads(body)

                message_type_str = message_data["type"]
                message_content = message_data.get("content", "")

                print(f"Received message: {message_type_str}, content={message_content}")

                try:
                    message_type = MessageType(message_type_str)

                    await self.handle_message(message_type, message_content)

                    save_message(message_type, message_content)

                    print(f"Message processed: {message_type}")

                except ValueError:
                    print(f"Unknown message type: {message_type_str}")

            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")

    async def start_consuming(self):
        connected = await self.connect()
        if connected:
            # 소비 시작 후 즉시 반환 (대기 없음)
            await self.queue.consume(self.process_message)
            print("Consumer started successfully")
            return True
        return False


    async def close(self):
        try:
            if self.channel is not None and not self.channel.is_closed:
                await self.channel.close()

            if self.connection is not None and not self.connection.is_closed:
                await self.connection.close()


            # cleaning resource
            self.channel = None
            self.connection = None
            self.queue = None

            return True
        except Exception as e:
            print(f"Failed to close connection: {e}")
            return False

