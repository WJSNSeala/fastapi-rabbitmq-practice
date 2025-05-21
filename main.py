import asyncio
from typing import Optional, Set

from fastapi import FastAPI
from contextlib import asynccontextmanager

from fastapi.params import Query

from consumer import RabbitMQConsumer
from database import init_db, close_db
from message_types import MessageType
from producer import RabbitMQProducer

producer = RabbitMQProducer()
consumer = RabbitMQConsumer()

background_tasks: Set[asyncio.Task] = set()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_db()

        await producer.connect()

        async def consume_messages():
            try:
                # start consumer connect
                await consumer.connect()
                print('consumer connected to RabbitMQ')

                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                print('consumer connect cancelled from RabbitMQ')
                await consumer.close()
            except Exception as e:
                print(f"Error in consumer task: {e}")
                # 비정상 종료 시에도 리소스 정리 시도
                await consumer.close()

        #
        consumer_task = asyncio.create_task(consume_messages())
        background_tasks.add(consumer_task)

        consumer_task.add_done_callback(background_tasks.discard)

        print('fastapi with rabbitmq started')

        yield
    except Exception as e:
        print(e)
        raise
    finally:
        print("Shutting down background tasks...")
        for task in background_tasks:
            task.cancel()

        # 모든 태스크가 완료될 때까지 대기 (최대 5초)
        if background_tasks:
            await asyncio.wait(background_tasks, timeout=5)

        # 생산자 연결 종료
        await producer.close()
        print("Producer connection closed")

        # 데이터베이스 연결 종료
        close_db()
        print("Database connection closed")

        print("FastAPI application shutdown complete")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}

@app.post("/messages")
async def send_message(
    message_type: MessageType = Query(..., description="Message type"),
    content: Optional[str] = Query(None, description="Message content"),
):
    # publish message to RabbitMQ
    success = await producer.publish_message(message_type, content)

    if success:
        return {"status": "success"}
    else:
        return {"status": "failure"}