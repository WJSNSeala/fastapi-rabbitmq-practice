from fastapi import FastAPI
from contextlib import asynccontextmanager

from database import init_db, close_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_db()
        yield
    except Exception as e:
        print(e)
        raise
    finally:
        close_db()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
