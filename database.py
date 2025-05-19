# app/database.py
import sqlite3
from typing import Optional

# 글로벌 데이터베이스 연결 객체
db_connection: Optional[sqlite3.Connection] = None


def get_db():
    """애플리케이션 전체에서 사용할 데이터베이스 연결을 반환합니다."""
    global db_connection
    if db_connection is None:
        db_connection = sqlite3.connect("fastapi_rabbitmq.db")
        db_connection.row_factory = sqlite3.Row
    return db_connection


def init_db():
    """데이터베이스 초기화 및 테이블 생성"""
    conn = get_db()
    cursor = conn.cursor()

    # messages 테이블 생성
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_type TEXT NOT NULL,
        message_content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    print("Database initialized successfully")


def close_db():
    """데이터베이스 연결을 닫습니다."""
    global db_connection
    if db_connection is not None:
        db_connection.close()
        db_connection = None


def save_message(message_type, message_content):
    """메시지를 데이터베이스에 저장"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO messages (message_type, message_content) VALUES (?, ?)",
        (message_type, message_content)
    )

    conn.commit()
    return True