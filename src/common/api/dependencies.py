from typing import Generator

from aiohttp import ClientSession, ClientTimeout
from asyncpg import Connection

from common.db.db import DatabaseProvider


async def get_client_session() -> Generator[ClientSession, None, None]:
    timeout = ClientTimeout(
        total=5,
        connect=5,
        sock_read=5,
        sock_connect=5,
        ceil_threshold=5
    )
    # async with ClientSession(timeout=timeout) as session:
    async with ClientSession() as session:
        yield session


async def get_db_connection() -> Generator[Connection, None, None]:
    pool = await DatabaseProvider.get_pool()
    async with pool.acquire() as connection:
        yield connection
