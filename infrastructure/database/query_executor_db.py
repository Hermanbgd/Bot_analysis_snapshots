import asyncio
import logging
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from config.config import Config, load_config
from infrastructure.database.connection import get_pg_connection

# Загрузка конфигурации
config: Config = load_config()

logging.basicConfig(
    level=logging.getLevelName(config.log.level),
    format=config.log.format,
)
logger = logging.getLogger(__name__)


async def execute_scalar_query(sql_query: str) -> Any:
    """
    Выполняет SQL-запрос, который возвращает ровно одно значение (одно число).

    Args:
        sql_query (str): Валидный SQL-запрос, возвращающий одну строку и один столбец.

    Returns:
        int | float | None: Одно число из результата запроса.

    Raises:
        Exception: Если запрос вернул не одно значение или произошла ошибка.
    """
    connection: AsyncConnection | None = None
    try:
        connection = await get_pg_connection(
            db_name=config.db.name,
            host=config.db.host,
            port=config.db.port,
            user=config.db.user,
            password=config.db.password,
        )

        async with connection:
            async with connection.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql_query)
                result = await cur.fetchone()

                if result is None:
                    logger.warning("Запрос не вернул данных: %s", sql_query.strip())
                    return None

                if len(result) != 1:
                    raise ValueError(f"Запрос вернул больше одного столбца: {len(result)}")

                value = list(result.values())[0]

                if not isinstance(value, (int, float)):
                    logger.warning("Результат не является числом: %s (тип: %s)", value, type(value))

                return value

    except Exception as e:
        logger.error("Ошибка при выполнении запроса: %s", e)
        logger.debug("SQL: %s", sql_query)
        raise
    finally:
        if connection and not connection.closed:
            await connection.close()
            logger.debug("Подключение к БД закрыто")
