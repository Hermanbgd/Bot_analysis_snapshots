import logging

from aiogram import Router, F
from aiogram.types import Message

from bot.services.llm import get_sql_query
from infrastructure.database.query_executor_db import execute_scalar_query

query_router = Router()
logger = logging.getLogger(__name__)


@query_router.message(F.text, ~F.command)
async def handle_text_query(message: Message):
    user_query = message.text.strip()

    try:
        # Генерация SQL через LLM
        sql = await get_sql_query(user_query)
        logger.info("Сгенерирован SQL для '%s': %s", user_query, sql)

        # Выполнение запроса
        result = await execute_scalar_query(sql)

        if result is None:
            answer = 0
        else:
            answer = result

        await message.answer(answer)

    except Exception as e:
        logger.error("Ошибка при обработке запроса '%s': %s", user_query, e)
        await message.answer("❌ Не удалось обработать запрос. Попробуй переформулировать.")