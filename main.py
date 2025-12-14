import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot.handlers.other import other_router
from bot.handlers.query import query_router
from bot.handlers.start_help import start_help_router
from config.config import Config, load_config

config: Config = load_config()

logger = logging.getLogger(__name__)

# Функция конфигурирования и запуска бота
async def main(config: Config) -> None:
    logger.info("Starting bot...")

    # Инициализируем бот и диспетчер
    bot = Bot(
        token=config.bot.token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()


    # Подключаем роутеры в нужном порядке
    logger.info("Including routers...")
    dp.include_routers(start_help_router, query_router, other_router)


    # Запускаем поллинг
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    asyncio.run(main(config))