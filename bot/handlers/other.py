from aiogram import Router
from aiogram.types import Message, ContentType

other_router = Router()


@other_router.message(lambda message: message.content_type != ContentType.TEXT)
async def handle_non_text(message: Message):
    await message.answer("Я понимаю только текстовые вопросы по статистике 📊\nНапиши текстом, например: «Сколько видео вышло в ноябре?»")