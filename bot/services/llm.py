import asyncio
import logging
from typing import List, Dict
import aiofiles

import aiohttp

from config.config import Config, load_config

logger = logging.getLogger(__name__)

config: Config = load_config()

URL = "https://app.chipp.ai/api/v1/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {config.ai.token}",
    "Content-Type": "application/json"
}


async def get_sql_query(user_query: str,) -> str:

    async with aiofiles.open("prompt.txt", 'r', encoding="utf-8") as prompt_file:
        prompt = await prompt_file.read()

        messages: List[Dict[str, str]] = [
            {
                "role": "user",
                "content": f'{prompt}\n Запрос пользователя: {user_query}'
                           f'Ответь исключительно SQL-кодом.'
            }
        ]
        payload = {
            "model": "newapplication-10028464",
            "messages": messages,
            "stream": False,
            "temperature": 0.0,
        }

        timeout = aiohttp.ClientTimeout(total=30)

        try:
            async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
                async with session.post(URL, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        content = data["choices"][0]["message"]["content"].strip()

                        # Удаляем возможные ```sql
                        if content.startswith("```"):
                            # Убираем блок кода
                            content = content.split("```", 2)[1]  # берём середину между ```
                            if content.lstrip().startswith("sql"):
                                content = content[3:]  # убираем "sql" в начале

                        content = content.strip()

                        logger.info(f"Очищенный SQL: {content}")

                        return content
                    else:
                        error = await resp.text()
                        print(f"API Error {resp.status}: {error}")
                        return f"Ошибка API: {resp.status}"
        except asyncio.TimeoutError:
            return "Таймаут запроса к Chipp.ai"
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")
            return "Внутренняя ошибка сервера"
