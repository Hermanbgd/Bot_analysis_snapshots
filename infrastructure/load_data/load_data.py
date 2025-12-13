import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional

from psycopg import AsyncConnection

from config.config import Config, load_config
from infrastructure.database.connection import get_pg_connection

config: Config = load_config()

logging.basicConfig(
    level=logging.getLevelName(level=config.log.level),
    format=config.log.format,
)

logger = logging.getLogger(__name__)


async def upsert_videos_and_snapshots(
    conn: AsyncConnection,
    *,
    data: List[Dict[str, Any]],
) -> None:
    """
    Загружает данные видео и их снапшотов из списка словарей.

    Использует UPSERT для видео (обновление счётчиков + updated_at)
    и INSERT ... ON CONFLICT (id) DO NOTHING для снапшотов.
    """
    if not data:
        logger.info("Нет данных для загрузки в videos/video_snapshots")
        return

    video_values: List[tuple] = []
    snapshot_values: List[tuple] = []

    try:
        for video in data:
            # Данные видео
            video_values.append((
                video["id"],
                video["creator_id"],
                video["video_created_at"],
                video["views_count"],
                video["likes_count"],
                video["comments_count"],
                video["reports_count"],
                video["created_at"],
                video["updated_at"],
            ))

            # Снапшоты
            for snap in video.get("snapshots", []):
                snapshot_values.append((
                    snap["id"],
                    snap["video_id"],
                    snap["views_count"],
                    snap["likes_count"],
                    snap["comments_count"],
                    snap["reports_count"],
                    snap["delta_views_count"],
                    snap["delta_likes_count"],
                    snap["delta_comments_count"],
                    snap["delta_reports_count"],
                    snap["created_at"],
                    snap["updated_at"],
                ))
    except KeyError as e:
        logger.error("Отсутствует обязательное поле в данных: %s", e)
        raise
    except Exception as e:
        logger.error("Ошибка при подготовке данных для загрузки: %s", e)
        raise

    try:
        async with conn.cursor() as cur:
            # UPSERT видео
            if video_values:
                await cur.executemany(
                    """
                    INSERT INTO videos (
                        id, creator_id, video_created_at,
                        views_count, likes_count, comments_count, reports_count,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        views_count = EXCLUDED.views_count,
                        likes_count = EXCLUDED.likes_count,
                        comments_count = EXCLUDED.comments_count,
                        reports_count = EXCLUDED.reports_count,
                        updated_at = EXCLUDED.updated_at;
                    """,
                    video_values,
                )
                logger.info("Успешно загружено/обновлено %d видео", len(video_values))

            # INSERT снапшотов
            if snapshot_values:
                await cur.executemany(
                    """
                    INSERT INTO video_snapshots (
                        id, video_id,
                        views_count, likes_count, comments_count, reports_count,
                        delta_views_count, delta_likes_count,
                        delta_comments_count, delta_reports_count,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    snapshot_values,
                )
                logger.info("Успешно загружено %d снапшотов (дубликаты пропущены)", len(snapshot_values))

        logger.info(
            "Загрузка данных завершена успешно. Время: %s. Видео: %d, Снапшоты: %d",
            datetime.now(timezone.utc).isoformat(),
            len(video_values),
            len(snapshot_values),
        )

    except Exception as e:
        logger.error("Ошибка при выполнении SQL-запросов: %s", e)
        raise


async def main():
    data_path = "videos.json"
    videos_data: List[Dict[str, Any]] = []

    # Чтение JSON
    try:
        with open(data_path, encoding="utf-8") as f:
            full_data = json.load(f)
        videos_data = full_data.get("videos", full_data)
        logger.info("Успешно загружен JSON-файл: %s. Количество видео: %d", data_path, len(videos_data))
    except FileNotFoundError:
        logger.error("Файл не найден: %s", data_path)
        return
    except json.JSONDecodeError as e:
        logger.error("Ошибка разбора JSON в файле %s: %s", data_path, e)
        return
    except Exception as e:
        logger.error("Неожиданная ошибка при чтении файла %s: %s", data_path, e)
        return

    connection: Optional[AsyncConnection] = None
    try:
        connection = await get_pg_connection(
            db_name=config.db.name,
            host=config.db.host,
            port=config.db.port,
            user=config.db.user,
            password=config.db.password,
        )
        async with connection:
            async with connection.transaction():
                await upsert_videos_and_snapshots(connection, data=videos_data)

    except Exception as e:
        logger.error("Критическая ошибка при работе с базой данных: %s", e)
        # Транзакция автоматически откатится при выходе из блока с исключением
    finally:
        if connection and not connection.closed:
            await connection.close()
            logger.debug("Подключение к БД закрыто")


asyncio.run(main())