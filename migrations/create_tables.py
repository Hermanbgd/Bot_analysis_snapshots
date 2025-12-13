import asyncio
import logging
import os
import sys

from infrastructure.database.connection import get_pg_connection
from config.config import Config, load_config
from psycopg import AsyncConnection, Error

config: Config = load_config()

logging.basicConfig(
    level=logging.getLevelName(level=config.log.level),
    format=config.log.format,
)

logger = logging.getLogger(__name__)


async def main():
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
            async with connection.transaction():
                async with connection.cursor() as cursor:
                    # Таблица видео
                    await cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS videos (
                            id BIGSERIAL PRIMARY KEY,
                            creator_id BIGINT NOT NULL,
                            video_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            views_count BIGINT,
                            likes_count BIGINT,
                            comments_count BIGINT,
                            reports_count BIGINT,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        );
                        """
                    )
                    # Таблица снэпшотов
                    await cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS video_snapshots (
                            id BIGSERIAL PRIMARY KEY,
                            video_id BIGINT REFERENCES videos(id),
                            views_count BIGINT,
                            likes_count BIGINT,
                            comments_count BIGINT,
                            reports_count BIGINT,
                            delta_views_count BIGINT,
                            delta_likes_count BIGINT,
                            delta_comments_count BIGINT,
                            delta_reports_count BIGINT,
                            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        );
                        """
                    )
                logger.info("Tables 'videos', 'video_snapshots' were successfully created")
    except Error as db_error:
        logger.exception("Database-specific error: %s", db_error)
    except Exception as e:
        logger.exception("Unhandled error: %s", e)
    finally:
        if connection:
            await connection.close()
            logger.info("Connection to Postgres closed")

asyncio.run(main())