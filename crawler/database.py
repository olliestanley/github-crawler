import logging
import time
from datetime import datetime, timezone

import asyncpg
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import Repository


class DatabaseConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="DB_", extra="ignore"
    )

    host: str = "localhost"
    port: int = 5432
    database: str = "github_data"
    user: str = "postgres"
    password: str = "postgres"


class PostgresDatabase:
    def __init__(self, config: DatabaseConfig, pool: asyncpg.pool.Pool):
        self.config = config
        self.pool = pool

    @classmethod
    async def create(cls):
        config = DatabaseConfig()

        db_config = {
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "user": config.user,
            "password": config.password,
        }

        pool = await asyncpg.create_pool(**db_config)
        return cls(config, pool)

    async def close(self):
        await self.pool.close()

    async def batch_insert_repos(self, repos: list[Repository]):
        if not repos:  # sanity check
            return

        start_time = time.time()

        # Values for batch insert
        values = [
            (
                repo.id,
                repo.name_with_owner,
                repo.stargazer_count,
                datetime.now(timezone.utc),
            )
            for repo in repos
            if repo
        ]

        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO repositories (repo_id, name_with_owner, stargazer_count, last_updated)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (repo_id) DO UPDATE SET
                    stargazer_count = EXCLUDED.stargazer_count,
                    last_updated = EXCLUDED.last_updated;
            """,
                values,
            )

        logging.info(f"DB insert time: {time.time() - start_time:.2f}s")
