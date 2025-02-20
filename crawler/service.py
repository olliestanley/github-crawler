import asyncio
import logging
import time

import aiohttp
import ujson
from pydantic_settings import BaseSettings, SettingsConfigDict

from .util import RateLimiter


class GitHubConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="GITHUB_", extra="ignore"
    )

    api_url: str = "https://api.github.com/graphql"
    token: str

    # Max retries for a single query
    max_retries: int = 3
    connection_limit: int = 4


class GitHubService:
    def __init__(
        self,
        config: GitHubConfig,
        session: aiohttp.ClientSession,
        rate_limiter: RateLimiter,
    ):
        self.settings = config
        self.session = session
        self.rate_limiter = rate_limiter

    @classmethod
    async def create(cls, rate_limiter: RateLimiter):
        config = GitHubConfig()
        connector = aiohttp.TCPConnector(limit=config.connection_limit)
        session = aiohttp.ClientSession(connector=connector, json_serialize=ujson.dumps)
        return cls(config, session, rate_limiter)

    async def close(self):
        await self.session.close()

    async def fetch_page(
        self,
        query: str,
        variables: dict,
    ) -> tuple[list[dict], dict]:
        result = await self.execute_github_query(query, variables)
        nodes = [node for node in result["data"]["search"]["nodes"] if node]
        return nodes, result["data"]["search"]["pageInfo"]

    async def execute_github_query(
        self,
        query: str,
        variables: dict,
        current_retry: int = 0,
    ) -> dict:
        # Estimate approx query cost and wait until we have the rate limit budget for it
        # Note that this would need to be adjusted if we query deeper per repo, such as fetching issues, PRs, etc
        estimated_cost = 1 + (variables.get("first", 0) // 100)
        while not await self.rate_limiter.consume(estimated_cost):
            logging.info("Rate limited, waiting...")
            await asyncio.sleep(5)

        try:
            logging.info("Attempting query...")

            request_start = time.time()
            async with self.session.post(
                "https://api.github.com/graphql",
                json={"query": query, "variables": variables},
                headers={"Authorization": f"Bearer {self.settings.token}"},
            ) as response:
                logging.info(f"Query time: {time.time() - request_start:.2f}s")
                if response.status != 200:
                    if await self.handle_error(response, None, current_retry):
                        return await self.execute_github_query(
                            query, variables, current_retry + 1
                        )

                # ujson is faster than the built-in json module
                result = await response.json(loads=ujson.loads)

                if "errors" in result:
                    if all(
                        [
                            "data" in result,
                            "search" in result["data"],
                            "nodes" in result["data"]["search"],
                            result["data"]["search"]["nodes"],
                        ]
                    ):
                        logging.warning("Partial success in query, but with errors:")
                        logging.warning(str(result["errors"]))
                    elif await self.handle_error(response, result, current_retry):
                        return await self.execute_github_query(
                            query, variables, current_retry + 1
                        )

                # Use response headers to update rate limits - API docs recommend this over querying for them directly
                # https://docs.github.com/en/graphql/overview/rate-limits-and-node-limits-for-the-graphql-api#checking-the-status-of-your-primary-rate-limit
                rate_limit = response.headers.get("x-ratelimit-remaining")
                reset_at = response.headers.get("x-ratelimit-reset")
                if rate_limit and reset_at:
                    await self.rate_limiter.update(int(rate_limit), reset_at)

                return result
        except aiohttp.ClientError:
            if current_retry >= self.settings.max_retries:
                raise
            logging.warning("Unknown error in query, retrying...")
            await asyncio.sleep(2**current_retry)
            return await self.execute_github_query(query, variables, current_retry + 1)
        except asyncio.TimeoutError:
            if current_retry >= self.settings.max_retries:
                raise
            logging.warning("Timeout in query, retrying...")
            await asyncio.sleep(2**current_retry)
            return await self.execute_github_query(query, variables, current_retry + 1)

    async def handle_error(self, response, result, current_retry) -> bool:
        """Handle logging, sleep if applicable, and return whether to retry."""
        context = str(result.get("errors")[0]) if result else ""
        logging.warning(f"Error in query: {response.status}: {context}")

        if current_retry >= self.settings.max_retries:
            logging.error(
                f"Failed to execute query after {self.settings.max_retries} retries. Response {response.status}:"
            )
            logging.error(
                str(result.get("errors")) if result else (await response.text())
            )
            return False

        if response.headers.get("retry-after"):
            logging.info(
                f"Received retry-after header: {response.headers['retry-after']}s..."
            )
            await self.rate_limiter.pause(int(response.headers["retry-after"]))
            # Retry after the specified time
            await asyncio.sleep(int(response.headers["retry-after"]))
        else:
            pause_length = await self.rate_limiter.get_pause_length()
            # Fall back to exponential backoff if no current retry-after timer
            sleep_time = pause_length or 2**current_retry
            await asyncio.sleep(sleep_time)
        return True
