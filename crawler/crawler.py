"""
Main crawler script. Asynchronous, allowing concurrent requests for speed.
Fetches ~N repositories and their star counts from GitHub, and stores them in a PostgreSQL database.
DB inserts are batched.
Tracks rate limits and retries failed queries.
"""

import asyncio
import logging
import time
from collections import deque

from pydantic_settings import BaseSettings, SettingsConfigDict

from .database import PostgresDatabase
from .models import Repository
from .service import GitHubService


class CrawlerConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Number of total repos to fetch
    target: int = 100000

    # GitHub GraphQL max per query
    batch_size: int = 100
    # Size of batches for DB inserts
    db_batch_size: int = 1000
    # Delay between tasks; adjust to avoid 403s, which require 60s waits before retry
    task_delay: float = 1

    # Should be set in conjunction with starting_searches
    max_concurrent_requests: int = 2
    # Should be as many starting searches as concurrent requests
    # There are ~100k repos >500 stars. Descending from the top & ascending from 500 should cover approx top 100k repos
    starting_searches_str: str = "is:public stars:<500000 sort:stars-desc,is:public stars:>500 sort:stars-asc"

    @property
    def starting_searches(self):
        return self.starting_searches_str.split(",")

GRAPHQL_QUERY = """
query ($queryString: String!, $first: Int!, $after: String) {
  search(query: $queryString, type: REPOSITORY, first: $first, after: $after) {
    nodes {
      ... on Repository {
        id
        nameWithOwner
        stargazerCount
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"""


async def crawl_repositories(service: GitHubService, database: PostgresDatabase):
    config = CrawlerConfig()

    logging.info(f"Fetching {config.target} repositories from GitHub...")

    start_time = time.time()

    queue = deque([("", search) for search in config.starting_searches])
    repos_buffer: list[Repository] = []
    total_repos = 0

    async def process_page(cursor, queryString) -> list[Repository]:
        variables = {
            "queryString": queryString,
            "first": config.batch_size,
            "after": cursor if cursor else None,
        }

        nodes, page_info = await service.fetch_page(GRAPHQL_QUERY, variables)

        if (total_repos + len(nodes)) < config.target:
            if page_info["hasNextPage"]:
                queue.append((page_info["endCursor"], queryString))
            elif nodes:
                logging.info("Search exhausted, starting fresh...")
                if "stars-desc" in queryString:
                    lowest_star_count = min([node["stargazerCount"] for node in nodes])
                    queue.append(
                        ("", f"is:public stars:<{lowest_star_count} sort:stars-desc")
                    )
                elif "stars-asc" in queryString:
                    highest_star_count = max([node["stargazerCount"] for node in nodes])
                    queue.append(
                        ("", f"is:public stars:>{highest_star_count} sort:stars-asc")
                    )
                else:
                    logging.error("Unknown search query, ending...")
            else:
                logging.error("No nodes, ending...")

        def node_to_repository(node) -> Repository | None:
            try:
                return Repository(
                    id=node["id"],
                    name_with_owner=node["nameWithOwner"],
                    stargazer_count=node["stargazerCount"],
                )
            except Exception as e:
                logging.error(f"Error processing node: {e}")
                return None

        return [repo for node in nodes if (repo := node_to_repository(node))]

    tasks = [
        asyncio.create_task(process_page(*queue.popleft()))
        for _ in range(min(config.max_concurrent_requests, len(queue)))
    ]

    logging.info(f"Created {len(tasks)} initial tasks...")

    while tasks:
        done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            repositories: list[Repository] = await task
            repos_buffer.extend(repositories)
            total_repos += len(repositories)

            logging.info(f"Total repos fetched: {total_repos}/{config.target}")

            # Batch insert whenever buffer hits desired batch size
            if len(repos_buffer) >= config.db_batch_size:
                # Avoid waiting for the DB insert to complete using a task
                asyncio.create_task(database.batch_insert_repos(repos_buffer[: config.db_batch_size]))
                repos_buffer = repos_buffer[config.db_batch_size :]

            # New task if there are more cursors
            if queue:
                # Delay to avoid secondary rate limits
                await asyncio.sleep(config.task_delay)
                logging.info(f"Creating new task...")
                tasks.add(asyncio.create_task(process_page(*queue.popleft())))

        logging.info(f"Elapsed time: {time.time() - start_time:.2f}s")

    # A final batch insert to handle any remaining repos
    if repos_buffer:
        await database.batch_insert_repos(repos_buffer)
