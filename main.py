import asyncio
import logging
import time

from crawler.crawler import crawl_repositories
from crawler.database import PostgresDatabase
from crawler.service import GitHubService
from crawler.util import RateLimiter


async def main():
    rate_limiter = RateLimiter()

    database = await PostgresDatabase.create()
    service = await GitHubService.create(rate_limiter)

    await crawl_repositories(service, database)

    await database.close()
    await service.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting...")
    start_time = time.time()
    asyncio.run(main())
    logging.info(f"Execution time: {time.time() - start_time:.2f}s")
