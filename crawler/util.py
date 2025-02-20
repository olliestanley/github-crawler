import asyncio
from datetime import datetime, timedelta, timezone


class RateLimiter:
    """
    Allows us to keep track of rate limits despite concurrency.
    Idea is to estimate our query cost, then only query if we have a sufficient budget.
    Once the response comes back, we update the budget again, in case our estimate was wrong.
    """

    def __init__(self, max_points: int = 1000):
        self.points = max_points
        self.reset_time = None
        self.pause_end = None
        self.lock = asyncio.Lock()

    async def consume(self, cost: int) -> bool:
        async with self.lock:
            if self.reset_time and datetime.now(timezone.utc) >= self.reset_time:
                self.points = 1000
                self.reset_time = None

            if self.pause_end:
                if datetime.now(timezone.utc) < self.pause_end:
                    return False
                self.pause_end = None

            if self.points >= cost:
                self.points -= cost
                return True
            return False

    async def get_pause_length(self) -> int | None:
        async with self.lock:
            if self.pause_end:
                if datetime.now(timezone.utc) < self.pause_end:
                    # Return seconds until pause_end
                    return (self.pause_end - datetime.now(timezone.utc)).total_seconds()
                self.pause_end = None
        return None

    async def pause(self, pause_length: int):
        async with self.lock:
            new_pause_end = datetime.now(timezone.utc) + timedelta(seconds=pause_length)
            if not self.pause_end or new_pause_end > self.pause_end:
                self.pause_end = new_pause_end

    async def update(self, remaining: int, reset_at: str):
        async with self.lock:
            self.points = remaining
            self.reset_time = datetime.fromtimestamp(int(reset_at), tz=timezone.utc)
