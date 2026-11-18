# SPDX-License-Identifier: MIT
import asyncio
from functools import cached_property
from typing import Literal

from pydantic import PositiveInt
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    LAREX_PROCESSOR_ID: str
    LAREX_DISPATCH_HMAC_SECRET: str
    LAREX_ALLOWED_CALLBACK_ORIGINS: list[str]
    LAREX_ROUTE_PREFIXES: list[str]
    LAREX_SDK_TRANSPORT_DEBUG: bool = False
    
    ACTION_DEBUG: bool = False
    ACTION_MAX_CONCURRENT_RUNS: PositiveInt = 1
    ACTION_LOADING_TIMEOUT: PositiveInt | None = 30
    ACTION_PROCESSING_TIMEOUT: PositiveInt | None = 30
    ACTION_TIMEOUT_MODE: Literal['fail_action', 'fail_page'] = 'fail_page'
    
    @cached_property
    def RUN_SEMAPHORE(self) -> asyncio.Semaphore:
        return asyncio.Semaphore(self.ACTION_MAX_CONCURRENT_RUNS)
    
    LINE_SEGMENTATION_THREADS: PositiveInt = 1
    

env = Settings()
