"""Asynchronous extractor for FakeStore API products."""

from __future__ import annotations

import json
import logging
import os
import asyncio
import time
import aiohttp
from datetime import datetime, timezone
from typing import Any, Dict, List


import json_formatter as Formatter
import config

class ProductExtractorAsync:
    """Extract products in chunks using asynchronous HTTP requests."""

    def __init__(self) -> None:
        """Initialize extractor state and runtime dependencies."""
        self.script_name = "extract_products_async"
        self.date_str = datetime.now().strftime("%y%m%d")
        self.time_str = datetime.now().strftime("%H%M%S")
        self.logger = self._setup_logger()
        self.expected_chunks = self._validate_config()
        self.semaphore = asyncio.Semaphore(config.CONCURRENCY_LIMIT)

    def _setup_logger(self) -> logging.Logger:
        """Create and configure a JSON file logger."""
        log_dir = os.path.join("logs", self.date_str)
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(
            log_dir,
            f"{self.script_name}_{self.date_str}_{self.time_str}.json",
        )

        logger = logging.getLogger(self.script_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.handlers.clear()

        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(Formatter.JsonFormatter())
        logger.addHandler(handler)
        return logger

    def _validate_config(self) -> int:
        """Validate extraction settings and return expected chunk count."""
        if config.CHUNK_SIZE <= 0:
            raise ValueError("CHUNK_SIZE must be greater than 0")
        if config.TOTAL_PRODUCTS <= 0:
            raise ValueError("TOTAL_PRODUCTS must be greater than 0")
        if config.RETRY_LIMIT < 0:
            raise ValueError("RETRY_LIMIT must be 0 or greater")
        if config.CONCURRENCY_LIMIT <= 0:
            raise ValueError("CONCURRENCY_LIMIT must be greater than 0")
        if config.TOTAL_PRODUCTS % config.CHUNK_SIZE != 0:
            raise ValueError("TOTAL_PRODUCTS must be divisible by CHUNK_SIZE")
        return config.TOTAL_PRODUCTS // config.CHUNK_SIZE

    def _build_url(self, skip: int) -> str:
        """Build chunk request URL with pagination parameters."""
        return (
            f"{config.API_BASE_URL}{config.PRODUCTS_ENDPOINT}"
            f"?limit={config.CHUNK_SIZE}&skip={skip}"
        )

    async def _request_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> List[Dict[str, Any]]:
        """Send GET request with exponential backoff retry behavior."""
        for attempt in range(config.RETRY_LIMIT + 1):
            request_id = f"req-{int(time.time() * 1000)}-{attempt}"
            started = time.perf_counter()
            status_code: int | None = None

            try:
                async with self.semaphore:
                    async with session.get(url) as response:
                        status_code = response.status
                        elapsed_ms = (time.perf_counter() - started) * 1000

                        self.logger.info(
                            "HTTP response received",
                            extra={
                                "request_id": request_id,
                                "url": url,
                                "method": "GET",
                                "status_code": status_code,
                                "elapsed_ms": round(elapsed_ms, 2),
                            },
                        )

                        if status_code == 429 or 500 <= status_code:
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=status_code,
                                message="Retryable response status",
                                headers=response.headers,
                            )

                        if 400 <= status_code < 500:
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=status_code,
                                message="Client error response",
                                headers=response.headers,
                            )

                        payload = await response.json()
                        if not isinstance(payload, list):
                            raise ValueError("API did not return a list payload")
                        return payload

            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                elapsed_ms = (time.perf_counter() - started) * 1000
                is_last = attempt >= config.RETRY_LIMIT
                self.logger.error(
                    "Request failed",
                    extra={
                        "request_id": request_id,
                        "url": url,
                        "method": "GET",
                        "status_code": status_code,
                        "elapsed_ms": round(elapsed_ms, 2),
                    },
                    exc_info=exc,
                )
                if is_last:
                    raise
                backoff = min(
                    config.BACKOFF_BASE_SECONDS * (2 ** attempt),
                    config.BACKOFF_MAX_SECONDS,
                )
                await asyncio.sleep(backoff)

        raise RuntimeError("Retry loop exhausted unexpectedly")

    def _write_chunk(self, chunk_number: int, products: List[Dict[str, Any]]) -> None:
        """Write one chunk of products to a JSON file."""
        os.makedirs(os.path.join("data", "json"), exist_ok=True)
        file_path = os.path.join(
            "data",
            "json",
            f"products_{chunk_number}_{self.date_str}_{self.time_str}.json",
        )
        with open(file_path, "w", encoding="utf-8") as output_file:
            json.dump(products, output_file, ensure_ascii=True, indent=2)
        self.logger.info(f"Wrote chunk file: {file_path}")

    async def _fetch_chunk(
        self,
        session: aiohttp.ClientSession,
        chunk_index: int,
    ) -> tuple[int, List[Dict[str, Any]]]:
        """Fetch a specific chunk and return chunk number with payload."""
        skip = chunk_index * config.CHUNK_SIZE
        url = self._build_url(skip)
        products = await self._request_with_retry(session, url)

        if len(products) != config.CHUNK_SIZE:
            raise ValueError(
                f"Chunk size mismatch at chunk {chunk_index + 1}: "
                f"expected {config.CHUNK_SIZE}, got {len(products)}"
            )
        return chunk_index + 1, products

    async def run(self) -> None:
        """Execute full extraction flow and write final timing log."""
        started = time.perf_counter()
        self.logger.info(
            f"Starting async extraction for {config.TOTAL_PRODUCTS} products in {self.expected_chunks} chunks"
        )

        timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(limit=config.CONCURRENCY_LIMIT)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [
                self._fetch_chunk(session, chunk_index)
                for chunk_index in range(self.expected_chunks)
            ]
            results = await asyncio.gather(*tasks)

        extracted_count = 0
        for chunk_number, products in sorted(results, key=lambda item: item[0]):
            extracted_count += len(products)
            self._write_chunk(chunk_number, products)
            self.logger.info(
                f"Chunk {chunk_number}/{self.expected_chunks} complete with {len(products)} products"
            )

        if extracted_count != config.TOTAL_PRODUCTS:
            raise ValueError(
                f"Total extraction mismatch: expected {config.TOTAL_PRODUCTS}, got {extracted_count}"
            )

        total_elapsed_ms = (time.perf_counter() - started) * 1000
        self.logger.info(
            "Async extraction completed",
            extra={"elapsed_ms": round(total_elapsed_ms, 2)},
        )


