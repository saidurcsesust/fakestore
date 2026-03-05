from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List
import aiohttp

import config
from sync_async.base_extractor import BaseProductExtractor


class ProductExtractorAsync(BaseProductExtractor):
    script_name = "extract_products_async"

    def __init__(self) -> None:
        super().__init__()
        if config.CONCURRENCY_LIMIT <= 0:
            raise ValueError("CONCURRENCY_LIMIT must be greater than 0")
        self.semaphore = asyncio.Semaphore(config.CONCURRENCY_LIMIT)

    async def _request_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
    ) -> List[Dict[str, Any]]:
        for attempt in range(config.RETRY_LIMIT + 1):
            request_id = self._make_request_id(attempt)
            started = time.perf_counter()
            status_code: int | None = None

            try:
                async with self.semaphore:
                    async with session.get(url) as response:
                        status_code = response.status
                        elapsed_ms = (time.perf_counter() - started) * 1000
                        self._log_response(request_id, url, status_code, elapsed_ms)

                        if status_code == 429 or status_code >= 500:
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
                self._log_response(
                    request_id, url, status_code, elapsed_ms,
                    level="error", exc=exc,
                )
                if attempt >= config.RETRY_LIMIT:
                    raise
                await asyncio.sleep(self._backoff(attempt))

        raise RuntimeError("Retry loop exhausted unexpectedly")

    async def _fetch_chunk(
        self,
        session: aiohttp.ClientSession,
        chunk_index: int,
    ) -> tuple[int, List[Dict[str, Any]]]:
        url = self._build_url(skip=chunk_index * config.CHUNK_SIZE)
        products = await self._request_with_retry(session, url)
        if len(products) != config.CHUNK_SIZE:
            raise ValueError(
                f"Chunk size mismatch at chunk {chunk_index + 1}: "
                f"expected {config.CHUNK_SIZE}, got {len(products)}"
            )
        return chunk_index + 1, products

    async def run(self) -> None:
        started = time.perf_counter()
        self.logger.info(
            f"Starting async extraction for {config.TOTAL_PRODUCTS} products "
            f"in {self.expected_chunks} chunks"
        )

        timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(limit=config.CONCURRENCY_LIMIT)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [
                self._fetch_chunk(session, i) for i in range(self.expected_chunks)
            ]
            results = await asyncio.gather(*tasks)

        extracted_count = 0
        for chunk_number, products in sorted(results, key=lambda t: t[0]):
            extracted_count += len(products)
            self._write_chunk(chunk_number, products, label="async")
            self.logger.info(
                f"Chunk {chunk_number}/{self.expected_chunks} complete "
                f"with {len(products)} products"
            )

        self._assert_total(extracted_count)
        elapsed_ms = (time.perf_counter() - started) * 1000
        self.logger.info(
            "Async extraction completed",
            extra={"elapsed_ms": round(elapsed_ms, 2)},
        )