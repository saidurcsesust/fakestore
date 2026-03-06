from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Tuple

import aiohttp

import config
from mockaroo.mockaroo_common import (
    build_mockaroo_headers,
    build_mockaroo_params,
    build_mockaroo_url,
    build_run_timestamps,
    ensure_chunk_size,
    parse_mockaroo_json_payload,
    setup_json_logger,
    validate_mockaroo_config,
    write_chunk_file,
)


class ProductExtractorMockarooAsync:
    def __init__(self) -> None:
        self.script_name = "extract_products_mockaroo_async"
        self.date_str, self.time_str = build_run_timestamps()
        self.logger = setup_json_logger(self.script_name, self.date_str, self.time_str)
        self.expected_chunks = validate_mockaroo_config(require_concurrency=True)
        self.semaphore = asyncio.Semaphore(config.CONCURRENCY_LIMIT)

        self.url = build_mockaroo_url()
        self.params = build_mockaroo_params()
        self.headers = build_mockaroo_headers()

    async def _request_with_retry(
        self,
        session: aiohttp.ClientSession,
        chunk_number: int,
    ) -> List[Dict[str, object]]:
        for attempt in range(config.RETRY_LIMIT + 1):
            request_id = f"req-{int(time.time() * 1000)}-{chunk_number}-{attempt}"
            started = time.perf_counter()
            status_code: int | None = None

            try:
                async with self.semaphore:
                    async with session.get(self.url, params=self.params) as response:
                        status_code = response.status
                        elapsed_ms = (time.perf_counter() - started) * 1000

                        self.logger.info(
                            "HTTP response received",
                            extra={
                                "request_id": request_id,
                                "url": str(response.url),
                                "method": "GET",
                                "status_code": status_code,
                                "elapsed_ms": round(elapsed_ms, 2),
                            },
                        )

                        if status_code == 429 or 500 <= status_code < 600:
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

                        payload = parse_mockaroo_json_payload(
                            response.headers.get("Content-Type", ""),
                            await response.json(),
                        )
                        return payload

            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                elapsed_ms = (time.perf_counter() - started) * 1000
                is_last = attempt >= config.RETRY_LIMIT
                self.logger.error(
                    "Request failed",
                    extra={
                        "request_id": request_id,
                        "url": self.url,
                        "method": "GET",
                        "status_code": status_code,
                        "elapsed_ms": round(elapsed_ms, 2),
                    },
                    exc_info=exc,
                )
                if is_last:
                    raise
                backoff = min(
                    config.BACKOFF_BASE_SECONDS * (2**attempt),
                    config.BACKOFF_MAX_SECONDS,
                )
                await asyncio.sleep(backoff)

        raise RuntimeError("Retry loop exhausted unexpectedly")

    async def _fetch_chunk(
        self,
        session: aiohttp.ClientSession,
        chunk_index: int,
    ) -> Tuple[int, List[Dict[str, object]]]:
        
        #Fetch one chunk and return chunk number with payload.
        chunk_number = chunk_index + 1
        products = await self._request_with_retry(session, chunk_number)
        ensure_chunk_size(products, chunk_number)
        return chunk_number, products

    async def run(self) -> None:
        started = time.perf_counter()
        self.logger.info(
            f"Starting Mockaroo async extraction for {config.TOTAL_PRODUCTS} products in {self.expected_chunks} chunks"
        )

        timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(limit=config.CONCURRENCY_LIMIT)
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self.headers,
        ) as session:
            tasks = [self._fetch_chunk(session, idx) for idx in range(self.expected_chunks)]
            results = await asyncio.gather(*tasks)

        extracted_count = 0
        for chunk_number, products in sorted(results, key=lambda row: row[0]):
            extracted_count += len(products)
            write_chunk_file(
                chunk_number,
                products,
                self.date_str,
                self.time_str,
                self.logger,
                "async"
            )
            self.logger.info(
                f"Chunk {chunk_number}/{self.expected_chunks} complete with {len(products)} products"
            )

        if extracted_count != config.TOTAL_PRODUCTS:
            raise ValueError(
                f"Total extraction mismatch: expected {config.TOTAL_PRODUCTS}, got {extracted_count}"
            )

        total_elapsed_ms = (time.perf_counter() - started) * 1000
        self.logger.info(
            "Mockaroo async extraction completed",
            extra={"elapsed_ms": round(total_elapsed_ms, 2)},
        )
