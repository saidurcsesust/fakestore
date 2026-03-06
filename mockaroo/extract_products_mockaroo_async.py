from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Tuple
import httpx

import config
from mockaroo_common import (
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

RETRY_LIMIT = 3
CHUNK_SIZE = 5
TOTAL_PRODUCTS = 20
CONCURRENCY_LIMIT = 4


class ProductExtractorMockarooAsync:
    def __init__(self) -> None:
        """Initialise logger, config, semaphore, and shared request values."""
        self.script_name = "extract_products_mockaroo_async"
        self.date_str, self.time_str = build_run_timestamps()
        self.logger = setup_json_logger(self.script_name, self.date_str, self.time_str)
        self.expected_chunks = validate_mockaroo_config(require_concurrency=True)
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

        self.url = build_mockaroo_url()
        self.headers = build_mockaroo_headers()

    async def _request_with_retry(
        self,
        client: httpx.AsyncClient,
        chunk_number: int,
        params: Dict,
    ) -> List[Dict[str, object]]:
        """Fetch one chunk from Mockaroo with exponential backoff retry."""
        for attempt in range(RETRY_LIMIT + 1):
            request_id = f"req-{int(time.time() * 1000)}-{chunk_number}-{attempt}"
            started = time.perf_counter()
            status_code: int | None = None

            try:
                async with self.semaphore:
                    response = await client.get(self.url, params=params)
                    status_code = response.status_code
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
                        raise httpx.HTTPStatusError(
                            message="Retryable response status",
                            request=response.request,
                            response=response,
                        )
                    if 400 <= status_code < 500:
                        raise httpx.HTTPStatusError(
                            message="Client error response",
                            request=response.request,
                            response=response,
                        )

                    payload = parse_mockaroo_json_payload(
                        response.headers.get("content-type", ""),
                        response.json(),
                    )
                    return payload

            except (httpx.HTTPError, asyncio.TimeoutError, ValueError) as exc:
                elapsed_ms = (time.perf_counter() - started) * 1000
                is_last = attempt >= RETRY_LIMIT
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
                    config.BACKOFF_BASE_SECONDS * (2 ** attempt),
                    config.BACKOFF_MAX_SECONDS,
                )
                await asyncio.sleep(backoff)

        raise RuntimeError("Retry loop exhausted unexpectedly")

    async def _fetch_chunk(
        self,
        client: httpx.AsyncClient,
        chunk_index: int,
    ) -> Tuple[int, List[Dict[str, object]]]:
        """Fetch one chunk and return chunk number with payload."""
        chunk_number = chunk_index + 1
        params = build_mockaroo_params(count=CHUNK_SIZE)
        products = await self._request_with_retry(client, chunk_number, params)
        ensure_chunk_size(products, chunk_number)
        return chunk_number, products

    async def run(self) -> None:
        """Run async Mockaroo extraction across all chunks concurrently."""
        started = time.perf_counter()
        self.logger.info(
            f"Starting Mockaroo async extraction for {TOTAL_PRODUCTS} "
            f"products in {self.expected_chunks} chunks"
        )

        timeout = httpx.Timeout(config.REQUEST_TIMEOUT_SECONDS)
        limits = httpx.Limits(max_connections=CONCURRENCY_LIMIT)

        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers=self.headers,
        ) as client:
            tasks = [self._fetch_chunk(client, idx) for idx in range(self.expected_chunks)]
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
                "async",
            )
            self.logger.info(
                f"Chunk {chunk_number}/{self.expected_chunks} complete "
                f"with {len(products)} products"
            )

        if extracted_count != TOTAL_PRODUCTS:
            raise ValueError(
                f"Total extraction mismatch: expected {TOTAL_PRODUCTS}, "
                f"got {extracted_count}"
            )

        total_elapsed_ms = (time.perf_counter() - started) * 1000
        self.logger.info(
            "Mockaroo async extraction completed",
            extra={"elapsed_ms": round(total_elapsed_ms, 2)},
        )