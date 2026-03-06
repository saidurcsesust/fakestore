from __future__ import annotations

import time
from typing import Any, Dict, List

import requests

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


class ProductExtractorMockarooSync:

    def __init__(self) -> None:
        """Initialise logger, config, session, and shared request values."""
        self.script_name = "extract_products_mockaroo_sync"
        self.date_str, self.time_str = build_run_timestamps()
        self.logger = setup_json_logger(self.script_name, self.date_str, self.time_str)
        self.expected_chunks = validate_mockaroo_config()

        self.session = requests.Session()
        self.session.headers.update(build_mockaroo_headers())
        self.url = build_mockaroo_url()

    def _request_with_retry(
        self,
        chunk_number: int,
        params: Dict,
    ) -> List[Dict[str, Any]]:
        """Fetch one chunk from Mockaroo with exponential backoff retry."""
        for attempt in range(RETRY_LIMIT + 1):
            request_id = f"req-{int(time.time() * 1000)}-{chunk_number}-{attempt}"
            started = time.perf_counter()
            status_code: int | None = None

            try:
                response = self.session.get(
                    self.url,
                    params=params,
                    timeout=config.REQUEST_TIMEOUT_SECONDS,
                )
                status_code = response.status_code
                elapsed_ms = (time.perf_counter() - started) * 1000

                self.logger.info(
                    "HTTP response received",
                    extra={
                        "request_id": request_id,
                        "url": response.url,
                        "method": "GET",
                        "status_code": status_code,
                        "elapsed_ms": round(elapsed_ms, 2),
                    },
                )

                if status_code == 429 or 500 <= status_code < 600:
                    raise requests.HTTPError(
                        f"Retryable response status: {status_code}",
                        response=response,
                    )
                if 400 <= status_code < 500:
                    response.raise_for_status()

                payload = parse_mockaroo_json_payload(
                    response.headers.get("Content-Type", ""),
                    response.json(),
                )
                return payload

            except (requests.RequestException, ValueError) as exc:
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
                time.sleep(backoff)

        raise RuntimeError("Retry loop exhausted unexpectedly")

    def run(self) -> None:
        """Execute the complete synchronous Mockaroo extraction workflow."""
        started = time.perf_counter()
        self.logger.info(
            f"Starting Mockaroo sync extraction for {TOTAL_PRODUCTS} "
            f"products in {self.expected_chunks} chunks"
        )

        extracted_count = 0
        for chunk_number in range(1, self.expected_chunks + 1):
            params = build_mockaroo_params(count=CHUNK_SIZE)
            products = self._request_with_retry(chunk_number, params)
            ensure_chunk_size(products, chunk_number)

            extracted_count += len(products)
            write_chunk_file(
                chunk_number,
                products,
                self.date_str,
                self.time_str,
                self.logger,
                "sync",
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
            "Mockaroo sync extraction completed",
            extra={"elapsed_ms": round(total_elapsed_ms, 2)},
        )