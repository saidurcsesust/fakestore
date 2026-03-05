from __future__ import annotations

import time
from typing import Any, Dict, List

import requests
import config
from sync_async.base_extractor import BaseProductExtractor


class ProductExtractorSync(BaseProductExtractor):
    script_name = "extract_products_sync"

    def __init__(self) -> None:
        super().__init__()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.USER_AGENT})

    def _request_with_retry(self, url: str) -> List[Dict[str, Any]]:
        for attempt in range(config.RETRY_LIMIT + 1):
            request_id = self._make_request_id(attempt)
            started = time.perf_counter()
            status_code: int | None = None

            try:
                response = self.session.get(url, timeout=config.REQUEST_TIMEOUT_SECONDS)
                status_code = response.status_code
                elapsed_ms = (time.perf_counter() - started) * 1000
                self._log_response(request_id, url, status_code, elapsed_ms)

                if status_code == 429 or status_code >= 500:
                    raise requests.HTTPError(
                        f"Retryable response status: {status_code}",
                        response=response,
                    )
                if 400 <= status_code < 500:
                    response.raise_for_status()

                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("API did not return a list payload")
                return payload

            except (requests.RequestException, ValueError) as exc:
                elapsed_ms = (time.perf_counter() - started) * 1000
                self._log_response(
                    request_id, url, status_code, elapsed_ms,
                    level="error", exc=exc,
                )
                if attempt >= config.RETRY_LIMIT:
                    raise
                time.sleep(self._backoff(attempt))

        raise RuntimeError("Retry loop exhausted unexpectedly")

    def run(self) -> None:
        started = time.perf_counter()
        self.logger.info(
            f"Starting sync extraction for {config.TOTAL_PRODUCTS} products "
            f"in {self.expected_chunks} chunks"
        )

        extracted_count = 0
        for chunk_index in range(self.expected_chunks):
            url = self._build_url(skip=chunk_index * config.CHUNK_SIZE)
            products = self._request_with_retry(url)

            if len(products) != config.CHUNK_SIZE:
                raise ValueError(
                    f"Chunk size mismatch at chunk {chunk_index + 1}: "
                    f"expected {config.CHUNK_SIZE}, got {len(products)}"
                )

            extracted_count += len(products)
            self._write_chunk(chunk_index + 1, products, label="sync")
            self.logger.info(
                f"Chunk {chunk_index + 1}/{self.expected_chunks} complete "
                f"with {len(products)} products"
            )

        self._assert_total(extracted_count)
        elapsed_ms = (time.perf_counter() - started) * 1000
        self.logger.info(
            "Sync extraction completed",
            extra={"elapsed_ms": round(elapsed_ms, 2)},
        )