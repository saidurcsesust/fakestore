"""Synchronous extractor for FakeStore API products."""

from __future__ import annotations

import json
import logging
import os
import time
import asyncio
from datetime import datetime
from typing import Any, Dict, List

import requests
import json_formatter as Formatter
import extract_products_async as AsyncMethod
import config


class ProductExtractorSync:
    """Extract products in chunks using synchronous HTTP requests."""

    def __init__(self) -> None:
        """Initialize extractor state and runtime dependencies."""
        self.script_name = "extract_products_sync"
        self.date_str = datetime.now().strftime("%y%m%d")
        self.time_str = datetime.now().strftime("%H%M%S")
        self.logger = self._setup_logger()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.USER_AGENT})
        self.expected_chunks = self._validate_config()

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
        if config.TOTAL_PRODUCTS % config.CHUNK_SIZE != 0:
            raise ValueError("TOTAL_PRODUCTS must be divisible by CHUNK_SIZE")
        return config.TOTAL_PRODUCTS // config.CHUNK_SIZE

    def _build_url(self, skip: int) -> str:
        """Build chunk request URL with pagination parameters."""
        return (
            f"{config.API_BASE_URL}{config.PRODUCTS_ENDPOINT}"
            f"?limit={config.CHUNK_SIZE}&skip={skip}"
        )

    def _request_with_retry(self, url: str) -> List[Dict[str, Any]]:
        """Send GET request with exponential backoff retry behavior."""
        for attempt in range(config.RETRY_LIMIT + 1):
            request_id = f"req-{int(time.time() * 1000)}-{attempt}"
            started = time.perf_counter()
            status_code: int | None = None
            try:
                response = self.session.get(url, timeout=config.REQUEST_TIMEOUT_SECONDS)
                status_code = response.status_code
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
                time.sleep(backoff)

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

    def run(self) -> None:
        """Execute full extraction flow and write final timing log."""
        started = time.perf_counter()
        self.logger.info(
            f"Starting sync extraction for {config.TOTAL_PRODUCTS} products in {self.expected_chunks} chunks"
        )

        extracted_count = 0
        for chunk_index in range(self.expected_chunks):
            skip = chunk_index * config.CHUNK_SIZE
            url = self._build_url(skip)
            products = self._request_with_retry(url)

            if len(products) != config.CHUNK_SIZE:
                raise ValueError(
                    f"Chunk size mismatch at chunk {chunk_index + 1}: "
                    f"expected {config.CHUNK_SIZE}, got {len(products)}"
                )

            extracted_count += len(products)
            self._write_chunk(chunk_index + 1, products)
            self.logger.info(
                f"Chunk {chunk_index + 1}/{self.expected_chunks} complete with {len(products)} products"
            )

        if extracted_count != config.TOTAL_PRODUCTS:
            raise ValueError(
                f"Total extraction mismatch: expected {config.TOTAL_PRODUCTS}, got {extracted_count}"
            )

        total_elapsed_ms = (time.perf_counter() - started) * 1000
        self.logger.info(
            "Sync extraction completed",
            extra={"elapsed_ms": round(total_elapsed_ms, 2)},
        )


def main() -> None:
    """Entry point for synchronous extraction."""
    extractor = ProductExtractorSync()
    extractor.run()
    extractor = AsyncMethod.ProductExtractorAsync()
    asyncio.run(extractor.run())

    


if __name__ == "__main__":
    main()
