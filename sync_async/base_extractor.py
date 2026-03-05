from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List

import sync_async.json_formatter as Formatter
import config


class BaseProductExtractor:
    script_name: str = "base_extractor"

    def __init__(self) -> None:
        self.date_str = datetime.now().strftime("%y%m%d")
        self.time_str = datetime.now().strftime("%H%M%S")
        self.logger = self._setup_logger()
        self.expected_chunks = self._validate_config()

    
    # Logging                                                            
    def _setup_logger(self) -> logging.Logger:
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

    
    # Config validation                                                   
    def _validate_config(self) -> int:
        if config.CHUNK_SIZE <= 0:
            raise ValueError("CHUNK_SIZE must be greater than 0")
        if config.TOTAL_PRODUCTS <= 0:
            raise ValueError("TOTAL_PRODUCTS must be greater than 0")
        if config.RETRY_LIMIT < 0:
            raise ValueError("RETRY_LIMIT must be 0 or greater")
        if config.TOTAL_PRODUCTS % config.CHUNK_SIZE != 0:
            raise ValueError("TOTAL_PRODUCTS must be divisible by CHUNK_SIZE")
        return config.TOTAL_PRODUCTS // config.CHUNK_SIZE


    # URL building                                                         
    def _build_url(self, skip: int) -> str:
        return (
            f"{config.API_BASE_URL}{config.PRODUCTS_ENDPOINT}"
            f"?limit={config.CHUNK_SIZE}&skip={skip}"
        )


    def _write_chunk(
        self,
        chunk_number: int,
        products: List[Dict[str, Any]],
        *,
        label: str,
    ) -> None:
        out_dir = os.path.join("data", "json")
        os.makedirs(out_dir, exist_ok=True)
        file_path = os.path.join(
            out_dir,
            f"products_{label}{chunk_number}_{self.date_str}_{self.time_str}.json",
        )
        with open(file_path, "w", encoding="utf-8") as fh:
            json.dump(products, fh, ensure_ascii=True, indent=2)
        self.logger.info(f"Wrote chunk file: {file_path}")


    def _log_response(
        self,
        request_id: str,
        url: str,
        status_code: int | None,
        elapsed_ms: float,
        *,
        level: str = "info",
        exc: BaseException | None = None,
    ) -> None:
        extra = {
            "request_id": request_id,
            "url": url,
            "method": "GET",
            "status_code": status_code,
            "elapsed_ms": round(elapsed_ms, 2),
        }
        log_fn = getattr(self.logger, level)
        if exc is not None:
            log_fn("Request failed", extra=extra, exc_info=exc)
        else:
            log_fn("HTTP response received", extra=extra)

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(
            config.BACKOFF_BASE_SECONDS * (2 ** attempt),
            config.BACKOFF_MAX_SECONDS,
        )

    @staticmethod
    def _make_request_id(attempt: int) -> str:
        return f"req-{int(time.time() * 1000)}-{attempt}"

    def _assert_total(self, extracted_count: int) -> None:
        if extracted_count != config.TOTAL_PRODUCTS:
            raise ValueError(
                f"Total extraction mismatch: "
                f"expected {config.TOTAL_PRODUCTS}, got {extracted_count}"
            )