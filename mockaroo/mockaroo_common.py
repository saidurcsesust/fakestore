"""Shared helpers for Mockaroo extraction scripts."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import config
import sync_async.json_formatter as Formatter


def build_run_timestamps() -> tuple[str, str]:
    """Return current date and time strings in required output format."""
    return datetime.now().strftime("%y%m%d"), datetime.now().strftime("%H%M%S")


def setup_json_logger(script_name: str, date_str: str, time_str: str) -> logging.Logger:
    """Create and return a JSON logger with required file path naming."""
    log_dir = os.path.join("logs", date_str)
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{script_name}_{date_str}_{time_str}.json")

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(Formatter.JsonFormatter())
    logger.addHandler(handler)
    return logger


def validate_mockaroo_config(require_concurrency: bool = False) -> int:
    """Validate extractor config and return expected chunk count."""
    if config.CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be greater than 0")
    if config.TOTAL_PRODUCTS <= 0:
        raise ValueError("TOTAL_PRODUCTS must be greater than 0")
    if config.RETRY_LIMIT < 0:
        raise ValueError("RETRY_LIMIT must be 0 or greater")
    if require_concurrency and config.CONCURRENCY_LIMIT <= 0:
        raise ValueError("CONCURRENCY_LIMIT must be greater than 0")
    if config.TOTAL_PRODUCTS % config.CHUNK_SIZE != 0:
        raise ValueError("TOTAL_PRODUCTS must be divisible by CHUNK_SIZE")
    if not config.MOCKAROO_API_KEY:
        raise ValueError("MOCKAROO_API_KEY is required")
    if not config.MOCKAROO_SCHEMA_KEY and not config.MOCKAROO_ENDPOINT:
        raise ValueError("MOCKAROO_SCHEMA_KEY is required when MOCKAROO_ENDPOINT is empty")
    return config.TOTAL_PRODUCTS // config.CHUNK_SIZE


def build_mockaroo_headers() -> Dict[str, str]:
    """Build HTTP headers for Mockaroo JSON requests."""
    headers = {
        "User-Agent": config.USER_AGENT,
        "Accept": "application/json",
    }
    if config.MOCKAROO_API_KEY:
        headers["X-API-Key"] = config.MOCKAROO_API_KEY
    return headers


def build_mockaroo_url() -> str:
    """Build Mockaroo endpoint URL."""
    if config.MOCKAROO_ENDPOINT:
        return f"{config.MOCKAROO_BASE_URL}{config.MOCKAROO_ENDPOINT}"
    return f"{config.MOCKAROO_BASE_URL}/api/{config.MOCKAROO_SCHEMA_KEY}.json"


def build_mockaroo_params() -> Dict[str, Any]:
    """Build query params for one Mockaroo chunk request."""
    return {
        "count": config.CHUNK_SIZE,
        "key": config.MOCKAROO_API_KEY,
        "format": "json",
    }


def parse_mockaroo_json_payload(content_type: str, payload: Any) -> List[Dict[str, Any]]:
    """Validate and return a JSON-list payload from Mockaroo response data."""
    if "application/json" not in content_type.lower():
        raise ValueError(f"Mockaroo returned non-JSON content type: {content_type}")
    if not isinstance(payload, list):
        raise ValueError("Mockaroo JSON response is not a list")
    return payload


def ensure_chunk_size(products: List[Dict[str, Any]], chunk_number: int) -> None:
    """Ensure each chunk contains exactly CHUNK_SIZE records."""
    if len(products) != config.CHUNK_SIZE:
        raise ValueError(
            f"Chunk size mismatch at chunk {chunk_number}: "
            f"expected {config.CHUNK_SIZE}, got {len(products)}"
        )


def write_chunk_file(
    chunk_number: int,
    products: List[Dict[str, Any]],
    date_str: str,
    time_str: str,
    logger: logging.Logger,
    type: str
) -> None:
    """Write one products chunk to the required JSON data path."""
    os.makedirs(os.path.join("data", "json"), exist_ok=True)
    file_path = os.path.join(
        "data",
        "json",
        f"products_{type}_{chunk_number}_{date_str}_{time_str}.json",
    )
    with open(file_path, "w", encoding="utf-8") as output_file:
        json.dump(products, output_file, ensure_ascii=True, indent=2)
    logger.info(f"Wrote chunk file: {file_path}")
