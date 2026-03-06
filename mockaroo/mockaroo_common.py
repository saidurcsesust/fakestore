from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import config
import json_formatter as Formatter

RETRY_LIMIT = 3
CHUNK_SIZE = 5
TOTAL_PRODUCTS = 20
CONCURRENCY_LIMIT = 4


def build_run_timestamps() -> tuple[str, str]:
    """Return current date and time strings for file naming."""
    now = datetime.now()
    return now.strftime("%y%m%d"), now.strftime("%H%M%S")


def setup_json_logger(script_name: str, date_str: str, time_str: str) -> logging.Logger:
    """Create and configure a JSON file logger."""
    log_dir = resolve_log_dir(date_str)
    log_path = os.path.join(log_dir, f"{script_name}_{date_str}_{time_str}.json")

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(Formatter.JsonFormatter())
    logger.addHandler(handler)
    return logger


def resolve_log_dir(date_str: str) -> str:
    """Return first writable log directory for current run date."""
    preferred_dirs = []
    env_log_dir = os.getenv("LOG_DIR")
    if env_log_dir:
        preferred_dirs.append(env_log_dir)
    preferred_dirs.extend(("/app/logs", "logs", "/tmp/logs"))

    for base_dir in preferred_dirs:
        candidate = os.path.join(base_dir, date_str)
        try:
            os.makedirs(candidate, exist_ok=True)
            return candidate
        except OSError:
            continue
    raise PermissionError("Unable to create a writable log directory")


def validate_mockaroo_config(require_concurrency: bool = False) -> int:
    """Validate config values and return expected number of chunks."""
    if CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be greater than 0")
    if TOTAL_PRODUCTS <= 0:
        raise ValueError("TOTAL_PRODUCTS must be greater than 0")
    if RETRY_LIMIT < 0:
        raise ValueError("RETRY_LIMIT must be 0 or greater")
    if require_concurrency and CONCURRENCY_LIMIT <= 0:
        raise ValueError("CONCURRENCY_LIMIT must be greater than 0")
    if TOTAL_PRODUCTS % CHUNK_SIZE != 0:
        raise ValueError("TOTAL_PRODUCTS must be divisible by CHUNK_SIZE")
    if not config.MOCKAROO_API_KEY:
        raise ValueError("MOCKAROO_API_KEY is required")
    if not config.MOCKAROO_SCHEMA_KEY:
        raise ValueError("MOCKAROO_SCHEMA_KEY is required")
    return TOTAL_PRODUCTS // CHUNK_SIZE


def build_mockaroo_headers() -> Dict[str, str]:
    """Build HTTP headers for Mockaroo requests."""
    return {
        "User-Agent": config.USER_AGENT,
        "Accept": "application/json",
    }


def build_mockaroo_url() -> str:
    """Build Mockaroo base URL using schema key."""
    return f"{config.MOCKAROO_BASE_URL}/api/{config.MOCKAROO_SCHEMA_KEY}.json"


def build_mockaroo_params(count: int) -> Dict[str, Any]:
    """Build Mockaroo query parameters with record count and API key."""
    return {
        "count": count,
        "key": config.MOCKAROO_API_KEY,
    }


def parse_mockaroo_json_payload(content_type: str, payload: Any) -> List[Dict[str, Any]]:
    """Validate and return Mockaroo JSON response as a list."""
    if "application/json" not in content_type.lower():
        raise ValueError(f"Mockaroo returned non-JSON content type: {content_type}")
    if not isinstance(payload, list):
        raise ValueError("Mockaroo JSON response is not a list")
    return payload


def ensure_chunk_size(products: List[Dict[str, Any]], chunk_number: int) -> None:
    """Assert the chunk contains exactly CHUNK_SIZE products."""
    if len(products) != CHUNK_SIZE:
        raise ValueError(
            f"Chunk size mismatch at chunk {chunk_number}: "
            f"expected {CHUNK_SIZE}, got {len(products)}"
        )


def write_chunk_file(
    chunk_number: int,
    products: List[Dict[str, Any]],
    date_str: str,
    time_str: str,
    logger: logging.Logger,
    label: str,
) -> None:
    """Write a list of products to a JSON chunk file."""
    out_dir = os.path.join("data", "mockaroo", label)
    os.makedirs(out_dir, exist_ok=True)
    file_path = os.path.join(
        out_dir,
        f"products_mockaroo_{label}_{chunk_number}_{date_str}_{time_str}.json",
    )
    with open(file_path, "w", encoding="utf-8") as output_file:
        json.dump(products, output_file, ensure_ascii=True, indent=2)
    logger.info(f"Wrote chunk file: {file_path}")
