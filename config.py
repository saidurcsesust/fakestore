"""Configuration for extraction scripts."""

import os

API_BASE_URL = os.getenv("API_BASE_URL", "https://fakestoreapi.com")
PRODUCTS_ENDPOINT = os.getenv("PRODUCTS_ENDPOINT", "/products")

MOCKAROO_BASE_URL = os.getenv("MOCKAROO_BASE_URL", "https://api.mockaroo.com")
MOCKAROO_ENDPOINT = os.getenv("MOCKAROO_ENDPOINT", "")
MOCKAROO_SCHEMA_KEY = os.getenv("MOCKAROO_SCHEMA_KEY", "d1484c70")
MOCKAROO_API_KEY = os.getenv("MOCKAROO_API_KEY", "e85a5800")

TOTAL_PRODUCTS = int(os.getenv("TOTAL_PRODUCTS", "20"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "3"))
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "4"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "1"))
BACKOFF_MAX_SECONDS = float(os.getenv("BACKOFF_MAX_SECONDS", "16"))
USER_AGENT = os.getenv("USER_AGENT", "fakestore-extractor/1.0")
