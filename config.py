import os

# API Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://fakestoreapi.com")
PRODUCTS_ENDPOINT = os.getenv("PRODUCTS_ENDPOINT", "/products")

# Mockaroo configuration
MOCKAROO_BASE_URL = os.getenv("MOCKAROO_BASE_URL", "https://api.mockaroo.com")
MOCKAROO_SCHEMA_KEY = os.getenv("MOCKAROO_SCHEMA_KEY", "d1484c70")
MOCKAROO_API_KEY = os.getenv("MOCKAROO_API_KEY", "5fcfa850")

# HTTP Settings
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "1"))
BACKOFF_MAX_SECONDS = float(os.getenv("BACKOFF_MAX_SECONDS", "16"))
USER_AGENT = os.getenv("USER_AGENT", "fakestore-extractor/1.0")
