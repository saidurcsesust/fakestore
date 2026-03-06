# Product Extractors (FakeStore + Mockaroo)

## Purpose of Each Script
- `extract_products.py`: Unified entry point that runs FakeStore sync extraction followed by async extraction.
- `extract_products_sync.py`: Synchronous class-based extractor (`ProductExtractorSync`) that fetches products in chunks, writes each chunk to `data/json/`, and logs JSON events to `logs/<date>/`.
- `extract_products_async.py`: Asynchronous class-based extractor (`ProductExtractorAsync`) with configurable concurrency to fetch chunks faster, then writes chunk files and JSON logs.
- `extract_products_mockaroo.py`: Unified entry point that runs Mockaroo sync extraction followed by async extraction.
- `extract_products_mockaroo_sync.py`: Synchronous class-based extractor (`ProductExtractorMockarooSync`) for Mockaroo.
- `extract_products_mockaroo_async.py`: Asynchronous class-based extractor (`ProductExtractorMockarooAsync`) for Mockaroo.
- `base_extractor.py`: Shared base class with common logic (logging, config validation, URL building, file writing, retry helpers).
- `mockaroo_common.py`: Shared helpers for Mockaroo extractors (URL/header/param builders, payload parsing, chunk validation).

---

## Running with Docker

### Prerequisites
- Docker and Docker Compose installed

### Build and run both extractors
```bash
docker-compose up --build
```

`fakestore` runs first; `mockaroo` starts automatically once it completes successfully.

### Run a single extractor
```bash
docker-compose up --build fakestore
docker-compose up --build mockaroo
```

### Check live output while running
```bash
docker-compose logs -f
```

### Confirm output data was written
```bash
ls data/json/
```

### Clean up containers
```bash
docker-compose down
```


## Output Locations
- Data chunks: `data/json/products_<type>_<chunk_number>_<date>_<time>.json`
- Logs: `logs/<date>/<script_name>_<date>_<time>.json`

## Notes
- Date format: `YYMMDD`
- Time format: `HHMMSS`
- If `TOTAL_PRODUCTS` is not divisible by `CHUNK_SIZE`, scripts stop with validation error.
- Each chunk is validated to contain exactly `CHUNK_SIZE` products.
- Total chunks are validated as `TOTAL_PRODUCTS / CHUNK_SIZE`.
- `MOCKAROO_ENDPOINT` should be left empty in `.env` to use the schema-key-based URL (`/api/<schema_key>.json`).