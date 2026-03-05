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

## Complete Setup Procedure
1. Create and activate a Python virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Export values you want to override, for example:
   ```bash
   export TOTAL_PRODUCTS=20
   export CHUNK_SIZE=5
   export RETRY_LIMIT=3
   export CONCURRENCY_LIMIT=4
   export MOCKAROO_SCHEMA_KEY=your_mockaroo_schema_key
   export MOCKAROO_API_KEY=your_mockaroo_api_key
   ```

## Commands to Run the Scripts
- Run FakeStore sync + async extraction:
  ```bash
  python extract_products.py
  ```
- Run Mockaroo sync + async extraction:
  ```bash
  python extract_products_mockaroo.py
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