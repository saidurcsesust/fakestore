# FakeStore Product Extractors

## Purpose of Each Script
- `extract_products_sync.py`: Synchronous class-based extractor (`ProductExtractorSync`) that fetches products in chunks, writes each chunk to `data/json/`, and logs JSON events to `logs/<date>/`.
- `extract_products_async.py`: Asynchronous class-based extractor (`ProductExtractorAsync`) with configurable concurrency to fetch chunks faster, then writes chunk files and JSON logs.

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
3.
   Then export values you want to override, for example:
   ```bash
   export TOTAL_PRODUCTS=20
   export CHUNK_SIZE=5
   export RETRY_LIMIT=3
   export CONCURRENCY_LIMIT=4
   ```

## Commands to Run the Scripts
- Run synchronous extraction:
  ```bash
  python extract_products_sync.py
  ```


## Output Locations
- Data chunks: `data/json/products_<chunk_number>_<date>_<time>.json`
- Logs: `logs/<date>/<script_name>_<date>_<time>.json`

## Notes
- Date format: `YYMMDD`
- Time format: `HHMMSS`
- If `TOTAL_PRODUCTS` is not divisible by `CHUNK_SIZE`, scripts stop with validation error.
- Each chunk is validated to contain exactly `CHUNK_SIZE` products.
- Total chunks are validated as `TOTAL_PRODUCTS / CHUNK_SIZE`.
