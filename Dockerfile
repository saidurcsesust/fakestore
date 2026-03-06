FROM python:3.12-slim

ARG UID=1000
ARG GID=1000

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY extract_products.py .
COPY extract_products_mockaroo.py .
COPY sync_async/json_formatter.py .
COPY sync_async/base_extractor.py .
COPY sync_async/extract_products_sync.py .
COPY sync_async/extract_products_async.py .
COPY mockaroo/mockaroo_common.py .
COPY mockaroo/extract_products_mockaroo_sync.py .
COPY mockaroo/extract_products_mockaroo_async.py .

RUN groupadd -g $GID appuser \
    && useradd -u $UID -g $GID -m appuser \
    && chown -R appuser:appuser /app

USER appuser

RUN mkdir -p /app/data/json /app/logs

CMD ["python", "-u", "extract_products.py"]