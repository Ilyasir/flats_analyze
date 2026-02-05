FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY parsers/requirements.txt .

RUN apt-get update && \
    pip install --no-cache-dir -r requirements.txt && \
    playwright install --with-deps chromium --only-shell && \
    apt-get purge -y --auto-remove && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /root/.cache/pip && \
    rm -rf /root/.cache/ms-playwright/firefox-* && \
    rm -rf /root/.cache/ms-playwright/webkit-*

COPY parsers/ ./parsers/

ENV PYTHONPATH=/app/parsers

CMD ["python", "parsers/run_mini_parser.py"]
