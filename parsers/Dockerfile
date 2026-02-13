FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/parsers

COPY parsers/requirements.txt .

# скачиваем chromium для playwright, со всеми зависимостями
RUN pip install --no-cache-dir -r requirements.txt && \
    playwright install --with-deps chromium --only-shell && \
    apt-get purge -y --auto-remove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY parsers/ ./parsers/
# tini для управления процессами внутри контейнера (чтобы не было зомби-процессов при завершении)
ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["python", "parsers/run_parser.py"]