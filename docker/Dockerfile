FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    RXLAKE_LAKE_ROOT=/data/lake \
    RXLAKE_LANDING_ROOT=/data/landing

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY conf ./conf
RUN pip install --no-cache-dir -e .

# Pre-fetch the Delta jars so the first run doesn't need to download them
RUN python -c "from rxlake.spark import get_spark; get_spark().stop()"

ENTRYPOINT ["rxlake"]
CMD ["--help"]
