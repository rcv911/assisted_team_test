FROM python:3.7-alpine

RUN apk update && apk add --virtual build-deps \
    autoconf \
    automake \
    g++ \
    make \
    build-deps \
    gcc \
    python3-dev \
    musl-dev

RUN pip install --no-cache-dir \
    pytoml \
    python-dateutil \
    aiohttp \
    aiohttp-rest-api

RUN apk del build-deps gcc

COPY . /app

VOLUME ["/config/config.toml"]

WORKDIR /app

ENTRYPOINT ["python", "test_api.py", "-c", "/config/config.toml"]
