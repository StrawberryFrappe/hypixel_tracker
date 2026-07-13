FROM ghcr.io/astral-sh/uv:0.11.21 AS uv

FROM python:3.13-slim

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1

COPY --from=uv /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY migrations ./migrations
COPY alembic_raw.ini alembic_app.ini ./

RUN uv sync --locked --no-dev --no-group docs \
    && groupadd --gid 10001 bazaar \
    && useradd --uid 10001 --gid bazaar --no-create-home --home-dir /app bazaar \
    && chown -R bazaar:bazaar /app

USER bazaar

CMD ["uvicorn", "bazaar_guru.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
