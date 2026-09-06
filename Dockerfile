FROM python:3.14-slim-bookworm

# Берём uv из официального образа, а приложение запускаем на slim-образе Python.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Зависимости кешируются отдельно от исходного кода.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

CMD ["uv", "run", "--frozen", "--no-dev", "--no-sync", "kafka-app", "--help"]
