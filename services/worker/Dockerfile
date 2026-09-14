FROM python:3.12-slim AS build
WORKDIR /src
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY pyproject.toml ./
COPY services ./services
RUN pip install --no-cache-dir .

FROM python:3.12-slim AS runtime
RUN useradd --uid 10001 --create-home app
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
WORKDIR /app
USER app
ENTRYPOINT ["python", "-m", "services.worker.main"]
