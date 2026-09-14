FROM python:3.12-slim AS build
WORKDIR /src
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY pyproject.toml ./
COPY services ./services
RUN pip install --no-cache-dir .

# Used by the `tests` compose service. Not part of the runtime image.
FROM build AS tests
RUN pip install --no-cache-dir "pytest>=9.1.1" "pytest-asyncio>=1.4.0"

FROM python:3.12-slim AS runtime
RUN useradd --uid 10001 --create-home app
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
WORKDIR /app
COPY config /app/config
USER app
EXPOSE 5001
ENTRYPOINT ["python", "-m", "services.executor.main"]
