FROM python:3.13-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

# Example handlers; `python -m pgqueue worker` imports them from the working directory.
COPY examples ./examples

EXPOSE 8000
CMD ["python", "-m", "pgqueue", "api", "--host", "0.0.0.0", "--port", "8000"]
