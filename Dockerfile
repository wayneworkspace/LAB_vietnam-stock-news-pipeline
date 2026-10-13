# Dedicated Python container for the Stock pipeline (n8n calls it over HTTP).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 TZ=Asia/Bangkok
WORKDIR /app

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Run as a normal user, not root. uid 1000 matches the usual first user on a Linux host, so the
# bind-mounted ./data stays writable (on Linux create the folder yourself first: mkdir data).
RUN useradd --create-home --uid 1000 app
USER app

# The code is mounted from ./python by docker-compose, so editing a file needs no rebuild
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "/app", "--reload"]
