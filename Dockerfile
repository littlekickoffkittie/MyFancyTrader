# ── FangBlenny Bot — Railway Dockerfile ──────────────────────────
FROM python:3.11-slim

# system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc curl git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# install Python deps first (cache layer)
COPY requirements_deploy.txt .
RUN pip install --no-cache-dir -r requirements_deploy.txt

# copy project
COPY . .

# create dirs the bot writes to
RUN mkdir -p bots logs

# Railway sets $PORT; default 8080 for local
ENV PORT=8080
EXPOSE 8080

# entrypoint
CMD ["python", "main.py"]
