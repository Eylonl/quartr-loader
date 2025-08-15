FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# System deps for Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
  libc6 libnss3 libatk1.0-0 libatk-bridge2.0-0 libxcomposite1 libxrandr2 libxdamage1 \
  libgbm1 libasound2 libpangocairo-1.0-0 libpango-1.0-0 libxshmfence1 libglib2.0-0 \
  libx11-6 libx11-xcb1 libxcb1 libxext6 libxfixes3 libdrm2 libxkbcommon0 libcups2 \
  libexpat1 libjpeg62-turbo libpng16-16 libxrender1 fonts-liberation ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && python -m playwright install chromium

COPY . .

# Bind to Railway's dynamic $PORT (fallback 8080 for local)
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
