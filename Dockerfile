FROM python:3.11-slim

# System deps for pandas/lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Workdir
WORKDIR /app

# Copy deps early for caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY digest_maker_bot.py /app/digest_maker_bot.py

# Security: do not bake tokens into the image. Use env BOT_TOKEN at runtime.
ENV PYTHONUNBUFFERED=1

# Optional: create a non-root user (good practice)
# RUN useradd -ms /bin/bash appuser
# USER appuser

CMD ["python", "digest_maker_bot.py"]