# Use official Python runtime as base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-mysql-client \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Create necessary directories
RUN mkdir -p logs backup

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application files
COPY config.py .
COPY migration.py .
COPY validator.py .
COPY rollback.py .
COPY main.py .
COPY backup_v1.py .
COPY test_migration.py .
COPY duplicate_resolver.py .

# Copy .env.example as .env template
COPY .env.example .env.example

# Set proper permissions
RUN chmod +x main.py && \
    chmod 755 logs backup

# Create non-root user
RUN useradd -m -u 1000 migrator && \
    chown -R migrator:migrator /app

# Switch to non-root user
USER migrator

# Default command
CMD ["python", "main.py"]