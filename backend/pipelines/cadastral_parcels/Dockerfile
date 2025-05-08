# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Install system dependencies and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libspatialindex-dev \
    libgeos-dev \
    libproj-dev \
    proj-bin \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster package installation
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir uv

# Set the working directory
WORKDIR /app

# Copy pyproject.toml for dependency installation
COPY pyproject.toml ./

# Create and activate a virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install dependencies
RUN uv pip install --system . --no-cache

# Copy the rest of the application code
COPY . .

# Command to run the application
CMD ["python", "main.py"]