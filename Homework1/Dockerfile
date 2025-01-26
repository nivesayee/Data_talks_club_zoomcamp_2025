# Dockerfile
FROM python:3.12.8

WORKDIR /app

# Copy the current directory to the container
COPY . .

# Install required Python packages
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2

# Default command to keep the container running
CMD ["tail", "-f", "/dev/null"]
