FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy and set up entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Create output directory (as root)
RUN mkdir -p /output

# Create directory for DuckDB ODBC
RUN mkdir -p /opt/duckdb_odbc

# Download and extract DuckDB ODBC driver
RUN curl -L https://github.com/duckdb/duckdb-odbc/releases/download/v1.3.0.0/duckdb_odbc-linux-amd64.zip \
    -o /tmp/duckdb_odbc.zip && \
    unzip /tmp/duckdb_odbc.zip -d /opt/duckdb_odbc && \
    rm /tmp/duckdb_odbc.zip

# Run system-level ODBC setup
RUN cd /opt/duckdb_odbc && \
    chmod +x ./unixodbc_setup.sh && \
    ./unixodbc_setup.sh -s

# Set working directory
WORKDIR /app

# Copy application files
COPY . .

# Install dependencies
ENV UV_CACHE_DIR=/tmp/uv-cache
RUN uv sync --locked

# Volume for output
VOLUME /output

# Set container environment
ENV CONTAINER_ENV=true
ENTRYPOINT ["entrypoint.sh"]
CMD ["--help"]
