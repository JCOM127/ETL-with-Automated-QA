FROM python:3.11-slim

# Install OpenJDK 17 — required by PySpark to start the JVM.
# openjdk-17-jdk-headless is the smallest JDK variant (no GUI libs).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Copy only the dependency manifest first so Docker can cache the pip layer.
COPY pyproject.toml .
COPY etl_challenge/ etl_challenge/

# Install the package and all dev dependencies (pytest, ruff, jupyter).
RUN pip install --no-cache-dir -e ".[dev]"

# Copy remaining project files (tests, notebooks, docs).
COPY tests/ tests/
COPY notebooks/ notebooks/

# Default: run the full test suite.
# Override at runtime: docker run etl pytest -m unit
CMD ["pytest", "tests/", "-v", "--tb=short"]
