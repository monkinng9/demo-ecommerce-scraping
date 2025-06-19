# Dockerfile for the e_commerce_scrap project
# ---------------------------------------------
# This Dockerfile sets up an environment for running web scraping tasks.
# It includes:
#   - Python 3.9 (via apache/airflow base image)
#   - OpenJDK 8 (for BrowserMob Proxy)
#   - Chromium and ChromeDriver (for Selenium)
#   - BrowserMob Proxy (for capturing network traffic)
#   - Python dependencies specified in requirements.txt
#
# Build command (run from the project root directory):
#   docker build -t e_commerce_scrap -f docker/Dockerfile .
#
# Stages:
#   1. jdk_builder: Builds OpenJDK 8 from an Ubuntu base.
#   2. Main image: Based on apache/airflow, installs system dependencies,
#      copies OpenJDK 8 from the builder stage, installs Python packages,
#      downloads and sets up BrowserMob Proxy, and copies the application code.
#
# Key Environment Variables:
#   - JAVA_HOME: Set to the OpenJDK 8 installation path.
#
# Default User: airflow (though root is used for installations)
# ---------------------------------------------

# run on root dir: docker build -t e_commerce_scrap -f docker/Dockerfile .

# Stage 1: Build OpenJDK 8 from Ubuntu
FROM ubuntu:20.04 AS jdk_builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Use a specific version of Airflow with Python 3.9 for reproducibility
FROM apache/airflow:3.0.2-python3.10

# Set the working directory in the container
WORKDIR /opt/airflow

# Install system dependencies
USER root

ENV DEBIAN_FRONTEND=noninteractive

# Copy OpenJDK 8 from the builder stage
COPY --from=jdk_builder /usr/lib/jvm/java-8-openjdk-amd64 /usr/lib/jvm/java-8-openjdk-amd64

# Set JAVA_HOME environment variable for Java 8
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Chromium, ChromeDriver, and dependencies for Selenium
RUN apt-get update && \
    apt-get install -y wget unzip ca-certificates chromium chromium-driver \
    libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 libx11-6 libxfixes3 libxtst6 libxrandr2 \
    libcups2 libasound2 libdbus-glib-1-2 libgbm1 libxshmfence1 --no-install-recommends && \
    # Clean up
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* 

USER airflow

# Copy the application code and proxy tool
COPY . /opt/airflow/app

USER root
RUN chown -R airflow /opt/airflow/app && chmod -R u+w /opt/airflow/app
USER airflow

# Download and install BrowserMob Proxy
# USER airflow is active from previous USER airflow directive.
# Download and extract browsermob-proxy to a temporary location as airflow user
RUN mkdir -p /tmp/bmp && \
    cd /tmp/bmp && \
    wget --progress=dot:giga -O browsermob-proxy-2.1.4-bin.zip https://github.com/lightbody/browsermob-proxy/releases/download/browsermob-proxy-2.1.4/browsermob-proxy-2.1.4-bin.zip && \
    unzip -o browsermob-proxy-2.1.4-bin.zip && \
    rm browsermob-proxy-2.1.4-bin.zip

# Switch to root to move files into /opt/airflow/app, set permissions, and clean up
USER root
RUN rm -rf /opt/browsermob-proxy-2.1.4 && \
    mkdir -p /opt/browsermob-proxy-2.1.4 && \
    # Check if the unzipped directory exists before moving
    if [ -d /tmp/bmp/browsermob-proxy-2.1.4 ]; then \
        mv /tmp/bmp/browsermob-proxy-2.1.4/* /opt/browsermob-proxy-2.1.4/ ; \
    else \
        echo "Error: Expected directory /tmp/bmp/browsermob-proxy-2.1.4 not found after unzip." ; \
        exit 1 ; \
    fi && \
    chown -R airflow /opt/browsermob-proxy-2.1.4 && \
    rm -rf /tmp/bmp



# Switch back to airflow user for subsequent operations
USER airflow

COPY credentials.json .
COPY private_key.json .
COPY requirements.txt .
RUN pip install -r requirements.txt

# Command to run the script
# This will run when the container starts
# CMD ["python", "app/selenium_browser_proxy.py"]
