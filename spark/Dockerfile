FROM apache/spark:3.5.1-python3.11

USER root

# Set environment variables
ENV PYTHONHASHSEED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    software-properties-common \
    ssh \
    net-tools \
    ca-certificates \
    git \
    build-essential \
    python3-pip \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Python 3 as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Install Delta Lake
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel
RUN pip3 install --no-cache-dir \
    delta-spark==3.2.0 \
    boto3 \
    pandas==2.1.1 \
    numpy==1.26.0 \
    pyarrow==14.0.0 \
    python-dateutil==2.8.2

# Configure S3 dependencies
RUN cd ${SPARK_HOME}/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.480/aws-java-sdk-bundle-1.12.480.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/3.2.0/delta-core_2.12-3.2.0.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

# Create directories for data and jobs
RUN mkdir -p /data /data/tableau_exports /opt/jobs /opt/scripts

# Set working directory
WORKDIR /opt

# Copy configuration
COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# Set permissions
RUN chmod -R 777 /data

# Expose ports for Spark UI (master, worker)
EXPOSE 8080 8081 4040 7077

# Keep container running
CMD ["bash", "-c", "tail -f /dev/null"]