# Use an official Spark image with Python support
FROM apache/spark-py:v3.5.0 # Using a specific version is good practice

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
# Ensure pip is up-to-date and install build-essentials for any native compilation
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application source code into the container
COPY src/ ./src/

# Command to run the application
# The application creates its own SparkSession, so direct python execution is fine.
CMD ["python", "src/streaming_app.py"]
