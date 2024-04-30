# Use an official Python runtime as a base image
FROM python:3.9-slim

# Set environment variables to ensure Python runs in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Install any necessary system dependencies
RUN apt-get update && apt-get install -y gcc libssl-dev

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run the application
CMD ["python", "./consumer_twitter.py"]
