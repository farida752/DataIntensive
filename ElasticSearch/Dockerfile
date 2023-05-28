# Use an official Python runtime as the base image
FROM python:3.9.7

# Set the working directory in the container
WORKDIR /app

# Copy the Python script to the container
COPY main.py /app

# Copy the requirements file to the container
COPY requirements.txt /app

# Install necessary dependencies
RUN pip install -r requirements.txt

# Run the Python script
CMD ["python", "main.py"]
