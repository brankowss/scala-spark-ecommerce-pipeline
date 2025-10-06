# Use an official lightweight Python image as the base image.
# The 'slim' version is a good choice as it includes the necessary OS libraries
# without the extra bloat of a full distribution, keeping the final image smaller.
FROM python:3.9-slim

# Install the 'procps' package, which contains the 'ps' and 'kill' commands.
# This makes our container more robust for process management.
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container to /app.
# All subsequent commands (COPY, RUN, CMD) will be executed from this path.
WORKDIR /app

# Copy the requirements file first.
# Copying and installing dependencies.
COPY requirements.txt .

# Install the Python dependencies specified in requirements.txt.
# The '--no-cache-dir' flag prevents pip from storing the downloaded packages,
# which further reduces the size of the final image.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main application script into the container.
COPY . .

# This entrypoint will keep the container running indefinitely,
# but it will NOT automatically start the producer.py script. This gives us manual control.
ENTRYPOINT ["tail", "-f", "/dev/null"]
