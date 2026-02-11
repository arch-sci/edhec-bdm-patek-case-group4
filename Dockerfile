# Base Image
FROM python:3.10-slim

# Setup
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Our Package
COPY . .
RUN pip install .

# Default Command: Run the data script
#CMD ["python", "-m", "patek_analysis.data"]
# For us, we tell it to run data.py AND model.py .. to show that we can 
CMD ["sh", "-c", "python -m patek_analysis.data && python -m patek_analysis.model"] 