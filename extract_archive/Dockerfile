FROM python:3.8-slim

RUN apt-get update && apt-get install -y postgresql-client

# Copy the requirements.txt file and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "archive.py"]