FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# app.json is expected to be mounted/provided at runtime — not baked in.
CMD ["python3", "-m", "connector.main"]
