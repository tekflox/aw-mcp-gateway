FROM python:3.12-slim

LABEL org.opencontainers.image.source="https://github.com/tekflox/aw-mcp-gateway"
LABEL org.opencontainers.image.description="Agentic Workspace MCP Gateway"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 9200

CMD ["python3", "-m", "gateway.server"]
