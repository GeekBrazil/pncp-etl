FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn reportlab psycopg2-binary sse-starlette
COPY . .
CMD ["uvicorn", "pncp_dashboard:app", "--host", "0.0.0.0", "--port", "8790", "--reload"]
