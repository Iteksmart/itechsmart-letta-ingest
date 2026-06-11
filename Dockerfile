FROM python:3.12-slim
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
COPY app.py /app/app.py
EXPOSE 8100
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD ["python3", "-c", "import urllib.request,sys;sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8100/health', timeout=4).status == 200 else 1)"]
CMD ["python3", "/app/app.py"]
