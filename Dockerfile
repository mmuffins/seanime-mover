FROM python:3.12-slim
WORKDIR /app
RUN mkdir -p /source /dest /config
COPY mover.py /app/mover.py
CMD ["python", "/app/mover.py"]
