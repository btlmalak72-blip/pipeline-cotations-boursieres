FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.2.15 redis==6.4.0 websockets==15.0.1 dagster==1.11.16 dagster-webserver==1.11.16
COPY bridge.py collecteur.py orchestration.py ./
