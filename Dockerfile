# Container do S1 — Zonas, ocupação e eventos (equipe VER-6).
#
#   docker build -t ods-s1 .
#   docker run --rm \
#     -e S1_KAFKA_BOOTSTRAP_SERVERS=<host:porta> \
#     -e S1_ZONES_FILE=/app/data/zones_a3.json -e S1_ZONES_VERSION=zones-a3-1 \
#     ods-s1
#
# Toda a configuração vem de variáveis de ambiente (ver s1/__main__.py e o
# README). O ZONES de uma build real pode ser montado por volume
# (-v ./zones.json:/zones/zones.json:ro -e S1_ZONES_FILE=/zones/zones.json).
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY s1/ s1/
COPY data/ data/

RUN useradd --system --no-create-home s1
USER s1

# `docker stop` manda SIGTERM: o S1 termina o laço, entrega o que falta e
# fecha a conexão com o broker.
STOPSIGNAL SIGTERM
ENTRYPOINT ["python", "-m", "s1"]
