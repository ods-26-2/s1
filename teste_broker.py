"""Teste do broker Kafka: só LÊ, não publica nada.

Uso:  python teste_broker.py host:porta

Se o broker pedir segurança, passe as mesmas variáveis do S1, por exemplo:
  S1_KAFKA_SECURITY_PROTOCOL=SASL_SSL  S1_KAFKA_SASL_MECHANISMS=PLAIN
  S1_KAFKA_SASL_USERNAME=...           S1_KAFKA_SASL_PASSWORD=...
(`_` vira `.` e `__` vira `_`, como no S1.)
"""
import json
import os
import sys
import uuid

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

BROKER = sys.argv[1] if len(sys.argv) > 1 else "172.18.4.56:9092"
TOPICOS_S1 = [
    "ods.i2.tracks", "ods.i3.calibration", "ods.session.started", "ods.artifact.published",
    "ods.s1.position", "ods.s1.zone_transition", "ods.s1.zone_occupancy",
]


def chave_kafka(sufixo):
    return ".".join(p.replace("\0", "_") for p in sufixo.lower().replace("__", "\0").split("_"))


extras = {
    chave_kafka(nome[len("S1_KAFKA_"):]): valor
    for nome, valor in os.environ.items()
    if nome.startswith("S1_KAFKA_") and nome != "S1_KAFKA_BOOTSTRAP_SERVERS" and valor.strip()
}
base = {"bootstrap.servers": BROKER, "log_level": 0, **extras}
seguranca = {k: ("***" if "password" in k else v) for k, v in extras.items()}

print(f"1) Conectando a {BROKER} " + (f"com {seguranca}" if seguranca else "sem autenticação") + " ...")
try:
    meta = AdminClient(base).list_topics(timeout=10)
except Exception as erro:
    sys.exit(f"   FALHOU: {erro}")
print("   OK. Endereço que o broker anuncia:", ", ".join(f"{b.host}:{b.port}" for b in meta.brokers.values()))

print("2) Tópicos do S1:")
for topico in TOPICOS_S1:
    info = meta.topics.get(topico)
    print(f"   {topico:<24} " + (f"existe ({len(info.partitions)} partição(ões))" if info else "NÃO EXISTE"))

if "ods.i2.tracks" not in meta.topics:
    sys.exit("3) Sem o tópico ods.i2.tracks, não há rastros para espiar.")

print("3) Espiando até 5 mensagens de ods.i2.tracks (só leitura) ...")
leitor = Consumer({**base, "group.id": f"teste-leitura-{uuid.uuid4().hex[:8]}",
                   "auto.offset.reset": "earliest", "enable.auto.commit": False})
leitor.subscribe(["ods.i2.tracks"])
lidas = 0
for _ in range(20):
    registro = leitor.poll(1.0)
    if registro is None or registro.error():
        continue
    lidas += 1
    try:
        msg = json.loads(registro.value())
        print(f"   schema={msg.get('schema')}  source_id={msg.get('source_id')}  session_id={msg.get('session_id')}")
    except ValueError:
        print("   mensagem que não é JSON:", registro.value()[:80])
    if lidas == 5:
        break
leitor.close()
print(f"   {lidas} mensagem(ns) lida(s).")
