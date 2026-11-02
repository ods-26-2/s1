from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

from s1.domain import S1Service
from s1.ingestion import default_artifact_id, load_json, load_messages, parse_zones
from s1.output import POSITION, ZONE_OCCUPANCY, ZONE_TRANSITION, S1Publisher
from s1.runtime import S1Runtime

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
OUTPUTS = (POSITION, ZONE_TRANSITION, ZONE_OCCUPANCY)

def _describe(message: dict) -> str:
    payload = message["payload"]
    head = f"[{message['captured_at']}] {message['source_id']} #{message['sequence']:<3}"
    if message["schema"] == POSITION:
        items = ", ".join(
            f"{item['track_id']} {item['point_px']} zonas={item['zone_ids']}" for item in payload["positions"]
        )
        return f"{head} position   quadro {payload['frame_index']}: {items or 'nenhum rastro'}"
    if message["schema"] == ZONE_TRANSITION:
        dwell = f", dwell_s={payload['dwell_s']}" if "dwell_s" in payload else ""
        return f"{head} transition {payload['transition']} {payload['track_id']} em {payload['zone_id']}{dwell}"
    if message["schema"] == ZONE_OCCUPANCY:
        return (
            f"{head} occupancy  {payload['zone_id']}: count={payload['count']} "
            f"{payload['count_by_class']} track_ids={payload['track_ids']}"
        )
    return head

def run_local(zones, incoming, output_path) -> int:
    """O S1 roda aqui dentro; o simulador entrega as mensagens e mostra as saídas."""

    runtime = S1Runtime(S1Service(zones), S1Publisher(), zones_artifact_id=default_artifact_id(zones.artifact_version))
    published = []
    for message in incoming:
        for output in runtime.handle(message):
            published.append(output)
            print(_describe(output))

    counts = {schema: sum(m["schema"] == schema for m in published) for schema in OUTPUTS}
    print("\n" + ", ".join(f"{count} {schema}" for schema, count in counts.items()) + " publicada(s).")
    if output_path:
        path = Path(output_path)
        path.write_text(json.dumps(published, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Gravado em: {path}")
    return 0

def run_kafka(zones, calibration, demo, broker: str, wait_s: float) -> int:
    """O S1 roda em outro processo (container); o simulador publica as
    entradas no Kafka, lê as saídas e confere."""

    import jsonschema
    from confluent_kafka import Consumer, Producer

    camera = zones.source_ids[0]
    # Começa a ler antes de publicar, a partir do fim de cada tópico.
    consumer = Consumer({"bootstrap.servers": broker, "group.id": f"simulador-{uuid.uuid4().hex[:8]}",
                         "auto.offset.reset": "latest"})
    consumer.subscribe(list(OUTPUTS))
    deadline = time.time() + 30
    while not consumer.assignment():
        if time.time() > deadline:
            print(f"FALHA sem resposta do broker {broker} (rodou `docker compose up -d`?)")
            return 1
        consumer.poll(0.5)

    producer = Producer({"bootstrap.servers": broker})

    def send(message, topic=None, value=None):
        producer.produce(
            topic or message["schema"],
            key=(message or {}).get("source_id", camera).encode(),
            value=value if value is not None else json.dumps(message).encode(),
        )
        producer.flush(10)

    # Uma sessão nova a cada execução, para poder repetir com o mesmo S1 no ar.
    session = str(uuid.uuid4())
    fresh = lambda m: {**m, "message_id": str(uuid.uuid4()), "session_id": session}  # noqa: E731
    calibration = [{**m, "message_id": str(uuid.uuid4())} for m in calibration]
    demo = [fresh(m) for m in demo]
    sample_frame = next(m for m in demo if m["schema"] == "ods.i2.tracks")

    for message in calibration:
        send(message)
    time.sleep(1)  # tópicos diferentes não têm ordem entre si
    send(None, topic=sample_frame["schema"], value=b"{isto nao e json")      # descartada
    send({**fresh(sample_frame), "schema_version": "2.0"})                    # versão maior 2: descartada
    send({**fresh(sample_frame), "source_id": "cam_de_outra_aplicacao"})      # fora do ZONES: ignorada
    for message in demo:
        send(message)
        if message["schema"] == "ods.session.started":
            time.sleep(1)
    send(sample_frame)                                                        # entrega repetida: ignorada

    runtime = S1Runtime(S1Service(zones), S1Publisher(), zones_artifact_id=default_artifact_id(zones.artifact_version))
    expected = [out for message in calibration + demo for out in runtime.handle(message)]

    received, deadline = [], time.time() + wait_s
    while time.time() < deadline and len(received) < len(expected) + 1:
        record = consumer.poll(0.5)
        if record is not None and not record.error():
            received.append((record.topic(), record.key(), json.loads(record.value())))
    consumer.close()

    validators = {name: jsonschema.Draft202012Validator(load_json(ROOT / "contracts" / f"{name}.schema.json"))
                  for name in OUTPUTS}
    strip = lambda m: {k: v for k, v in m.items() if k not in ("message_id", "published_at")}  # noqa: E731
    mine = [m for _, _, m in received if m["session_id"] == session]
    # A ordem só é garantida dentro de uma partição; o sequence restaura a ordem.
    got = sorted((strip(m) for m in mine), key=lambda m: m["sequence"])

    checks = {
        f"recebidas {len(mine)} de {len(expected)} mensagens": len(mine) == len(expected),
        "todas seguem os JSON Schemas": all(validators[m["schema"]].is_valid(m) for m in mine),
        "cada uma no tópico do seu schema": all(t == m["schema"] for t, _, m in received),
        f"chave = source_id ({camera})": all(k == m["source_id"].encode() for _, k, m in received),
        "iguais às do modo sem Kafka": got == [strip(m) for m in expected],
    }
    for description, ok in checks.items():
        print(("OK   " if ok else "FALHA"), description)
    return 0 if all(checks.values()) else 1

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--zonas", default=str(DATA_DIR / "zones_a3.json"), help="Conteúdo do artefato ZONES.")
    parser.add_argument("--versao-zonas", default="zones-a3-1", help="artifact_version do ZONES carregado.")
    parser.add_argument("--entrada", default=str(DATA_DIR / "entrada_demo.json"), help="Mensagens de entrada, na ordem de chegada.")
    parser.add_argument("--calibracao", help="Mensagens ods.i3.calibration a entregar antes da entrada (opcional; com --kafka, padrão data/i3_calibration.json).")
    parser.add_argument("--saida", help="Grava as mensagens publicadas pelo S1 neste arquivo (opcional).")
    parser.add_argument("--kafka", metavar="HOST:PORTA", help="Testa um S1 rodando de verdade neste broker Kafka.")
    parser.add_argument("--espera", type=float, default=20, help="Com --kafka: segundos esperando as saídas.")
    args = parser.parse_args()

    zones = parse_zones(load_json(args.zonas), args.versao_zonas)
    demo = load_messages(args.entrada)

    if args.kafka:
        calibration = load_messages(args.calibracao or DATA_DIR / "i3_calibration.json")
        return run_kafka(zones, calibration, demo, args.kafka, args.espera)

    incoming = (load_messages(args.calibracao) if args.calibracao else []) + demo
    print(
        f"ZONES {zones.artifact_version} em {zones.coordinates} para {list(zones.source_ids)}: "
        f"{len(zones.zones)} zona(s); {len(incoming)} mensagem(ns) de entrada.\n"
    )
    return run_local(zones, incoming, args.saida)

if __name__ == "__main__":
    sys.exit(main())