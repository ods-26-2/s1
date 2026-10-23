"""Simulador do componente S1 - Zonas, ocupação e eventos.

Faz o papel do barramento: entrega ao S1, na ordem de chegada, as
mensagens de um arquivo (ods.session.started e ods.i2.tracks no cenário
padrão) e mostra o que o S1 publica — ods.s1.position, ods.s1.zone_transition
e ods.s1.zone_occupancy, no envelope comum da Especificação 1.0.

Cenário padrão (A3, modo pixel): um objeto oscilando na borda do Portão
Sul, que a histerese nunca confirma, e o "Objeto 99", que entra na
Recepção, fica ocluído e sai quando I2 encerra o rastro (LOST).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from s1.domain import S1Service
from s1.ingestion import load_json, load_messages, parse_zones
from s1.output import POSITION, ZONE_OCCUPANCY, ZONE_TRANSITION, S1Publisher
from s1.runtime import S1Runtime

DATA_DIR = Path(__file__).parent / "data"


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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--zonas", default=str(DATA_DIR / "zones_a3.json"), help="Conteúdo do artefato ZONES.")
    parser.add_argument("--versao-zonas", default="zones-a3-1", help="artifact_version do ZONES carregado.")
    parser.add_argument("--entrada", default=str(DATA_DIR / "entrada_demo.json"), help="Mensagens de entrada, na ordem de chegada.")
    parser.add_argument("--calibracao", help="Mensagens ods.i3.calibration a entregar antes da entrada (opcional).")
    parser.add_argument(
        "--saida",
        default=str(DATA_DIR / "output" / "eventos_s1.json"),
        help="Arquivo com as mensagens publicadas pelo S1 ('-' para não gravar).",
    )
    args = parser.parse_args()

    zones = parse_zones(load_json(args.zonas), args.versao_zonas)
    runtime = S1Runtime(S1Service(zones), S1Publisher(), zones_artifact_id=args.versao_zonas.rsplit("-", 1)[0])

    incoming = (load_messages(args.calibracao) if args.calibracao else []) + load_messages(args.entrada)
    print(
        f"ZONES {zones.artifact_version} em {zones.coordinates} para {list(zones.source_ids)}: "
        f"{len(zones.zones)} zona(s); {len(incoming)} mensagem(ns) de entrada.\n"
    )

    published = []
    for message in incoming:
        for output in runtime.handle(message):
            published.append(output)
            print(_describe(output))

    counts = {schema: sum(m["schema"] == schema for m in published) for schema in (POSITION, ZONE_TRANSITION, ZONE_OCCUPANCY)}
    print("\n" + ", ".join(f"{count} {schema}" for schema, count in counts.items()) + " publicada(s).")
    if args.saida != "-":
        output_path = Path(args.saida)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(published, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Gravado em: {output_path}")


if __name__ == "__main__":
    main()
