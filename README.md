# Trabalho ODS — Equipe VER-6 · Componente S1

Implementação do **S1 — Zonas, ocupação e eventos** (Camada 3), conforme a
**Arquitetura 6c** e a **Especificação de interfaces 1.0** da ODS 2026/2: dar
sentido espacial ao que a camada 2 detecta — onde cada rastro está e há
quanto tempo — em três mensagens (posição, transição e ocupação), com modo
pixel quando não há calibração.

## O que entra e o que sai

```
ods.i2.tracks ─────────┐
ods.i3.calibration ────┤                               ┌─► ods.s1.position        (por quadro e câmera)
ods.session.started ───┼─► s1/runtime.py ─► S1Service ─┼─► ods.s1.zone_transition (ENTER / EXIT)
ods.artifact.published ┘      (deduplica,  (domain.py)  └─► ods.s1.zone_occupancy  (por zona e câmera)
                               roteia)          ▲
                       artefatos ZONES e CALIBRATION (registro de B5)
```

| Entrada | Dono | O S1 usa para |
|---|---|---|
| `ods.i2.tracks` | I2 | rastros do quadro: `track_id`, `class`, `point_px`, `state` (`CONFIRMED`/`LOST`) |
| `ods.i3.calibration` / CALIBRATION | I3 | `homography_px_to_m` e `reference_frame.space_id` de cada câmera |
| ZONES | A3, A5, A8 (editor de B6) | polígonos, histerese, `max_occupancy`, `classes` |
| `ods.session.started` | P4, P5, P6 | reiniciar o estado das câmeras da nova sessão |
| `ods.artifact.published` | dono do artefato | recarregar o ZONES da build quando sai uma versão nova |

A arquitetura também permite ao S1 assinar `ods.i1.detections` e
`ods.p6.replay.state`, mas nenhuma saída do S1 depende deles; o runtime os
ignora.

Consumidores das saídas (lista fechada da 6c): `ods.s1.position` e
`ods.s1.zone_transition` → S2, S5, A3, A4, A5, A8; `ods.s1.zone_occupancy` →
S2, S5, A3, A8.

## Estrutura

```
s1/
  domain.py     # ZONES, geometria, homografia/calibração, histerese e S1Service
  ingestion.py  # leitura dos contratos de entrada e dos artefatos; registro de artefatos
  output.py     # S1Publisher: envelope comum + as três mensagens ods.s1.*
  runtime.py    # S1Runtime: recebe cada mensagem, deduplica pelo message_id e roteia
data/
  zones_a3.json         # ZONES da demonstração (A3, modo PIXELS)
  zones_exemplo_a8.json # ZONES de exemplo da especificação (A8, METERS)
  entrada_demo.json     # mensagens de entrada da demonstração, na ordem de chegada
  i3_calibration.json   # ods.i3.calibration de exemplo (opcional na demonstração)
  output/eventos_s1.json
simulate.py             # faz o papel do barramento e mostra o que o S1 publica
tests/                  # domain, ingestion, output, runtime (+ factories.py)
```

## Como o S1 funciona

**Quadro a quadro, por câmera.** Cada `ods.i2.tracks` é um quadro de uma
câmera. O S1 publica **uma** `ods.s1.position` por quadro, com um item por
rastro `CONFIRMED` — inclusive com a lista vazia.

**Posição.** `point_px` vem de I2 (centro da base da caixa, onde o objeto
toca o chão). Quando a câmera tem calibração, o S1 aplica a homografia de I3
— que já parte do quadro bruto, sem correção de distorção — e publica
`position_m` e `space_id`. `zone_ids` traz **todas** as zonas que contêm o
ponto, sem desempate (o desempate é de S2).

**Zonas (artefato ZONES).** `coordinates` é `METERS` ou `PIXELS`. Em METERS,
o ponto testado é `position_m`, e só vale se a calibração for do mesmo
`space_id` do artefato. Em PIXELS, o ponto testado é `point_px`, e o
artefato cobre exatamente uma câmera. Só leituras das câmeras em `source_ids`
são consideradas.

**Entrada e saída (histerese).** Cada (câmera, rastro, zona) tem seu estado.
A entrada só é confirmada depois de `enter_hysteresis_ms` contínuos dentro;
a saída, depois de `exit_hysteresis_ms` contínuos fora. Um objeto oscilando
na borda nunca é confirmado. A transição é publicada com o **instante do
cruzamento** (`captured_at`), não o da confirmação, então `dwell_s` (inteiro,
em segundos) mede a permanência real. Uma zona com `classes` só conta essas
classes. Um rastro pode estar em várias zonas ao mesmo tempo (ex.: "mesa 4"
dentro do "galpão").

**Rastro encerrado.** Oclusões curtas são seguradas por I2 (o rastro continua
`CONFIRMED`, com `predicted = true`). Quando I2 encerra o rastro, publica-o
uma vez com `state = LOST`: o S1 publica na hora a saída de toda zona em que
ele estava, no instante em que ele foi visto pela última vez (ou em que a
saída começou, se já estava saindo).

**Ocupação.** Por zona **e por câmera** — cada câmera conta o que ela vê;
juntar câmeras que veem a mesma pessoa é trabalho de S2. Publicada a cada
mudança, com `count`, `count_by_class` (com as classes da zona, mesmo em
zero), `max_occupancy` (quando definido) e `track_ids`.

**Sessão e replay.** `track_id` só vale dentro de (`source_id`,
`session_id`). Em `ods.session.started` — ou quando um quadro já chega com
outra sessão — o S1 descarta o estado daquelas câmeras **sem inventar
saídas** (num SEEK ou LOOP o tempo de captura volta) e publica a ocupação
zerada das zonas que tinham alguém. Quadros de sessões encerradas e quadros
repetidos ou fora de ordem (`frame_index` não crescente) são ignorados.

**Artefatos.** O ZONES é carregado na partida pela versão fixada na build.
Quando chega `ods.artifact.published` do mesmo `artifact_id`, o S1 carrega a
nova versão do registro (conferindo o `hash_sha256`) e a aplica na próxima
troca de sessão — ou na hora, se ainda não processou nenhum quadro. A
calibração chega com o conteúdo em `ods.i3.calibration`; para cada quadro
vale a de `valid_from` mais recente.

## Saída — envelope comum

Toda mensagem do S1 tem os campos do envelope da Especificação 1.0:
`message_type`, `message_id` (UUID v4), `schema`, `schema_version`,
`producer` (`S1`), `correlation_id` (a mensagem de entrada que a causou),
`published_at`, `captured_at`, `source_id`, `session_id`, `sequence`
(contador por sessão, a partir de 0), `artifact_version` (versão do ZONES que
produziu a mensagem) e `payload`. O payload nunca repete um campo do
envelope.

```json
{ "schema": "ods.s1.position", "producer": "S1", "source_id": "cam0", "artifact_version": "zones-a8-2",
  "payload": { "frame_index": 48213, "positions": [
    { "track_id": "t-0391", "class": "person", "point_px": [505, 702], "position_m": [3.42, 7.10],
      "space_id": "patio-1", "zone_ids": ["vaga-12"] } ] } }

{ "schema": "ods.s1.zone_transition", "producer": "S1", "source_id": "cam0", "artifact_version": "zones-a8-2",
  "payload": { "track_id": "t-0391", "class": "vehicle", "zone_id": "vaga-12", "transition": "EXIT",
               "entered_at": "2026-09-23T12:10:04.200Z", "dwell_s": 8395 } }

{ "schema": "ods.s1.zone_occupancy", "producer": "S1", "source_id": "cam0", "artifact_version": "zones-a8-2",
  "payload": { "zone_id": "vaga-12", "count": 0, "count_by_class": { "vehicle": 0 }, "max_occupancy": 1,
               "track_ids": [] } }
```

(Exemplos com o envelope abreviado, como na especificação.)

## Como rodar

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt

# Demonstração com A3 de exemplo
python simulate.py

# A mesma demonstração com a calibração de I3: as posições ganham position_m
python simulate.py --calibracao data/i3_calibration.json

# Testes
python -m pytest
```