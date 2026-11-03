# Trabalho ODS — Equipe VER-6 · Componente S1

Implementação do **S1 — Zonas, ocupação e eventos** (Camada 3), conforme a
**Arquitetura 6c** e a **Especificação de interfaces 1.0** da ODS 2026/2: dar
sentido espacial ao que a camada 2 detecta — onde cada rastro está e há
quanto tempo — em três mensagens (posição, transição e ocupação), com modo
pixel quando não há calibração.

## O que entra e o que sai

```
            Kafka (B3)                                                          Kafka (B3)
ods.i2.tracks ─────────┐                                                ┌─► ods.s1.position        (por quadro e câmera)
ods.i3.calibration ────┤          s1/runtime.py                      ├─► ods.s1.zone_transition (ENTER / EXIT)
ods.session.started ───┼─► KafkaBus ─► S1Runtime ──────► S1Service ─┴─► ods.s1.zone_occupancy  (por zona e câmera)
ods.artifact.published ┘   (JSON,      (versão, deduplica,   (domain.py)
                            tópicos)    roteia)
                                                                        ▲
                                  artefatos ZONES e CALIBRATION (fixados na build / registro de B5)
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
  runtime.py    # S1Runtime (deduplica e roteia cada mensagem) e o adaptador Kafka
  __main__.py   # `python -m s1`: configuração, partida e o serviço em produção
contracts/      # JSON Schemas das três mensagens ods.s1.* (com exemplos válidos)
data/
  zones_a3.json         # ZONES da demonstração (A3, modo PIXELS)
  zones_exemplo_a8.json # ZONES de exemplo da especificação (A8, METERS)
  entrada_demo.json     # mensagens de entrada da demonstração, na ordem de chegada
  i3_calibration.json   # ods.i3.calibration de exemplo (opcional na demonstração)
simulate.py             # faz o papel das outras equipes, sem Kafka ou num Kafka local
Dockerfile              # container do S1 (P3)
docker-compose.yml      # Kafka local + S1, para testar sem o broker da nuvem
tests/                  # um arquivo de teste por módulo de s1/ (+ factories.py)
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
artefato cobre exatamente uma câmera. **Só as câmeras em `source_ids` são
deste S1:** no barramento compartilhado chegam quadros e inícios de sessão de
câmeras de outras aplicações, e o S1 os ignora — não publica posição sobre
elas, e a sessão delas não reinicia o estado daqui.

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

## Saída — envelope comum (perfil EV-SINAL)

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

## Contratos (JSON Schema)

`contracts/` tem o JSON Schema (draft 2020-12) de cada mensagem que o S1
publica — `ods.s1.position`, `ods.s1.zone_transition` e
`ods.s1.zone_occupancy` —, com o envelope completo, para o B1 registrar e o
B4 gerar o produtor falso e o validador. Cada schema traz exemplos válidos
no campo `examples`; os exemplos inválidos (um defeito por caso, com nome que
diz o erro) ficam em `tests/test_output.py`. Os testes conferem os dois e
também tudo o que o S1 publica na demonstração.

## Como rodar

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt

# Demonstração sem Kafka (A3, modo pixel): objeto oscilando na borda do Portão
# Sul (nunca confirmado) e o "Objeto 99" entrando na Recepção, ocluído e
# encerrado por I2 (LOST)
python simulate.py

# A mesma demonstração com a calibração de I3: as posições ganham position_m
python simulate.py --calibracao data/i3_calibration.json

# Gravar as mensagens publicadas num arquivo
python simulate.py --saida eventos_s1.json

# Testes (nenhum usa broker de verdade)
python -m pytest
```

### Com um Kafka local

```bash
docker compose up -d --build    # Kafka local, tópicos e o S1 em container
python simulate.py --kafka localhost:9092   # publica a demonstração e confere as saídas
docker compose logs s1
docker compose down
```

Com `--kafka`, o simulador faz o papel de I3, P6 e I2 (publica a calibração, o início de sessão
e os quadros, além de um JSON quebrado, uma mensagem versão 2.0, uma câmera de
outra aplicação e uma entrega repetida) e confere que as saídas seguem os
schemas, saem no tópico certo com a chave certa e são iguais às do modo sem Kafka.

### Em produção: `python -m s1`

Toda a configuração vem de variáveis de ambiente — nenhum endereço fica no
código, porque o broker da nuvem é temporário.

| Variável | Obrigatória | Para quê |
|---|---|---|
| `S1_KAFKA_BOOTSTRAP_SERVERS` | sim | `host:porta` do broker |
| `S1_ZONES_FILE` | sim | conteúdo do ZONES fixado na build |
| `S1_ZONES_VERSION` | sim | `artifact_version` desse ZONES (ex.: `zones-a3-1`) |
| `S1_ZONES_ARTIFACT_ID` | não | `artifact_id` acompanhado em `ods.artifact.published` (padrão: a versão sem o `-N`) |
| `S1_ZONES_SHA256` | não | confere o arquivo do ZONES na partida |
| `S1_REGISTRY_DIR` | não | pasta do registro de artefatos, para carregar versões anunciadas |
| `S1_CALIBRATION_FILE` | não | mensagens `ods.i3.calibration` aplicadas na partida |
| `S1_KAFKA_GROUP_ID` | não | grupo de consumo (padrão: `S1-zonas-<artifact_id>`, um por aplicação) |
| `S1_KAFKA_<CHAVE>` | não | qualquer chave do librdkafka: `_` vira `.`, `__` vira `_` (`S1_KAFKA_SECURITY_PROTOCOL`, `S1_KAFKA_SASL_USERNAME`...) |
| `S1_TOPIC_<NOME>` | não | troca o nome de um tópico: `I2_TRACKS`, `I3_CALIBRATION`, `SESSION_STARTED`, `ARTIFACT_PUBLISHED`, `POSITION`, `ZONE_TRANSITION`, `ZONE_OCCUPANCY` (padrão: o nome do schema) |
| `S1_TIME_FIELD` | não | `captured_at` (especificação, padrão) ou `occurred_at` (guia do Kafka) |
| `S1_LOG_LEVEL` | não | `INFO` (padrão), `DEBUG`, `WARNING` |

```bash
docker build -t ods-s1 .
docker run --rm \
  -e S1_KAFKA_BOOTSTRAP_SERVERS=<host:porta do broker> \
  -e S1_ZONES_FILE=/app/data/zones_a3.json -e S1_ZONES_VERSION=zones-a3-1 \
  ods-s1
```

Num S1 por aplicação (A3, A5, A8...), cada instância tem o seu ZONES e, por
isso, o seu grupo de consumo: duas instâncias no mesmo grupo dividiriam as
mensagens entre si. O ZONES de uma build real pode ser montado por volume
(`-v ./zones.json:/zones/zones.json:ro -e S1_ZONES_FILE=/zones/zones.json`).

## Operação

**O que o S1 lê e publica.** Assina `ods.i2.tracks`, `ods.i3.calibration`,
`ods.session.started` e `ods.artifact.published`; publica cada mensagem no
tópico com o nome do seu `schema`, com **`source_id` como chave**. Com a chave,
todas as mensagens de uma câmera caem na mesma partição e chegam em ordem.
Tópicos diferentes não têm ordem entre si: a calibração deve ser publicada
antes dos quadros que a usam (ou passada em `S1_CALIBRATION_FILE`).

**Volume.** Uma `ods.s1.position` por quadro e por câmera, na taxa de quadros
de I2 (com I2 a 10 quadros/s, por exemplo, são 10 mensagens/s por câmera).
Cada mensagem tem cerca de 0,5 kB em JSON compacto, mais ~70 bytes por rastro
a mais no quadro. Transições e ocupação só saem quando
algo muda. Não faça testes longos ou de carga no broker da nuvem sem combinar
com o TRA-1: ele consome os créditos da turma.

**Validação da entrada** (os três níveis do guia do Kafka): (1) o valor tem de
ser um objeto JSON; (2) o envelope tem de ter o `schema` esperado e
`schema_version` 1.x — versões menores novas são aceitas (só acrescentam
campos opcionais, que o S1 ignora), versão maior nova é descartada; (3) o
payload tem de seguir o contrato. Mensagem reprovada é registrada no log
(`WARNING`) e descartada; o serviço continua.

**Idempotência.** O S1 descarta mensagens com `message_id` já visto (guarda os
últimos 10 000), então uma entrega repetida pelo broker não publica nada de
novo. Quadros repetidos ou fora de ordem (`frame_index` não crescente) também
são ignorados. As mensagens que o S1 publica têm `message_id` novo (UUID v4),
para os consumidores deduplicarem do lado deles.

**Falhas.**
- *Mensagem com problema*: descartada com log; um erro inesperado ao processar
  uma mensagem também só é registrado (`ERROR`, com o traceback).
- *Broker fora do ar*: o cliente do Kafka reconecta sozinho; o S1 fica
  esperando. Ao voltar, o grupo continua de onde parou (offsets confirmados
  automaticamente).
- *Falha de entrega*: registrada no log (`falha ao publicar`); o produtor já
  tenta de novo sozinho antes de desistir.
- *Configuração inválida* (variável faltando, chave do Kafka inexistente,
  ZONES com hash errado): o S1 nem começa — sai com código 2 e diz o motivo.
- *Parada* (`docker stop`, Ctrl+C): termina o laço, entrega o que falta
  publicar e fecha a conexão; sai com código 0.
- *Reinício*: o estado (quem está em cada zona) fica só em memória. Ao
  voltar, o S1 continua lendo de onde o grupo parou, mas começa sem ninguém
  nas zonas: quem estava dentro ganha uma nova entrada (depois da histerese),
  e o `dwell_s` da saída passa a contar a partir dela.

## Em aberto (para confirmar com as outras equipes)

- **A4 sem ZONES:** A4 consome o S1, mas não é dono de ZONES. Se o S1 do A4
  for só para obter `position_m`, ele precisará rodar sem artefato de zonas
  (`zone_ids` sempre vazio); hoje o S1 exige um ZONES.
- **`count_by_class`:** o exemplo da especificação mostra `"person": 0` numa
  vaga que só conta `vehicle`. O S1 publica as classes declaradas na zona
  (mais as presentes); se a intenção for listar todo o vocabulário do modelo,
  ele precisa vir de algum artefato.
- **Registro de B5:** o `ArtifactRegistry` lê o conteúdo de uma pasta local
  pela URI `registry://…`; o acesso ao registro real depende do que B5
  definir.
- **Nomes de tópico e do campo de horário (TRA-1):** a especificação usa
  `ods.s1.*`/`ods.i2.tracks` e `captured_at`; o guia do Kafka sugere
  `ods.<domínio>.<evento>` e `occurred_at`. O S1 segue a especificação por
  padrão, aceita os dois campos na entrada e troca tópicos e campo por
  configuração (`S1_TOPIC_*`, `S1_TIME_FIELD`), sem mudar o código.
