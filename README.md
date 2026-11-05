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
| `ods.i2.tracks` | I2 | rastros do quadro: `track_id`, `class`, `point_px`, `state` (`CONFIRMED`/`LOST`); o I2 publica a 30 quadros/s |
| `ods.i3.calibration` / CALIBRATION | I3 | `homography_px_to_m` e `reference_frame.space_id` de cada câmera |
| ZONES | A3, A4, A5 (editor de B6) | polígonos, histerese, `max_occupancy`, `classes` |
| `ods.session.started` | P4, P5, P6 | reiniciar o estado das câmeras da nova sessão |
| `ods.artifact.published` | dono do artefato | recarregar o ZONES da build quando sai uma versão nova |

Consumidores das saídas (lista fechada da 6c): `ods.s1.position` e
`ods.s1.zone_transition` → S2, S5, A3, A4, A5, A8; `ods.s1.zone_occupancy` →
S2, S5, A3, A8. O S2 e o S5 consomem as três mensagens. O A8 mudou de escopo
(controle de acesso por placa) e pediu uma revisão da 6c para deixar de
consumir o S1; a revisão ainda não foi respondida.

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
docker-compose.yml      # Kafka local + S1, para testar sem o broker da turma
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
elas, e a sessão delas não reinicia o estado daqui. Por isso o `source_ids`
tem de usar exatamente os nomes de câmera que o I2 publica.

**Entrada e saída (histerese).** Cada (câmera, rastro, zona) tem seu estado.
A entrada só é confirmada depois de `enter_hysteresis_ms` contínuos dentro;
a saída, depois de `exit_hysteresis_ms` contínuos fora. Um objeto oscilando
na borda nunca é confirmado. A transição é publicada com o **instante do
cruzamento** (`captured_at`), não o da confirmação, então `dwell_s` (inteiro,
em segundos) mede a permanência real. Uma zona com `classes` só conta essas
classes — os nomes são os do modelo da aplicação (artefato MODEL). Um rastro
pode estar em várias zonas ao mesmo tempo (ex.: "mesa 4" dentro do
"galpão").

**Rastro encerrado.** Oclusões curtas são seguradas por I2 (o rastro continua
`CONFIRMED`, com `predicted = true`). Quando I2 encerra o rastro, publica-o
uma vez com `state = LOST`: o S1 publica na hora a saída de toda zona em que
ele estava, no instante em que ele foi visto pela última vez (ou em que a
saída começou, se já estava saindo). **Sem o `LOST`, o S1 não tem como saber
que o objeto foi embora:** ele continua contado na zona e o `EXIT` não sai.

**Ocupação.** Por zona **e por câmera** — cada câmera conta o que ela vê;
juntar câmeras que veem a mesma pessoa é trabalho de S2 (por `position_m`,
só entre câmeras calibradas no mesmo `space_id`). Publicada a cada mudança,
com `count`, `count_by_class` (só as classes da zona, mesmo em zero; numa
zona sem `classes`, as classes presentes), `max_occupancy` (quando definido)
e `track_ids`.

**Sessão e replay.** `track_id` só vale dentro de (`source_id`,
`session_id`), e o `session_id` tem de ser um UUID. Em `ods.session.started`
— ou quando um quadro já chega com outra sessão — o S1 descarta o estado
daquelas câmeras **sem inventar saídas** (num SEEK ou LOOP o tempo de captura
volta) e publica a ocupação zerada das zonas que tinham alguém. Quadros de
sessões encerradas e quadros repetidos ou fora de ordem (`frame_index` não
crescente) são ignorados.

**Artefatos.** O ZONES é carregado na partida pela versão fixada na build,
por caminho (`S1_ZONES_FILE`), conferindo o hash se informado — como a MOD-1
sugeriu enquanto o B5 não define como resolver uma `uri registry://`.
Quando chega `ods.artifact.published` do mesmo `artifact_id`, o S1 carrega a
nova versão do registro (conferindo o `hash_sha256`) e a aplica na próxima
troca de sessão — ou na hora, se ainda não processou nenhum quadro. A
calibração chega com o conteúdo em `ods.i3.calibration`; para cada quadro
vale a de `valid_from` mais recente.

## Saída — envelope comum (perfil EV-SINAL)

Toda mensagem do S1 tem os campos do envelope da Especificação 1.0:
`message_type`, `message_id` (UUID v4), `schema`, `schema_version`,
`producer` (`S1`), `correlation_id` (o `message_id` da mensagem de entrada
que a causou — numa posição, a `ods.i2.tracks` daquele quadro),
`published_at`, `captured_at`, `source_id`, `session_id` (UUID), `sequence`
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

## Contratos (JSON Schema)

`contracts/` tem o JSON Schema (draft 2020-12) de cada mensagem que o S1
publica — `ods.s1.position`, `ods.s1.zone_transition` e
`ods.s1.zone_occupancy` —, com o envelope completo, para o B1 registrar e o
B4 gerar o produtor falso e o validador. Cada schema traz exemplos válidos
no campo `examples`; os exemplos inválidos (um defeito por caso, com nome que
diz o erro) ficam em `tests/test_output.py`. Os testes conferem os dois e
também tudo o que o S1 publica na demonstração.

**Estes schemas são um acordo:** o S2 (VER-6) e o S5 (MOD-2) consomem as
mensagens exatamente como estão aqui. Qualquer mudança é combinada antes com
os dois; uma mudança que só acrescenta um campo opcional sai como `1.1`, e
qualquer outra como `2.0`.

## Como rodar

```bash
python -m venv .venv
source .venv/bin/activate          # Windows (PowerShell): .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# Demonstração sem Kafka (A3, modo pixel): objeto oscilando na borda do Portão
# Sul (nunca confirmado) e o "Objeto 99" entrando na Recepção, ocluído e
# encerrado por I2 (LOST)
python simulate.py

# A mesma demonstração com a calibração de I3: as posições ganham position_m
python simulate.py --calibracao data/i3_calibration.json

# Gravar as mensagens publicadas num arquivo (ex.: para o A3 testar)
python simulate.py --saida eventos_s1.json

# Testes (nenhum usa broker de verdade)
python -m pytest
```

### Com um Kafka local (recomendado antes de usar o broker da turma)

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

⚠️ **Não use `--kafka` contra o broker da turma** sem combinar com o TRA-1: o
simulador publica mensagens falsas em `ods.i2.tracks`, que o S2 e os outros
consumidores leriam como se fossem reais.

### Em produção: `python -m s1`

Toda a configuração vem de variáveis de ambiente.

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
| `S1_TOPIC_<NOME>` | não | troca o nome de um tópico: `I2_TRACKS`, `I3_CALIBRATION`, `SESSION_STARTED`, `ARTIFACT_PUBLISHED`, `POSITION`, `ZONE_TRANSITION`, `ZONE_OCCUPANCY`. Padrão: o nome do schema, como na especificação — confirmado pelo TRA-1 |
| `S1_TIME_FIELD` | não | `captured_at` (padrão, confirmado pelo TRA-1) ou `occurred_at` |
| `S1_LOG_LEVEL` | não | `INFO` (padrão), `DEBUG`, `WARNING` |

Num S1 por aplicação (A3, A4, A5...), cada instância tem o seu ZONES e, por
isso, o seu grupo de consumo: duas instâncias no mesmo grupo dividiriam as
mensagens entre si. O ZONES de uma build real pode ser montado por volume
(`-v ./zones.json:/zones/zones.json:ro -e S1_ZONES_FILE=/zones/zones.json`).

### No broker da Jetson (rede do DCOMP)

O broker da turma está na Jetson, em **`172.18.4.56:9092`**, acessível só de
dentro da rede do DCOMP e sem autenticação — basta `S1_KAFKA_BOOTSTRAP_SERVERS`.

**1. Conferir se o broker responde:**

```bash
nc -vz 172.18.4.56 9092                       # Linux ou Mac: "succeeded" / "open"
Test-NetConnection 172.18.4.56 -Port 9092     # Windows (PowerShell): TcpTestSucceeded : True
```

**2. Rodar o S1** — de preferência em container, que já traz o Python e as
bibliotecas certas (a imagem pode ser construída na própria Jetson: a imagem base e as
bibliotecas têm versão para o processador ARM dela):

```bash
docker build -t ods-s1 .
docker run -d --name s1-a3 --restart unless-stopped \
  -e S1_KAFKA_BOOTSTRAP_SERVERS=172.18.4.56:9092 \
  -e S1_ZONES_FILE=/app/data/zones_a3.json -e S1_ZONES_VERSION=zones-a3-1 \
  ods-s1
docker logs -f s1-a3     # deve aparecer "S1 assinando ods.i2.tracks, ..."
```

**Cuidados:**

- **Um S1 por aplicação publicando de cada vez.** Duas instâncias do S1 do A3
  no ar (por exemplo, uma na Jetson e outra num notebook) publicam as mesmas
  saídas em dobro, com `message_id` diferentes — os consumidores não percebem
  a repetição. Para testar do próprio computador, combine com o grupo e use
  um `S1_KAFKA_GROUP_ID` próprio.
- **Nomes de câmera.** O `source_ids` do ZONES tem de usar os nomes que o I2
  publica na Jetson; senão o S1 recebe os quadros e descarta todos. O
  `data/zones_a3.json` usa `cam_frontal`, um nome provisório.
- **Se conectar e logo depois falhar** com `localhost:9092` ou outro nome no
  log, o broker está anunciando um endereço que só vale dentro da Jetson: o
  `advertised.listeners` dele precisa ser `172.18.4.56:9092` (é com o TRA-1).
- **Se os tópicos não existirem**, o S1 conecta e assina, mas não recebe nada
  (só registra aviso de tópico desconhecido).

## Operação

**O que o S1 lê e publica.** Assina `ods.i2.tracks`, `ods.i3.calibration`,
`ods.session.started` e `ods.artifact.published`; publica cada mensagem no
tópico com o nome do seu `schema`, com **`source_id` como chave**. Com a chave,
todas as mensagens de uma câmera caem na mesma partição e chegam em ordem.
Tópicos diferentes não têm ordem entre si: a calibração deve ser publicada
antes dos quadros que a usam (ou passada em `S1_CALIBRATION_FILE`).

**Volume.** Uma `ods.s1.position` por quadro e por câmera, na taxa de quadros
do I2: a 30 quadros/s, são 30 mensagens/s por câmera, cerca de 15 kB/s. Cada
mensagem tem cerca de 0,5 kB em JSON compacto, mais ~70 bytes por rastro a
mais no quadro. Transições e ocupação só saem quando algo muda. O broker é
compartilhado pela turma: testes longos ou de carga, só combinando com o TRA-1.

**Validação da entrada** (os três níveis do guia do Kafka): (1) o valor tem de
ser um objeto JSON; (2) o envelope tem de ter o `schema` esperado e
`schema_version` 1.x — versões menores novas são aceitas (só acrescentam
campos opcionais, que o S1 ignora), versão maior nova é descartada; (3) o
payload tem de seguir o contrato, e o `session_id` tem de ser um UUID.
Mensagem reprovada é registrada no log (`WARNING`) e descartada; o serviço
continua. Mensagens de outros schemas (ex.: o formato antigo
`ods.visao.*`) são ignoradas.

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

- **`LOST` do I2 (INV-1) — o mais importante:** hoje o I2 não publica o
  rastro uma última vez com `state = LOST` (ofereceu publicar). Sem isso, um
  objeto que sai de cena fica contado para sempre na zona e o `EXIT` nunca
  sai; o S1 não consegue resolver isso sozinho.
- **Formato real do I2 (INV-1):** no relatório de integração do TRA-2, a saída
  do I2 ainda usa `ods.visao.identidade_atribuida`, e não `ods.i2.tracks`.
  Até isso mudar, o S1 conectado ao broker não recebe nenhum rastro.
- **Quadros vazios (INV-1):** o I2 só publica quando há rastro; a
  especificação pede uma mensagem por quadro, mesmo com `tracks` vazio. Sem
  isso, o S1 não publica a posição dos quadros vazios.
- **Nomes de câmera e calibração (INV-1 e aplicações):** dependem das
  câmeras na Jetson. O `zones_a3.json` usa `cam_frontal`, provisório; sem
  calibração do I3, um ZONES em `METERS` (como o do A4) não encontra zonas.
- **Tópicos no broker (TRA-1):** quem cria os tópicos na Jetson, e com
  quantas partições.
- **Áreas do A5 (VER-3):** declarar `classes: ["pessoa"]` em cada área (o
  modelo também detecta capacete e colete) e, em `PIXELS`, um ZONES e uma
  instância do S1 por câmera.
- **Registro do B5 (MOD-1):** como uma `uri registry://<dono>/<tipo>/<versão>`
  é resolvida. Até lá, o ZONES vem por caminho na build, com hash.
- **A8 (VER-5 e Álvaro):** revisão da 6c para o A8 deixar de consumir o S1 e
  de publicar ZONES. O código do S1 não muda nos dois casos.
