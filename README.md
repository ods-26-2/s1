# Trabalho ODS — Equipe VER-6 — Componente S1

Implementação do componente **S1 — Zonas, ocupação e eventos**

## Escopo implementado (Sprint 2)

- **Entrada real de I2 + I3**: consome os eventos de rastreio de I2
  (`ods.inferencia.rastreio`, coordenadas em pixel) e projeta cada track
  para coordenadas de mundo usando a homografia de calibração publicada
  por I3 (`ods.inferencia.calibracao`), vigente para a câmera/instante da
  captura (`s1/ingestion.py`).
- **ROI e zonas nomeadas, com sistema de coordenadas explícito**: cada zona
  declara se está em `"pixel"` (instalação simples, presa a uma câmera,
  sem precisar de I3) ou `"world"` (coordenadas de mundo, quando há métrica
  ou mais de uma câmera) — o contrato aceita os dois modos, não só mundo
  (`Zone` em `s1/domain.py`, artefato em `data/definicao_zonas.json`).
- **Associação (carimbo) entre evento e região**: motor de geometria que
  resolve em qual polígono um ponto caiu — testando cada zona no seu
  próprio sistema de coordenadas — desempatando sobreposições pela zona de
  centróide mais próximo (`ZoneResolver` em `s1/domain.py`).
- **Entrada, saída e permanência com histerese assimétrica**: controlador
  de estado que só confirma uma transição depois de N leituras
  consecutivas no novo estado — com N **diferente para entrar e para
  sair** (padrão: 2 para entrada, 3 para saída), evitando tanto falsos
  positivos de entrada quanto perdas por oclusão breve na saída
  (`HysteresisController` em `s1/domain.py`).
- **Ocupação**: contagem instantânea de entidades confirmadas por zona,
  exposta em cada evento e via `S1Service.occupancy_snapshot()`.
- **Saída publicada**: além do `ZoneEvent` interno, o
  simulador grava um JSON com as mensagens `ods.visao.evento_espacial`
  publicadas pelo S1 — uma por leitura processada (`s1/output.py`).

Fora do escopo desta entrega (Sprint 3): contagem/ocupação resiliente a
oclusão com TTL, persistência mínima e limiar de confiança (S2), e a matriz
de decisão de A3. A entrada de I1 ainda não tem formato definido pela
equipe responsável — quando existir, deve entrar na mesma etapa de
ingestão (`s1/ingestion.py`), ao lado de I2/I3.

## Pipeline de entrada (I2 + I3 → S1)

```
I2 (pixel, por câmera/frame)  ─┐
                                 ├─► s1/ingestion.py ─► TrajectoryPoint (mundo) ─► S1Service (s1/domain.py)
I3 (homografia, por câmera)   ─┘                                                       │
                                                                                         ▼
                                                                 ZoneEvent (p/ A3) ──► s1/output.py ──► ods.visao.evento_espacial
```

1. **I3 (`ods.inferencia.calibracao`)** é carregado primeiro e registrado em
   um `CalibrationRegistry` (`s1/ingestion.py`), que guarda todas as
   versões de calibração publicadas por câmera.
2. **I2 (`ods.inferencia.rastreio`)** chega por frame, com uma lista de
   `tracks` em coordenadas de pixel (`u_px`, `v_px`) por `camera_id`.
3. Para cada track, `to_trajectory_points()` resolve a calibração vigente
   daquela câmera no instante `captured_at` (a de `valid_from` mais recente
   que ainda seja `<= captured_at`) e projeta o pixel para coordenadas de
   mundo via homografia: `[x, y, w]ᵀ = H · [u, v, 1]ᵀ`, ponto final em
   `(x/w, y/w)`.
4. O `track_id` de I2 é **local à câmera**. Por isso o `entity_id` usado
   pelo S1 é `"{camera_id}:{track_id}"` — duas câmeras observando a mesma
   entidade física gerariam dois `entity_id` diferentes.
5. O `TrajectoryPoint` resultante segue para o `S1Service` carregando as
   duas representações possíveis: `pixel_position` (sempre) e
   `world_position` (só quando havia calibração vigente para projetar).

Se uma câmera não tiver calibração vigente para o instante da leitura,
`to_trajectory_points()` **não falha** — `world_position` fica `None` e a
leitura segue adiante só com `pixel_position`. Isso é o que torna possível
uma instalação simples (zonas em pixel, sem I3 nenhum): antes desta
correção, a ingestão exigia calibração para toda leitura e travava sem
I3, mesmo quando nenhuma zona em modo mundo estava configurada.

## Zonas em pixel vs. zonas em mundo

Cada zona (`data/definicao_zonas.json`) declara `sistema_coordenadas`
(`"mundo"` ou `"pixel"`). Zonas em `"pixel"` também exigem `camera_id` (a
zona só é válida para aquela câmera) e podem referenciar
`versao_transformacao` (a versão de calibração de I3 vigente quando a
zona foi desenhada, para rastreabilidade). O `ZoneResolver` testa cada
zona no seu próprio sistema: zonas `"world"` usam `world_position` da
leitura (e são ignoradas se não houver); zonas `"pixel"` usam
`pixel_position` e só combinam com leituras da mesma `camera_id`.

## Estrutura

Três módulos em `s1/`, um por etapa do pipeline (entrada → núcleo → saída):

```
s1/
  domain.py       # Núcleo do S1: modelos (Point, Zone, TrajectoryPoint,
                  # ZoneEvent), ZoneResolver (geometria), HysteresisController
                  # (entrada/saída/permanência) e S1Service (orquestração)
  ingestion.py     # Entrada: parsing dos envelopes de I2/I3, homografia
                   # (pixel -> mundo), CalibrationRegistry, to_trajectory_points
                   # e o carregamento dos artefatos JSON
  output.py         # Saída: converte (TrajectoryPoint, ZoneEvent) na
                     # mensagem publicada ods.visao.evento_espacial
data/
  definicao_zonas.json    # artefato estático de zonas (sistema_coordenadas mundo/pixel)
  i3_calibracoes.json      # calibração de exemplo (cam_frontal)
  i2_eventos.json           # cenário padrão: objeto oscilando na borda + "Objeto 99" entrando na Zona Segura
  output/                     # JSON gerado pelo simulate.py (mensagens publicadas pelo S1)
simulate.py              # script simulador (CLI) para a demonstração
tests/
  test_domain.py       # geometria + histerese + S1Service
  test_ingestion.py     # parsing de I2/I3 + homografia + calibração + ingestão
  test_output.py         # mensagem ods.visao.evento_espacial
  test_integration.py     # ponta a ponta, usando os arquivos reais em data/
```

## Como rodar

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Demonstração: eventos de I2 (data/i2_eventos.json) + calibração de
# I3 (data/i3_calibracoes.json) — combina os dois casos da Sprint 2: um
# objeto oscilando na borda (histerese suprime) e o "Objeto 99" entrando
# de fato na Zona Segura
python simulate.py

# Testes
pytest
```

## Exemplo de saída do simulador

O cenário padrão (`data/i2_eventos.json`) tem dois objetos, provando os dois
casos pedidos na demonstração da Sprint 2:

- `cam_frontal:1` oscila entre pixel u=508 e u=511 — bem na fronteira da
  "Zona Proibida" (Portão Sul) depois de projetado para o mundo (x≈50.8 a
  51.1). Como cada leitura fica isolada (nunca duas leituras consecutivas na
  mesma zona), a histerese (janela padrão de 2 leituras) impede que isso
  seja confirmado como uma entrada na zona — nas mensagens desse objeto,
  `zone_id`/`transition` **não aparecem**, e isso é o comportamento
  correto (não um bug): a entidade nunca chega a entrar em zona nenhuma.
- `cam_frontal:99` ("Objeto 99", o mesmo exemplo do enunciado: "Objeto 99
  detectado na Zona Segura") entra de fato na Zona Segura depois de 2
  leituras consecutivas dentro do polígono — a mensagem dessa segunda
  leitura **tem** `zone_id: "zona_segura_01"` e
  `transition: {"type": "entrada"}`.

Ou seja: `zone_id`/`transition` só aparecem quando uma zona é realmente
confirmada — se você rodar com dados em que ninguém entra em zona nenhuma
(como só o `cam_frontal:1`), é esperado que nenhuma mensagem os tenha.

## Saída publicada pelo S1

A cada leitura processada, `simulate.py` publica uma mensagem
`ods.visao.evento_espacial` (gravadas, ao final, em
`data/output/eventos_s1.json` por padrão, ajustável com `--saida`):

```json
{
  "message_type": "event",
  "schema": "ods.visao.evento_espacial",
  "schema_version": "1.0",
  "producer": "S1",
  "published_at": "2026-09-16T10:47:28.500Z",
  "payload": {
    "entity_id": "cam_frontal:99",
    "source_id": "cam_frontal",
    "timestamp": "2026-09-16T10:47:28.500Z",
    "world_coordinates": { "x": 15.0, "y": 10.0 },
    "zone_id": "zona_segura_01",
    "transition": { "type": "entrada" }
  }
}
```

`entity_id`, `source_id`, `timestamp` e `world_coordinates` são sempre
preenchidos — numa instalação em pixel sem calibração de I3,
`world_coordinates` recebe a posição em pixel (é a melhor localização
disponível, e o contrato exige o campo sempre presente). `zone_id` só
aparece quando a entidade está, naquele instante, confirmada dentro de
alguma zona; `transition` só aparece quando a leitura corresponde a uma
transição confirmada pela histerese:

| `event_type` interno | `transition.type` | `previous_zone_id` |
|---|---|---|
| `enter` | `entrada` | — |
| `exit` | `saida` | zona que foi deixada |
| `transfer` (troca direta entre zonas) | `transferencia` | zona que foi deixada |
| `update` / `none` | (chave `transition` ausente) | — |

Essa conversão é feita por `to_spatial_event()` em `s1/output.py`. O formato
de `transition` não veio detalhado no contrato (campo `object` genérico) —
a tabela acima é a convenção adotada.

## Formato interno (`ZoneEvent`)

Estrutura usada internamente pelo `S1Service`. Junto
com o `TrajectoryPoint` de origem, é o que `to_spatial_event()` consome
para montar a mensagem `ods.visao.evento_espacial`.

| Campo | Descrição |
|---|---|
| `entity_id` | `"{camera_id}:{track_id}"` — identificador da entidade, por fonte (pré-fusão) |
| `zone_id` / `zone_name` | zona confirmada (ou `None` se fora de qualquer zona) |
| `event_type` | `enter`, `exit`, `transfer`, `update` ou `none` |
| `entered_at` / `dwell_seconds` | timestamp de entrada e tempo de permanência |
| `occupancy` | contagem atual de entidades na zona |
| `risco` / `cor_dashboard` | metadados estáticos da zona (para A3) |
| `previous_zone_id` | zona confirmada anterior, em caso de transição |
