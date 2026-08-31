# SDCC Progetto B5 — Key-Value Store Distribuito e Replicato basato su Raft

Key-value store distribuito, replicato e fortemente consistente, sviluppato
in Go per il progetto B5 del corso di Sistemi Distribuiti e Cloud Computing.

Nessun database centrale: la consistenza dello stato tra le
repliche è garantita da un'implementazione da zero dell'algoritmo di
consenso **Raft**.

Per la descrizione completa di architettura, scelte di design, ed
esperimenti di valutazione, si rimanda al report (scritto in LaTeX, stile
IEEEtran) in [`report/report.tex`](report/report.tex)


## Requisiti

- **Docker** - unico requisito per avviare il
  sistema.
- **Go 1.26+** — necessario solo per compilare i client/strumenti usati per
  interagire col sistema o rieseguire gli esperimenti (non per avviare il
  sistema stesso, che gira dentro i container).

## Clonare il repository

```bash
git clone https://github.com/Cellu83/sdcc-Progetto-b5-kvstor.git
cd sdcc-Progetto-b5-kvstor
```

Tutti i comandi delle sezioni seguenti assumono di essere lanciati dalla
radice di questa cartella.

## Avvio rapido (Docker Compose)

Dalla radice del repository:

```bash
docker compose up -d --build
docker compose ps
```
Questo avvia **7 container**:
5 consensus node, 1 client proxy, 1 servizio di snapshot & backup. In pochi
secondi il cluster elegge un leader — verificabile con:

```bash
docker compose logs | grep "eletto Leader"
```

Per fermare tutto (rimuovendo anche i volumi persistenti):

```bash
docker compose down -v
```

## Interagire col sistema

Serve compilare il client a riga di comando (richiede Go in locale):

```bash
go build -o bin/raft-client ./cmd/raft-client

./bin/raft-client -addr localhost:50060 -rpc put -key ciao -value mondo
./bin/raft-client -addr localhost:50060 -rpc get -key ciao
```

Le richieste vanno sempre al **proxy** (porta 50060), che scopre da solo il
Leader corrente e instrada la chiamata — non serve sapere quale dei 5 nodi
sia leader in un dato momento.

### Dimostrare la fault-tolerance

Uccidi il container del leader corrente e osserva la rielezione in tempo
reale:

```bash
docker kill sdcc-b5-kvstore-<nodo-leader>-1
docker compose logs --tail 30 | grep -E "avvio elezione|eletto Leader|riconosciuto leader"
```

Il nome esatto del container (`sdcc-b5-kvstore-nodeX-1`) è visibile con
`docker compose ps`. Una nuova elezione avviene tipicamente entro 150-300ms
(vedere Sezione Results del report per le misure precise).









## Esecuzione senza Docker (nativa)

```bash
go build -o bin/consensus-node ./cmd/consensus-node
go build -o bin/proxy ./cmd/proxy
go build -o bin/snapshot ./cmd/snapshot

./bin/consensus-node -config configs/node1.yaml &
./bin/consensus-node -config configs/node2.yaml &
./bin/consensus-node -config configs/node3.yaml &
./bin/proxy -config configs/proxy.yaml &
```

Ogni binario è generico e parametrizzato interamente da file YAML esterni
(nessun valore hard-coded nel codice, per scelta di design fin dalla prima
fase del progetto) — per aggiungere o modificare nodi basta scrivere nuove
config.

### Modificare la configurazione

Per cambiare i dettagli di un nodo locale, si modifica direttamente il suo
file in `configs/` (es. `configs/node1.yaml`):

```yaml
node:
  id: "node1"                # identificativo del nodo
  bind_address: "0.0.0.0"    # indirizzo su cui il nodo resta in ascolto
  raft_port: 50051           # porta gRPC del nodo
  data_dir: "./data/node1"   # cartella dove persiste log e stato Raft

cluster:
  peers:                     # vista del cluster nota a bootstrap: deve
    - id: "node1"             # essere identica su tutti i nodi e sul proxy
      address: "localhost:50051"
    - id: "node2"
      address: "localhost:50052"
    - id: "node3"
      address: "localhost:50053"

raft:
  election_timeout_min_ms: 150   # timeout di elezione, randomizzato in [min, max]
  election_timeout_max_ms: 300
  heartbeat_interval_ms: 50      # intervallo di heartbeat del Leader
  rpc_timeout_ms: 50             # timeout delle singole RPC RequestVote/AppendEntries

log_level: "info"
```

Se si cambia l'indirizzo/porta di un nodo, la sezione `cluster.peers` va
aggiornata **in tutti i file** (ogni nodo e il proxy), perché ognuno deve
avere la stessa vista del cluster fin dall'avvio. Il file del proxy
(`configs/proxy.yaml`) ha una struttura analoga, con in più i parametri di
retry e Circuit Breaker.

## Esperimenti di valutazione

I due esperimenti richiesti dalla traccia (scalabilità e fault-tolerance)
sono strumenti Go standalone in `experiments/`, con i risultati già
raccolti in `experiments/results/*.csv` e riassunti nel report (Sezione
Results). Per rieseguirli:

```bash
go build -o bin/gen-cluster-config ./experiments/gen-cluster-config
go build -o bin/bench-client ./experiments/bench-client
go build -o bin/fault-tolerance ./experiments/fault-tolerance

# Scalabilità: genera un cluster di N nodi e misura la latenza Put/Get
./bin/gen-cluster-config -n 5 -base-port 52000 -out-dir configs/scale/n5
# (avviare il cluster generato, poi:)
./bin/bench-client -addr localhost:52000 -n 50 -nodes 5 -warmup 5 -repeat 5 -out results.csv

# Fault-tolerance: avvia un cluster, uccide il leader, misura rielezione/convergenza
./bin/fault-tolerance -n 5 -trials 10 -base-port 57000 -out results-ft.csv
```

Entrambi gli strumenti sono autosufficienti: generano la propria
configurazione, avviano e fermano i processi necessari, misurano, salvano
il CSV.











### Come orientarsi tra i CSV già raccolti

`experiments/results/` contiene 10 file. 
I dati ufficiali raccolti dagli esperimenti ed ai quali si fa riferimento diretto nel report sono disponibili ai file `summary-scalability.csv` per la
scalabilità (già aggregato, leggibile a colpo d'occhio) e
`fault-tolerance-n5-ec2.csv` per la fault-tolerance.

Gli altri sono un confronto metodologico secondario
(vedi report, Sezione Discussion, sul perché l'ambiente di misura conta più
della potenza di calcolo grezza).

| File | Cosa contiene |
|---|---|
| **`results-n{3,5,9,20}-ec2.csv`** | Misure grezze di latenza Put/Get (una riga per operazione) sull'istanza EC2 — il dataset ufficiale di scalabilità |
| **`summary-scalability.csv`** | Le stesse misure EC2 **aggregate**: media/min/p50/p95/max per N e operazione — il modo più rapido di leggere il risultato, è la tabella riportata nel report |
| **`fault-tolerance-n5-ec2.csv`** | 10 trial di crash-e-rielezione del leader su EC2 — il dataset ufficiale di fault-tolerance |
| `results-n{3,5,9,20}.csv` | Le stesse misure di scalabilità, ma rifatte in locale sul portatile di sviluppo — tenute come confronto, mostrano risultati più rumorosi (vedere Discussion) |


**Legenda**:
- `summary-scalability.csv`: `nodes` = N testato ("totale" = aggregato su
  tutti gli N), `operation` = put/get, poi media/min/p50/p95/max in
  millisecondi.
- `results-n*.csv` (grezzi): una riga per singola operazione — `nodes` = N
  del cluster, `run` = numero di ripetizione, `operation`, `attempt` =
  indice dell'operazione dentro quella ripetizione, `latency_ms`.
- `fault-tolerance-n5-ec2.csv`: una riga per trial — `crashed_leader`/
  `new_leader` e i rispettivi term, `reelection_ms`/`convergence_ms` (i
  tempi misurati), `converged_count`/`expected_count` (quanti superstiti
  hanno riconosciuto il nuovo leader, su quanti ci si aspettava),
  `service_restored` (se una scrittura dopo il crash è andata a buon
  fine).

## Struttura del repository

```
cmd/                consensus-node, proxy, snapshot, raft-client (eseguibili)
internal/
  raft/              algoritmo di consenso (elezione, replica, RPC client/server)
  raftlog/           persistenza del log su disco
  kvstore/           state machine chiave-valore
  discovery/         pattern Service Discovery
  proxy/              pattern Circuit Breaker + routing del proxy
  snapshot/          servizio di snapshot & backup
  config/            caricamento/validazione configurazione YAML
proto/               definizioni gRPC/Protocol Buffers
configs/             file di configurazione YAML (locale e Docker)
deployments/         Dockerfile
experiments/         strumenti per gli esperimenti di valutazione
report/              report LaTeX, diario di sviluppo, diagrammi, evidenza deployment
docker-compose.yml   orchestrazione dei 7 container
```
