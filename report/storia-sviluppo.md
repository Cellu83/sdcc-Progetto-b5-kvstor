# Storia dello sviluppo — Progetto B5 (Sistemi Distribuiti e Cloud Computing)

Questo documento ripercorre lo sviluppo del progetto fase per fase, così come
è realmente avvenuto: le decisioni prese, il perché, i problemi incontrati e
come sono stati risolti. Non è ancora il report finale (quello richiederà la
struttura ACM/IEEE con Abstract, Background, ecc. e un limite di pagine) —
è la base narrativa completa da cui il report finale verrà poi ritagliato.

---

## Fase 0 — Setup iniziale del progetto

**Obiettivo**: prima di scrivere qualunque logica di consenso, mettere in
piedi lo scheletro minimo del progetto: il modulo Go, un sistema di
configurazione, e uno stub eseguibile che lo usa. Nessuna logica applicativa
in questa fase — solo le fondamenta su cui costruire tutto il resto.

**Cosa è stato costruito**:
- Il modulo Go (`go.mod`) e la struttura di cartelle di base (`cmd/`,
  `internal/`, `configs/`).
- `internal/config`: un pacchetto che carica la configurazione di un nodo da
  file YAML, con struct tipizzate (`NodeConfig`, `ClusterConfig`, `RaftConfig`,
  `SnapshotConfig`) invece di leggere valori sparsi a mano.
- Tre file di esempio (`configs/node1.yaml`, `node2.yaml`, `node3.yaml`) che
  descrivono un cluster locale a 3 nodi.
- Uno stub di `cmd/consensus-node/main.go` che si limita a caricare la
  configurazione e stamparla — nessun server gRPC, nessun Raft ancora.

**Decisione di design — nessun valore hard-coded**: fin dal primo commit, la
regola è stata che *nessun* parametro operativo del sistema (ID del nodo,
indirizzi dei peer, porte, timeout di elezione, intervallo di heartbeat,
intervallo di snapshot, livello di log) potesse vivere nel codice. Tutto
passa dal file YAML caricato via `config.Load`. Questa scelta, presa subito e
mai più rimessa in discussione, è quella che ha reso possibile — molte fasi
più avanti, nella Fase 10 — generare programmaticamente cluster di dimensioni
diverse (`experiments/gen-cluster-config`) *riusando le stesse struct* invece
di scrivere un generatore separato: il tool di generazione altro non fa che
istanziare `config.Config`/`config.ProxyConfig` e serializzarle, con la
garanzia che il formato prodotto sia sempre quello che `config.Load` si
aspetta davvero, perché è lo stesso identico tipo.

**Perché struct tipizzate e non un semplice `map[string]interface{}`**: la
validazione (range dei timeout, presenza dei campi obbligatori, coerenza
peers/cluster) è imposta dal compilatore e da `config.Load` stesso al momento
del caricamento, non lasciata a controlli sparsi altrove nel codice — un file
di configurazione malformato fallisce subito, con un errore chiaro, invece di
propagare uno zero-value silenzioso in qualche punto lontano del sistema.

**Da ricordare per il report**: questa fase non ha prodotto nulla di
"dimostrabile" a schermo (uno stub che stampa la sua configurazione), ma è la
decisione qui presa — configurazione esterna, zero hard-coding — che ha
permesso in seguito: (a) di far girare lo stesso identico binario come
`node1`, `node2`, `node3` in Docker Compose semplicemente passando config
diverse; (b) di generare cluster di N nodi per gli esperimenti di
scalabilità senza toccare il codice sorgente. Vale la pena menzionarlo nel
report come scelta architetturale iniziale con ricadute fino alla fine del
progetto, non come dettaglio implementativo minore.

---

## Fase 1-3 — State machine, persistenza, plumbing gRPC

**Obiettivo**: costruire i tre pezzi che Raft userà ma che *non sono* Raft
in senso stretto — la state machine applicativa, la persistenza su disco
dello stato che deve sopravvivere a un riavvio, e il canale di comunicazione
in rete tra nodi — prima ancora di scrivere la prima riga di logica di
elezione o replicazione. L'idea è poter verificare ogni strato singolarmente,
piuttosto che scrivere Raft e la rete insieme e non sapere dove cercare se
qualcosa non torna.

**Correzione avvenuta in questa fase — il module path**: il progetto era
partito con un modulo Go placeholder,
`github.com/federicocola/sdcc-b5-kvstore`, scelto prima ancora di creare il
repository reale su GitHub. In questa fase il modulo è stato rinominato al
suo nome definitivo, `github.com/Cellu83/sdcc-Progetto-b5-kvstor`, per
combaciare con il repository effettivamente creato — con conseguente
aggiornamento di tutti gli import interni. Un dettaglio minore, ma buono da
ricordare se nel report si mostra la cronologia dei commit: il primo commit
usa ancora il path placeholder.

**Cosa è stato costruito**:

- **`internal/kvstore`** — la state machine chiave-valore vera e propria.
  Deliberatamente ignara di rete e di consenso: sa solo eseguire un
  `Command` (`Put`/`Delete`) tramite `Apply`, e rispondere a `Get`. L'accesso
  concorrente è protetto da un `sync.RWMutex`, perché più goroutine
  (handler gRPC del proxy più avanti, e l'applicazione ordinata del log
  Raft) accederanno allo store in parallelo — questa è, in nuce, la
  gestione della concorrenza su risorse condivise richiesta dalla traccia
  del progetto in generale, non solo la parte B5-specifica.

- **`internal/raftlog`** — la persistenza su disco dei tre pezzi di stato
  che Raft impone di rendere durevoli: `CurrentTerm`, `VotedFor`, e il log
  delle entry. La scrittura è **atomica**: si scrive prima su un file
  temporaneo (`raft_state.json.tmp`) e solo dopo si fa una `rename` sopra il
  file definitivo. Questo garantisce che un crash del processo a metà
  scrittura non lasci mai uno stato corrotto/parziale su disco — al riavvio
  il nodo troverà o lo stato vecchio (se il crash è avvenuto prima della
  rename) o quello nuovo completo (se dopo), mai una via di mezzo. È una
  scelta presa presto e mai rivista, e diventerà rilevante molto più avanti
  (Fase 9) quando si osserverà dal vivo su AWS cosa succede — correttamente
  — quando un nodo viene riavviato con questo stato persistito.

- **`proto/raft` e `proto/kvstore`** — le definizioni Protobuf/gRPC per le
  due API del sistema: `RaftService` (RequestVote, AppendEntries, e le RPC
  di sola lettura GetStatus/GetSnapshot che arriveranno nelle fasi
  successive) e `KVStoreService` (Get/Put/Delete, l'API applicativa esposta
  a client e proxy).

- **`internal/raft/server.go`** — un server gRPC che implementa
  `RaftServiceServer`, ma con **risposte finte**: `RequestVote` concede
  sempre il voto, `AppendEntries` risponde sempre con successo. Nessuna
  delle regole di sicurezza di Raft è ancora implementata. Lo scopo
  dichiarato di questo stub, esplicito nel commento del file stesso, è
  verificare che due nodi sappiano davvero parlarsi in rete — che il
  deserializzarsi delle RPC funzioni — prima di introdurre la logica reale,
  cosicché un eventuale bug di rete non si confonda con un bug
  nell'algoritmo.

- **`cmd/raft-client`** — un piccolo tool a riga di comando per chiamare a
  mano le RPC di un nodo già in esecuzione (RequestVote, AppendEntries, e
  poi Get/Put/Delete non appena l'API applicativa è stata esposta). Nato qui
  come strumento di verifica manuale per questa fase, è rimasto utile fino
  alla fine del progetto — è lo stesso tool riusato in Fase 10 per il
  controllo di "il servizio è tornato a rispondere dopo il crash del
  leader?" nell'esperimento di fault-tolerance.

**Da ricordare per il report**: il filo conduttore di questa fase è la
separazione netta tra "il meccanismo di trasporto/persistenza funziona" e
"l'algoritmo è corretto" — verificati come due preoccupazioni distinte, in
sequenza, invece che insieme. Vale la pena spiegarlo nel report come
approccio metodologico deliberato allo sviluppo di un sistema distribuito,
non solo descrivere i moduli uno per uno.

---

## Fase 4 — Leader election

**Obiettivo**: sostituire gli handler finti della fase precedente con la
prima vera fetta di Raft — la macchina a stati Follower/Candidate/Leader e
tutte le regole di sicurezza che decidono chi può diventare leader. Ancora
nessuna replica del log applicativo: solo elezione.

**Cosa è stato costruito**:

- **`internal/raft/node.go`** — il cuore dell'algoritmo. Uno stato esplicito
  a tre valori (`Follower`, `Candidate`, `Leader`), un election timer
  **randomizzato** (per evitare che più nodi tentino l'elezione
  esattamente nello stesso istante, causando split vote sistematici), e le
  transizioni di stato (`becomeFollowerLocked`, `becomeLeaderLocked`)
  protette da un mutex, dato che vengono toccate sia dal ciclo di elezione
  che dagli handler RPC in arrivo su goroutine diverse.

- **Le tre regole di sicurezza di `HandleRequestVote`**, implementate e poi
  verificate una per una nei test:
  1. *Rifiuto dei term vecchi* — un candidato con `term` inferiore al nostro
     non viene nemmeno considerato.
  2. *Un solo voto per term* — se abbiamo già votato per un candidato
     diverso in questo term, il voto viene negato (rivotare lo stesso
     candidato, es. per un messaggio duplicato, resta permesso).
  3. *Election restriction* — il log del candidato deve essere almeno
     aggiornato quanto il nostro (confrontando prima l'ultimo term del log,
     poi a parità di term l'ultimo indice); altrimenti eleggerlo
     rischierebbe di far perdere entry già confermate da una maggioranza
     precedente. Questa è la regola che, più avanti nel progetto (Fase 9),
     si è potuta osservare "in azione" dal vivo su AWS.

- **Riconoscimento del leader via `HandleAppendEntries`** — in questa fase
  l'RPC non applica ancora nessuna entry di log (arriverà in Fase 5): serve
  solo a far riconoscere ai follower un leader legittimo e a resettare il
  loro timer di elezione, così un leader attivo che manda heartbeat regolari
  impedisce elezioni spurie.

- **Decisione di design — tipi RPC "puri Go" separati dai tipi Protobuf**:
  `RequestVoteArgs`/`Result` e `AppendEntriesArgs`/`Result` sono strutture
  Go semplici, distinte dai tipi generati da `raft.proto`. Lo scopo,
  dichiarato nel codice stesso, è poter testare tutta la logica di consenso
  (`node_test.go`, 9 unit test) chiamando direttamente questi metodi, senza
  dover avviare un vero server/client gRPC per ogni test — la conversione
  tra i due mondi (fatta più avanti in `internal/raft/convert.go`) resta un
  dettaglio isolato al bordo del sistema, non sparso ovunque nella logica.

**Verifica — due livelli, come nella fase precedente**: prima 9 unit test
sulla logica pura (nessuna rete, nessun processo reale — solo chiamate
dirette ai metodi di `Node`), poi una verifica end-to-end con **3 processi
locali reali** che eleggono un leader e, dopo un crash indotto a mano del
leader, ne eleggono uno nuovo. Questo secondo tipo di verifica — avviare
davvero i processi da terminale e osservare i log — è il pattern di lavoro
che si è ripetuto per tutto il resto del progetto ogni volta che c'era
comportamento distribuito da capire, non solo codice da far compilare.

**Da ricordare per il report**: questa è la fase in cui il sistema comincia
a mostrare comportamento distribuito osservabile (elezione, timeout,
rielezione dopo un crash) — un buon punto, nel report, per introdurre le
tre regole di sicurezza di Raft con un piccolo esempio concreto, dato che
sono il cuore dell'algoritmo e la parte più probabile su cui verranno fatte
domande in sede di presentazione.

---

## Fase 5-6 — Log replication, client proxy, Service Discovery, Circuit Breaker

Questa fase, fatta in un unico commit, copre due traguardi distinti: la
replica vera e propria del log (Fase 5) e i due pattern architetturali
richiesti dalla traccia del progetto, incarnati nel Client proxy (Fase 6).

### Fase 5 — Log replication reale

**Obiettivo**: sostituire l'`AppendEntries` "riconosci solo il leader" della
Fase 4 con la vera replicazione del log, e collegare finalmente client →
log → state machine, così le scritture diventano davvero persistenti e
replicate, non solo locali.

**Cosa è stato costruito**:

- **Controllo di coerenza del log** — `AppendEntries` ora verifica
  `prevLogIndex`/`prevLogTerm` contro il proprio log prima di accettare
  nuove entry: se non c'è coincidenza, il follower rifiuta, e il leader
  risolve il disaccordo **troncando** il proprio log del follower fino al
  punto di divergenza e ritentando con entry precedenti — il meccanismo
  standard con cui Raft fa convergere log divergenti dopo un cambio di
  leader.
- **Avanzamento del `commitIndex` per maggioranza**, in
  `maybeAdvanceCommitIndexLocked`: il leader considera committato l'indice
  più alto N per cui la maggioranza dei nodi (se stesso incluso) ha
  replicato fino a N. **Con una condizione aggiuntiva, sottile e facile da
  dimenticare**: quell'entry N deve appartenere al *term corrente* del
  leader. È la regola di sicurezza del paper di Raft, §5.4.2 — un leader
  non può concludere che un'entry di un term *precedente* sia committata
  solo contando le repliche, perché in casi patologici quell'entry potrebbe
  ancora venire sovrascritta da un futuro leader; deve aspettare che si
  cometta insieme a (almeno) un'entry del proprio term corrente. Questa
  regola, implementata qui, è la stessa che più avanti (Fase 9) si
  osserverà "dal vivo" su un'istanza EC2 riavviata.
- **`Node.Propose`** — il punto in cui client, log Raft e state machine si
  incontrano finalmente: accetta un comando applicativo, lo aggiunge al
  proprio log (solo se il nodo è Leader — è così che si garantisce
  consistenza forte: letture e scritture passano solo dal Leader, mai da un
  follower), lo replica, e ne aspetta il commit prima di rispondere al
  chiamante.

**Verifica — tre livelli, non due**: agli unit test sulla logica pura si è
aggiunto (a) un test di integrazione con 3 nodi reali comunicanti via gRPC
nello stesso processo di test, e (b) una prova con processi realmente
separati in cui la replica è stata confermata **ispezionando a mano i file
`raft_state.json` persistiti dei follower** — cioè verificando sul disco,
non solo a schermo, che il log fosse davvero arrivato e persistito
ovunque.

### Fase 6 — Client proxy: Service Discovery + Circuit Breaker

**Obiettivo**: costruire il secondo tipo di nodo richiesto dalla traccia —
il Client proxy, stateless — e con esso i due pattern architetturali
richiesti esplicitamente dalla specifica del progetto.

**Cosa è stato costruito**:

- **`internal/discovery` — Service Discovery**: parte da una lista statica
  di nodi seed (gli stessi indirizzi noti a bootstrap) e mantiene una vista
  aggiornata di chi sono i peer e chi è l'attuale Leader, interrogando
  periodicamente il cluster tramite una nuova RPC di sola lettura,
  `RaftService.GetStatus` (introdotta apposta in questa fase). È il
  meccanismo che permette al proxy di non avere mai un indirizzo fisso del
  Leader scritto da qualche parte: lo scopre e lo riscopre dinamicamente.
- **`internal/proxy/circuitbreaker.go` — Circuit Breaker**: tre stati
  classici del pattern — *closed* (funzionamento normale), *open* (troppi
  fallimenti consecutivi verso un indirizzo: le chiamate vengono rifiutate
  subito, senza nemmeno provare la rete, per un periodo di raffreddamento),
  *half-open* (raffreddamento scaduto: una sola chiamata di prova per
  capire se il nodo è tornato). Lo scopo dichiarato è evitare che il proxy
  continui a "martellare" di richieste — e ad aspettare inutilmente il
  timeout di rete — un nodo che sa già essere caduto.
- **`internal/proxy/proxy.go`** combina i due pattern: instrada le
  richieste verso il Leader corrente secondo la vista della Service
  Discovery, e se il Leader cambia (o cade) si ridirige da solo verso quello
  nuovo, con il Circuit Breaker a isolare rapidamente i nodi non più
  raggiungibili nel frattempo.

**Verifica**: un test che uccide il leader **a metà test** (per verificare
che il proxy si ridiriga davvero da solo verso il nuovo leader, non solo
che sappia parlare con quello iniziale) e una prova con **4 processi
separati reali** (3 nodi + proxy).

**Bug scoperto e corretto in questa fase — `Node.Stop()` non idempotente**:
proprio i test con processi/goroutine di chiusura multipla hanno fatto
emergere un panico da doppia chiusura di canale quando `Stop()` veniva
chiamato più di una volta sullo stesso nodo (una volta per simulare il
crash nel test, una volta nella pulizia finale del test stesso). Corretto
avvolgendo `close(n.stopCh)` in un `sync.Once`, rendendo `Stop()`
idempotente indipendentemente da quante volte venga invocato. Un bug piccolo
ma istruttivo: è emerso non da un caso d'uso "reale" previsto, ma dal modo
in cui i test stessi simulavano un crash — buon esempio, per il report, di
come i test di integrazione a processi reali abbiano trovato categorie di
bug diverse da quelli sulla sola logica.

**Da ricordare per il report**: questa fase è quella in cui compaiono
esplicitamente entrambi i pattern architetturali richiesti dalla traccia
(Service Discovery e Circuit Breaker) — vanno descritti nel report non solo
come "ce li abbiamo messi perché richiesti", ma spiegando perché risolvono
un problema reale del sistema (scoperta dinamica del leader senza indirizzi
fissi; isolamento rapido di nodi caduti senza pagare timeout di rete ripetuti).

**Limitazione nota, emersa rileggendo questa fase in sede di report**: la
lista dei peer di ogni nodo (`Config.Peers`) è statica, letta una volta sola
dallo YAML all'avvio, e non cambia mai durante l'esecuzione — non esiste
nessuna RPC per aggiungere un nodo a un cluster già in funzione. Un nodo
**già noto** che crasha e riparte si riallinea da solo, senza problemi
(grazie al meccanismo di coerenza del log di Fase 5): il suo ID è ancora
nella lista di tutti gli altri. Ma un nodo **nuovo**, mai presente nella
configurazione iniziale, non può unirsi a runtime: nessun leader lo
conterebbe per il quorum di maggioranza, e la Service Discovery — che pure
scopre dinamicamente *chi* tra i nodi noti è il leader — non può "scoprire"
un nodo che nessun altro nodo conosce già, perché la lista che rispecchia è
comunque quella statica. È il cambio di membership dinamico che il paper di
Raft tratta a parte (§6, joint consensus): non richiesto dalla traccia B5,
deliberatamente fuori scope, ma da dichiarare esplicitamente come
limitazione nota nel report — è un punto plausibile di domanda al Q&A della
presentazione.

---

## Fase 7 — Snapshot & backup service

**Obiettivo**: costruire il terzo e ultimo tipo di nodo richiesto dalla
traccia — il servizio di Snapshot & backup, stateless — che scarica
periodicamente un'istantanea dello stato applicativo dai consensus node e
la scrive su un checkpoint esterno.

**Cosa è stato costruito**:

- **Nuova RPC `RaftService.GetSnapshot`**: un consensus node espone la
  propria istantanea corrente tramite `Store.Snapshot()` (nuovo metodo:
  restituisce una copia difensiva dell'intera mappa chiave-valore sotto
  `RLock`, così il chiamante ottiene uno stato coerente in quel preciso
  istante), insieme a `last_included_index`/`last_included_term` — i due
  valori che identificano *fino a dove* quello snapshot è valido.
- **`internal/snapshot`** (nuovo pacchetto): il servizio Snapshot vero e
  proprio. **Riusa `internal/discovery`** (lo stesso meccanismo di Service
  Discovery costruito per il proxy in Fase 6) per trovare un nodo
  raggiungibile a cui chiedere lo snapshot — nessuna logica di scoperta
  duplicata. Scarica lo stato e lo scrive su un file di checkpoint esterno
  in modo **atomico**, con lo stesso schema file-temporaneo-poi-rename già
  usato da `raftlog.Storage` fin dalla Fase 1-3: la stessa soluzione a un
  problema (evitare stati parziali su crash a metà scrittura) riapplicata
  coerentemente in due punti diversi del sistema.

**Decisione di design — i consensus node non toccano mai il proprio log per
via dello snapshot**: questo è il punto più importante concettualmente di
questa fase. Un'implementazione "da manuale" di Raft userebbe lo snapshot
anche per **comprimere** il log sui nodi stessi (troncare le entry già
snapshottate, così il log non cresce all'infinito). Qui si è scelto
deliberatamente di *non* farlo: il servizio Snapshot è puramente esterno,
in sola lettura verso i nodi — scarica e basta, non modifica mai gli
indici o il log di nessun consensus node. La motivazione esplicita è stata
non rimettere in discussione la matematica degli indici (`commitIndex`,
`matchIndex`, la regola §5.4.2) già scritta e verificata nelle fasi
precedenti, per un rischio che i requisiti minimi della spec (un servizio
di *backup*, non necessariamente la compattazione del log) non
giustificavano. Una scelta di scope deliberata, non una scorciatoia per
pigrizia — vale la pena presentarla così nel report.

**Pulizia collegata**: `Config.Snapshot` — un campo introdotto già nello
stub di configurazione della Fase 0 ma mai realmente usato dai consensus
node — è stato rimosso dalla config dei nodi in questa fase. L'intervallo
di polling dello snapshot appartiene alla configurazione dedicata del
servizio Snapshot (`configs/snapshot.yaml`, nuovo), non al singolo
consensus node: un piccolo esempio di residuo morto identificato e tolto
non appena il suo posto corretto è diventato chiaro, invece di lasciarlo
lì "perché tanto non dà fastidio".

**Verifica — tre livelli anche qui**: unit test su `Store.Snapshot()`, un
test di integrazione che produce due checkpoint successivi *dopo* scritture
intermedie e verifica che il secondo rifletta davvero lo stato aggiornato
(non solo che un file venga scritto, ma che il suo contenuto sia corretto
nel tempo), e una prova con 4 processi separati reali — incluso lasciar
girare il ciclo di polling automatico per un intervallo di attesa e
osservare che il checkpoint si aggiorni da solo, senza intervento manuale.

**Da ricordare per il report**: con questa fase il sistema ha tutti e tre i
tipi di nodo richiesti dalla traccia (consensus, proxy, snapshot) e
entrambi i pattern architetturali. È un buon punto naturale, nel report,
per una figura d'insieme dell'architettura — i tre tipi di nodo e come si
parlano tra loro via gRPC — prima di passare alle fasi di deployment e
valutazione sperimentale.

---

## Fase 8 — Containerizzazione con Docker Compose

**Obiettivo**: passare da "3 processi Go lanciati a mano su `localhost`" a
un sistema orchestrato con Docker Compose — il primo passo verso il
requisito di deployment su AWS EC2 (Fase 9).

**Cosa è stato costruito**:

- **Un solo Dockerfile, generico e parametrizzato** (`deployments/Dockerfile`),
  invece di tre quasi identici. Build multi-stadio: uno stage `build` con
  l'intero toolchain Go (`golang:1.26-alpine`, pesante, usato solo per
  compilare), e uno stage finale minimale (`alpine:3.20`) che contiene
  *solo* il binario già compilato — mai il compilatore né i sorgenti. Quale
  dei tre binari compilare (`consensus-node`, `proxy`, o `snapshot`) è
  deciso da un build-arg, `SERVICE`, con una guardia esplicita
  (`test -n "$SERVICE" || ...`) che fa fallire la build subito, con un
  messaggio chiaro, se il build-arg non viene passato — invece di un errore
  criptico più a valle. Le immagini risultanti sono piccole (circa 24MB
  ciascuna, verificato più avanti in Fase 10 su AWS), perché non portano con
  sé il toolchain di compilazione.
- **Caching dei layer Docker**: `go.mod`/`go.sum` vengono copiati e scaricati
  (`go mod download`) *prima* di copiare il codice sorgente. Dato che le
  dipendenze cambiano molto meno spesso del codice applicativo, Docker può
  riusare questo layer dalla cache a ogni rebuild finché `go.mod`/`go.sum`
  non cambiano — build successive molto più veloci durante lo sviluppo.
- **`docker-compose.yml` con 5 servizi**: lo stesso Dockerfile viene
  riferito 5 volte, ciascuna con un `SERVICE` build-arg diverso e un file di
  configurazione diverso montato come volume in sola lettura (non incorporato
  nell'immagine: le immagini restano generiche, la configurazione resta
  esterna, in linea con la decisione presa fin dalla Fase 0). Volumi
  **nominati** (`node1-data`, `node2-data`, `node3-data`, `snapshot-data`)
  danno persistenza al `data_dir` di ogni nodo e ai checkpoint dello
  snapshot service, indipendente dal ciclo di vita del container — un
  container può essere ricreato senza perdere lo stato Raft persistito.
  Porte pubblicate verso l'host solo per i 3 nodi e per il proxy, per poter
  usare `raft-client` dall'host durante il debug.
- **Configurazioni dedicate all'ambiente Docker** (`configs/docker/`):
  identiche a quelle locali tranne per un dettaglio cruciale — gli indirizzi
  dei peer usano i **nomi dei servizi Compose** (`node1`, `node2`, `node3`)
  invece di `localhost`. La rete bridge di default creata da Compose
  risolve questi nomi via DNS interno, quindi ogni container raggiunge gli
  altri per nome, non per IP fisso.

**Verifica — l'intera suite di prove già fatte in locale, rifatta dentro
Docker**: elezione del leader confermata via i log (stavolta con l'indirizzo
del leader espresso come nome di servizio, non `localhost:porta`), Put/Get
reali dall'host verso il proxy containerizzato, un `docker kill` sul
container del leader con failover del proxy confermato (stesso collaudo
della Fase 6, ma con container Docker reali al posto di processi nudi), e
il riavvio di un nodo con recupero corretto dello stato dal volume
persistente — quest'ultimo test è il presagio di quello che si osserverà
più a fondo, dal vivo su AWS, nella Fase 9.

**Da ricordare per il report**: il punto da sottolineare qui è che
containerizzare non ha richiesto *nessuna* modifica al codice applicativo —
solo un Dockerfile, dei file di configurazione con indirizzi diversi, e un
file di orchestrazione. È una conseguenza diretta, e vale la pena dirlo
esplicitamente nel report, della decisione di Fase 0 di non avere mai nulla
di hard-coded: lo stesso identico binario può girare come processo nudo su
`localhost` o come container su una rete Docker, cambiando solo il file YAML
che gli si passa.

---

## Fase 9 — Deployment reale su AWS EC2

*(nessun commit dedicato: questa fase è puro deployment/operatività sullo
stesso `docker-compose.yml` della Fase 8, su un'istanza reale — non ha
prodotto codice nuovo, ma ha prodotto scoperte importanti)*

**Obiettivo**: soddisfare il requisito di deployment della traccia
("Docker Compose, running on an AWS EC2 instance") su un'istanza AWS
Academy Learner Lab reale, non solo in locale, e verificarlo dall'esterno.

**Scelta operativa**: gestione interamente tramite Console AWS (non CLI),
per scelta esplicita — istanza `t2.micro`, regione `us-east-1`, Amazon Linux
2023, connessione via **Session Manager** (terminale nel browser, non
richiede una chiave SSH) invece di EC2 Instance Connect.

**Problemi incontrati e risolti — parte "infrastruttura"**:

- **Esaurimento risorse durante la build**: `docker compose up -d --build`
  tenta di buildare i 5 servizi **in parallelo**; su un `t2.micro` (1 vCPU,
  1GB RAM) questo ha saturato CPU e memoria al punto che l'istanza è
  diventata irraggiungibile — nemmeno una nuova sessione Session Manager
  riusciva ad aprirsi. Risolto riavviando l'istanza dalla Console EC2
  (operazione che non dipende da nessun agente dentro l'istanza stessa, utile
  proprio perché l'istanza non rispondeva) e poi buildando **un servizio
  alla volta** (`docker compose build node1`, poi `node2`, ecc.) invece del
  flag combinato `--build`. Da ricordare per il report come limite pratico e
  reale dell'hardware assegnato dal Lab, non un problema del sistema.
- **Connessione al terminale sbagliata**: un tentativo iniziale di
  connessione dalla scheda "EC2 Instance Connect" invece di "Session
  Manager" ha dato un errore SSH fuorviante — risolto semplicemente
  selezionando la scheda corretta.
- **Repository non trovato dopo un riavvio**: il `git clone` iniziale non
  aveva fatto in tempo a completarsi prima che l'istanza si bloccasse per il
  problema precedente — risolto riclonando da zero.
- **Regola del Security Group scaduta**: dopo un riavvio dell'istanza, le
  chiamate da `raft-client` dal Mac fallivano in timeout
  (`DeadlineExceeded`) — non un problema del sistema, ma il fatto che l'IP
  di casa dell'utente era cambiato da quando la regola "My IP" era stata
  creata. Risolto riselezionando "My IP" nella regola inbound, che
  ricalcola l'IP corrente.

**La scoperta più significativa — la regola di sicurezza §5.4.2 osservata
dal vivo**: dopo un riavvio dell'istanza, una `Get` su una chiave che
sicuramente esisteva già (scritta prima dello stop) restituiva
`found=false`. Non era una perdita di dati: `commitIndex` e `lastApplied`
sono deliberatamente **volatili** (mai persistiti — solo `CurrentTerm`,
`VotedFor` e il log lo sono, per costruzione fin dalla Fase 1-3), e si
azzerano a ogni riavvio del processo. Il nuovo leader, eletto in un nuovo
term dopo il riavvio, non poteva far avanzare il proprio `commitIndex` oltre
le entry di un term *precedente* — esattamente la regola implementata in
`maybeAdvanceCommitIndexLocked` fin dalla Fase 5-6 — finché non fosse
avvenuta una nuova scrittura nel term corrente. Confermato scrivendo una
nuova chiave: la vecchia è ridiventata immediatamente visibile, perché il
commitIndex è potuto avanzare in ordine di log oltre entrambe le entry in un
colpo solo. Non un bug scoperto e corretto, ma **una regola già scritta e
studiata, vista in azione dal vivo su un'infrastruttura reale** — un pezzo
di narrazione con cui il report può mostrare comprensione profonda
dell'algoritmo, non solo la sua implementazione.

**Verifica finale**: elezione del leader, Put/Get, failover dopo crash del
leader, e recupero dello snapshot — tutti riverificati dal vivo su questa
istanza, con chiamate reali dall'esterno (dal Mac dell'utente, non
dall'istanza stessa) per confermare la raggiungibilità pubblica.

**Da ricordare per il report**: questa fase va raccontata su due binari
paralleli — da un lato la meccanica del deployment cloud (Learner Lab,
Session Manager, vincoli di un'istanza `t2.micro`, Security Group), dall'altro
la scoperta della regola §5.4.2 "in natura", che merita una sezione a sé
nel report (probabilmente in Discussion/Results) perché dimostra che il
sistema si comporta correttamente proprio nel caso limite più sottile che
Raft è progettato per gestire.

---

## Fase 10 — Valutazione sperimentale: scalabilità e fault-tolerance

**Obiettivo**: soddisfare i due scenari di valutazione richiesti dalla
traccia — tempo di risposta RPC al variare del numero di nodi, e tempo di
rielezione/convergenza dopo il crash del leader — con dati onesti e
metodologicamente solidi, non solo "un numero da mettere nel report".

**Decisione preliminare — esperimenti in locale, non su EC2 (motivazione da
riportare esplicitamente)**: si è deciso di eseguire gli esperimenti sul Mac
dell'utente invece che sull'istanza AWS Academy `t2.micro` usata per il
deployment. Motivo: un'istanza a 1 vCPU condivisa tra un numero crescente di
processi consensus avrebbe introdotto un collo di bottiglia artificiale da
contesa di CPU, che avrebbe misurato i limiti di quell'istanza minuscola
invece del comportamento reale dell'algoritmo. Il requisito di deployment su
EC2 era già soddisfatto indipendentemente (Fase 8-9); gli esperimenti sono
un requisito separato, sul comportamento dell'algoritmo. *(Questa decisione
verrà poi in parte rivista nel corso della fase stessa — vedi sotto.)*

### Strumenti costruiti

- **`experiments/gen-cluster-config`**: genera i file YAML per un cluster di
  N nodi + proxy **riusando direttamente le struct di `internal/config`**
  (le stesse della Fase 0) invece di scrivere YAML a mano — garanzia
  strutturale che il formato prodotto sia sempre quello che `config.Load` si
  aspetta, con autovalidazione immediata dopo la generazione. Tutti i
  parametri Raft restano fissi tra una dimensione di cluster e l'altra: N è
  l'unica variabile indipendente.
- **`experiments/bench-client`**: misura la latenza di Put/Get **in
  sequenza** (una RPC alla volta, aspettando la risposta prima della
  successiva) per isolare il tempo di risposta dal throughput sotto carico
  concorrente — sono due esperimenti concettualmente diversi, e la traccia
  chiede il primo. Nato con una singola passata di misure, è stato **esteso
  con `-warmup`** (richieste scartate, per far stabilizzare le connessioni
  gRPC prima di cronometrare) **e `-repeat`** (più cicli indipendenti
  aggregati insieme) non appena i primi risultati si sono rivelati rumorosi
  — vedi sotto.
- **`experiments/fault-tolerance`**: orchestra l'intero esperimento di
  crash del leader — avvia un cluster, lo scopre via RPC, lo uccide, misura.
  Descritto in dettaglio più sotto.

### Scalabilità — dal rumore alla scoperta metodologica

La prima passata (N=3/5/9, sul Mac) ha dato risultati **non monotoni** e
poco convincenti: PUT medio 26.72/24.64/27.61 ms, GET medio
20.74/18.02/21.59 ms. Su richiesta esplicita dell'utente, prima di
accontentarsi si è verificato che non ci fossero rielezioni spurie del
leader a confondere la misura tra un test e l'altro (nessuna trovata), e si
è aggiunta la media su più ripetizioni indipendenti (`-repeat`) per
combattere il rumore — il pattern non è cambiato.

Il ragionamento che ha sbloccato la situazione: la latenza **GET** è una
lettura locale sul Leader, **teoricamente indipendente da N** (nessuna
replica coinvolta). Se anche il GET mostra lo stesso andamento non
monotono del PUT, la spiegazione più economica non è che il metodo di
misura sia sbagliato, ma che il **rumore di sistema** (il Mac, condiviso con
la stessa sessione di lavoro che stava producendo la misura) domini
sull'effetto reale a queste differenze di N. Offerta la scelta tra inseguire
un segnale più pulito o documentare onestamente il limite, l'utente ha
scelto esplicitamente la seconda strada — una decisione metodologica
matura, non una scorciatoia.

Per curiosità, e per vedere l'effetto di N su una scala davvero grande
("possiamo testare fino a 50 o 200 nodi?" → si è convenuto di partire da
N=20 come primo salto netto), si è testato **N=20 in locale**: al bootstrap,
13 tentativi di elezione falliti per split-vote prima di convergere — un
effetto reale e non ancora misurato sistematicamente della dimensione del
cluster sulla probabilità di split-vote all'avvio a freddo (più nodi, più
probabilità che i timeout randomizzati di più candidati cadano vicini).
Ma la latenza RPC a regime è rimasta comunque dentro il range rumoroso
già visto (PUT 24.07ms, GET 19.51ms) — nessuna sorpresa qui.

**La svolta — lo stesso esperimento su EC2**: su suggerimento dell'utente,
si è testato N=20 anche sulla vera istanza AWS `t2.micro`, per curiosità/
confronto. Risultato sorprendente: **GET medio è sceso a 3.21ms** (contro
~19-21ms sul Mac) — un numero molto più plausibile per una lettura locale in
memoria. La spiegazione non è la potenza di calcolo (il t2.micro è
oggettivamente più debole del Mac), ma la **dedizione**: l'istanza EC2 non
esegue nient'altro, mentre il Mac condivideva la CPU con l'IDE, il browser,
e la sessione stessa che guidava l'esperimento. Questa scoperta ha motivato
la decisione di **rifare l'intera batteria N=3/5/9/20 su EC2** come dataset
ufficiale, tenendo il run sul Mac come confronto metodologico secondario.

**Risultato ufficiale (EC2, `experiments/results/results-n{3,5,9,20}-ec2.csv`,
5 ripetizioni × 50 operazioni, dopo 5 di warmup)**:

| N | PUT medio | GET medio |
|---|---|---|
| 3 | 9.35 ms | 3.00 ms |
| 5 | 9.20 ms | 3.28 ms |
| 9 | 9.31 ms | 3.09 ms |
| 20 | 15.16 ms | 3.21 ms |

Il GET resta piatto su tutti gli N, esattamente come previsto
teoricamente. Il PUT resta piatto fino a N=9 e sale nettamente a N=20 —
coerente con Raft: il leader deve attendere l'ack della maggioranza (11 su
20 follower a N=20), e con più follower cresce la coda per l'ack più lento
del gruppo di maggioranza. Una tabella riassuntiva con medie parziali (per
N) e totali aggregate, pronta per il report, è in
`experiments/results/summary-scalability.csv`.

**Intoppi tecnici superati lungo la strada su EC2** (utili da menzionare
come "esperienza operativa", non come bug del sistema): la toolchain Go
sull'istanza (installata via `dnf`) era troppo vecchia per il `go.mod` del
progetto, e il meccanismo di download automatico del toolchain
(`GOTOOLCHAIN=auto`) falliva per una questione di configurazione di
rete/checksum locale all'istanza — risolto installando il binario Go
ufficiale direttamente dal tarball; l'identità git non era mai stata
configurata su quell'istanza (mai fatto un commit da lì prima, solo
`pull`); un messaggio di commit contenente apostrofi ha rotto silenziosamente
il quoting della shell, facendo fallire il commit senza errori evidenti fino
a un controllo con `git status`; e la sessione del browser con Session
Manager si è chiusa accidentalmente più volte — mitigato lanciando i
processi con `nohup ... & disown`, così sopravvivono alla disconnessione.

### Fault-tolerance — crash del leader, rielezione e convergenza

**Precondizione necessaria — timestamp dei log a precisione di
microsecondo**: i log usavano i flag di default del pacchetto `log` di Go,
con precisione al secondo — inutile per misurare elezioni che durano
150-300ms. Aggiunto `log.SetFlags(log.LstdFlags | log.Lmicroseconds)` a
`cmd/consensus-node` e `cmd/proxy`. *(L'utente ha chiesto esplicitamente di
ricordare questa scelta per il report: va presentata come una decisione di
strumentazione deliberata, non un dettaglio implementativo minore.)*

**Lo strumento (`experiments/fault-tolerance`)**: per ogni trial, avvia un
cluster di N nodi, **scopre il leader corrente via la RPC `GetStatus`**
(scoperta via RPC, riservata all'individuazione iniziale del leader — non
serve misurare tempi qui), lo uccide con **`kill -9`** per simulare un vero
crash (non uno shutdown pulito), e poi misura **leggendo i timestamp già
presenti nei log dei nodi superstiti** (la strategia di misura scelta
esplicitamente dall'utente): tempo di rielezione = primo "eletto Leader" per
un term successivo a quello del leader caduto; tempo di convergenza =
l'ultimo "riconosciuto leader" tra i superstiti per quel nuovo leader/term.
Una scrittura via proxy prima e dopo il crash verifica anche che il
**servizio torni davvero operativo**, non solo che esista un nuovo leader.

**Validazione dello strumento in locale prima della run ufficiale**: un
primo test (N=5, 3 trial) ha rivelato un bug di fuso orario — `time.Parse`
senza fuso assume UTC, mentre `crashTime := time.Now()` usa il fuso locale,
producendo scarti assurdi di ~2 ore (esattamente l'offset UTC+2).
Corretto con `time.ParseInLocation(..., time.Local)`. Dopo la correzione,
risultati fisiologicamente plausibili (rielezione 198-469ms, convergenza
249-521ms) hanno confermato che lo strumento funzionava correttamente,
prima di fidarsi della run ufficiale.

**Risultato ufficiale (EC2, N=5, 10 trial indipendenti,
`experiments/results/fault-tolerance-n5-ec2.csv`)**: rielezione media
192.7ms (min 153.7, max 258.1), convergenza media 244.0ms (min 205.5, max
309.1). Il gap costante di circa 50ms tra rielezione e convergenza
corrisponde esattamente all'heartbeat interval configurato: il nuovo leader
manda l'`AppendEntries` subito dopo essersi eletto, e i follower lo
riconoscono quasi immediatamente. Tutti i 10 trial: convergenza completa
(3 superstiti su 3), servizio sempre ripristinato.

### Evidenza del deployment Docker Compose (raccolta durante questa fase)

Una domanda dell'utente — "durante questi esperimenti stiamo comunque
usando Docker Compose?" — ha chiarito una distinzione importante da rendere
esplicita nel report: gli esperimenti di questa fase usano processi Go
nativi (più leggeri, più veloci da orchestrare per decine di trial ripetuti),
**non** in contraddizione con il requisito di deployment — quello era già
stato soddisfatto e verificato separatamente in Fase 8-9. Colta l'occasione,
si è rilanciato davvero `docker compose up -d` sull'istanza EC2 (immagini
già buildate dalla Fase 8) per raccogliere evidenza fresca da mettere nel
report prima che il Learner Lab scada a fine corso: stato dei 5 container
(`docker compose ps`), una Put/Get riuscita dall'esterno (dal Mac dell'utente,
verso l'IP pubblico dell'istanza) e i log del container che mostrano
l'elezione del leader con i peer risolti via DNS del bridge Docker
(`node1@node1:50051`, non `localhost`).

È emersa anche una domanda più ampia, con risposta da riportare nel report:
la consegna del progetto non può includere un'istanza EC2 sempre viva
(l'account AWS Academy è temporaneo). Ciò che va consegnato è il codice
sorgente + `docker-compose.yml`, portabile su qualunque ambiente Docker; il
report documenta che il deployment *è stato* testato con successo su EC2
reale (con l'evidenza sopra), non che resterà vivo per sempre — la demo live
è il momento in cui si rilancia un'istanza fresca per mostrarlo davvero
funzionante.

**Da ricordare per il report**: questa fase ha tre filoni da tenere
distinti ma collegati — (1) la scoperta metodologica rumore-Mac vs
isolamento-EC2, da presentare come giudizio ingegneristico maturo, non come
errore recuperato; (2) i due dataset sperimentali puliti (scalabilità e
fault-tolerance), entrambi coerenti con la teoria di Raft; (3) la
distinzione, da chiarire esplicitamente, tra requisito di deployment
(soddisfatto in Fase 8-9), ambiente degli esperimenti (nativo, per
praticità), e natura del deliverable (codice portabile, non un'istanza
permanente).
