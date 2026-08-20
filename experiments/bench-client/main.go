// bench-client misura la latenza delle RPC Put/Get contro un proxy, per
// l'esperimento di scalabilità della Fase 10. Esegue le operazioni in
// sequenza (una alla volta, aspettando ogni risposta prima della
// successiva) apposta: l'obiettivo è misurare il tempo di risposta di ogni
// singola RPC, non il throughput sotto carico concorrente — sono due
// esperimenti diversi, e la spec chiede il primo ("impatto sui tempi di
// risposta RPC").
//
// Due meccanismi per combattere il rumore di sistema (CPU condivisa tra i
// processi del cluster e il resto della macchina):
//   - warmup: alcune richieste "usa e getta" scartate dalla misura, per far
//     stabilizzare le connessioni gRPC (che si aprono pigramente alla prima
//     chiamata) prima di iniziare a cronometrare
//   - repeat: l'intero ciclo Put+Get viene ripetuto più volte in momenti
//     diversi, e tutti i campioni vengono aggregati insieme — media su più
//     tentativi indipendenti, non solo su tante richieste consecutive
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// sample è la misura di una singola RPC: da quale ripetizione, quale
// operazione, quale tentativo, quanto ci ha messo.
type sample struct {
	run       int
	op        string
	attempt   int
	latencyMs float64
}

func main() {
	addr := flag.String("addr", "", "indirizzo del proxy da testare (obbligatorio)")
	n := flag.Int("n", 100, "numero di operazioni da eseguire per tipo (Put e Get), per ogni ripetizione")
	nodes := flag.Int("nodes", 0, "numero di consensus node del cluster testato (solo per etichettare i risultati)")
	out := flag.String("out", "", "file CSV di output (obbligatorio)")
	timeout := flag.Duration("timeout", 5*time.Second, "timeout per singola richiesta")
	keyPrefix := flag.String("key-prefix", "bench", "prefisso per le chiavi usate nel benchmark")
	warmup := flag.Int("warmup", 5, "richieste di riscaldamento (scartate) prima di ogni ripetizione")
	repeat := flag.Int("repeat", 1, "numero di ripetizioni indipendenti dell'intero ciclo, aggregate insieme")
	flag.Parse()

	if *addr == "" || *out == "" {
		log.Fatal("sono obbligatori -addr e -out")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connessione a %s: %v", *addr, err)
	}
	defer conn.Close()
	client := kvstorepb.NewKVStoreServiceClient(conn)

	fmt.Printf("Benchmark: %d ripetizioni x (%d Put + %d Get), %d di riscaldamento ciascuna, contro %s (cluster: %d nodi)\n",
		*repeat, *n, *n, *warmup, *addr, *nodes)

	var allResults []sample
	for run := 0; run < *repeat; run++ {
		doWarmup(client, *warmup, *timeout, *keyPrefix, run)
		runResults := runOnce(client, *n, *timeout, *keyPrefix, run)
		allResults = append(allResults, runResults...)
		fmt.Printf("  ripetizione %d/%d completata\n", run+1, *repeat)
	}

	if err := writeCSV(*out, *nodes, allResults); err != nil {
		log.Fatalf("scrittura CSV %s: %v", *out, err)
	}

	fmt.Println("--- Riepilogo aggregato su tutte le ripetizioni ---")
	printSummary("PUT", latenciesOf(allResults, "put"))
	printSummary("GET", latenciesOf(allResults, "get"))
	fmt.Printf("Risultati completi (%d righe) salvati in %s\n", len(allResults), *out)
}

// doWarmup manda richieste Put/Get "usa e getta" verso chiavi dedicate,
// scartando ogni risultato: serve solo a far aprire e stabilizzare le
// connessioni gRPC (client->proxy->Leader->Follower) prima della misura
// vera, così i primi campioni cronometrati non pagano il costo extra di
// una connessione nuova.
func doWarmup(client kvstorepb.KVStoreServiceClient, warmupN int, timeout time.Duration, keyPrefix string, run int) {
	for i := 0; i < warmupN; i++ {
		key := fmt.Sprintf("%s-warmup-%d-%d", keyPrefix, run, i)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, _ = client.Put(ctx, &kvstorepb.PutRequest{Key: key, Value: "v"})
		cancel()

		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		_, _ = client.Get(ctx, &kvstorepb.GetRequest{Key: key})
		cancel()
	}
}

// runOnce esegue un singolo ciclo cronometrato: n Put su chiavi nuove,
// seguiti da n Get che rileggono le stesse chiavi appena scritte.
func runOnce(client kvstorepb.KVStoreServiceClient, n int, timeout time.Duration, keyPrefix string, run int) []sample {
	var results []sample
	keys := make([]string, n)

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s-%d-%d", keyPrefix, run, i)
		keys[i] = key

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		_, err := client.Put(ctx, &kvstorepb.PutRequest{Key: key, Value: "v"})
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			log.Printf("Put run=%d #%d fallita: %v", run, i, err)
			continue
		}
		results = append(results, sample{run: run, op: "put", attempt: i, latencyMs: msOf(elapsed)})
	}

	for i, key := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		_, err := client.Get(ctx, &kvstorepb.GetRequest{Key: key})
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			log.Printf("Get run=%d #%d fallita: %v", run, i, err)
			continue
		}
		results = append(results, sample{run: run, op: "get", attempt: i, latencyMs: msOf(elapsed)})
	}

	return results
}

func msOf(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func latenciesOf(results []sample, op string) []float64 {
	out := make([]float64, 0, len(results))
	for _, r := range results {
		if r.op == op {
			out = append(out, r.latencyMs)
		}
	}
	sort.Float64s(out)
	return out
}

func writeCSV(path string, nodes int, results []sample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write([]string{"nodes", "run", "operation", "attempt", "latency_ms"}); err != nil {
		return err
	}
	for _, r := range results {
		row := []string{
			strconv.Itoa(nodes),
			strconv.Itoa(r.run),
			r.op,
			strconv.Itoa(r.attempt),
			fmt.Sprintf("%.3f", r.latencyMs),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// printSummary stampa media, minimo, massimo e percentili 50/95 — utile
// per un riscontro immediato a schermo, prima ancora di analizzare il CSV.
func printSummary(label string, sortedLatencies []float64) {
	if len(sortedLatencies) == 0 {
		fmt.Printf("%s: nessun campione riuscito\n", label)
		return
	}

	var sum float64
	for _, v := range sortedLatencies {
		sum += v
	}
	mean := sum / float64(len(sortedLatencies))

	fmt.Printf("%s: n=%d  media=%.2fms  min=%.2fms  p50=%.2fms  p95=%.2fms  max=%.2fms\n",
		label,
		len(sortedLatencies),
		mean,
		sortedLatencies[0],
		percentile(sortedLatencies, 0.50),
		percentile(sortedLatencies, 0.95),
		sortedLatencies[len(sortedLatencies)-1],
	)
}

// percentile assume che sorted sia già ordinato in modo crescente.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}
