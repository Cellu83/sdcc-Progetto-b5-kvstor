// fault-tolerance orchestra l'esperimento di fault-tolerance della Fase 10:
// avvia un cluster di N nodi, uccide il Leader corrente (kill -9, per
// simulare un crash reale e non uno shutdown pulito), e misura due tempi
// leggendo direttamente i timestamp (a precisione di microsecondo) che i
// nodi già scrivono nei loro log:
//
//   - tempo di rielezione: dal crash al momento in cui un nuovo nodo si
//     dichiara "eletto Leader" per un term successivo a quello del Leader
//     caduto;
//   - tempo di convergenza: dal crash al momento in cui l'ultimo dei nodi
//     superstiti riconosce quel nuovo Leader per quel term — cioè quando
//     l'intero cluster rimasto è di nuovo d'accordo su chi comanda.
//
// La scelta di leggere i timestamp dai log (anziché istrumentare il codice
// con contatori ad hoc) è deliberata: usa un segnale che il sistema produce
// comunque nel suo funzionamento normale, senza toccare internal/raft.
//
// Ogni trial riparte da zero (cluster fresco, data dir pulita) per essere
// indipendente dagli altri, sullo stesso principio già usato in bench-client
// per l'esperimento di scalabilità.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	kvstorepb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/kvstore"
	raftpb "github.com/Cellu83/sdcc-Progetto-b5-kvstor/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const logTimeLayout = "2006/01/02 15:04:05.000000"

var (
	electedRe    = regexp.MustCompile(`^(\S+ \S+) \[(\S+)\] eletto Leader per il term (\d+)`)
	recognizedRe = regexp.MustCompile(`^(\S+ \S+) \[(\S+)\] riconosciuto leader (\S+) per il term (\d+)`)
)

type nodeProc struct {
	id      string
	addr    string
	cmd     *exec.Cmd
	logPath string
}

type trialResult struct {
	trial           int
	nodes           int
	crashedLeader   string
	newLeader       string
	preCrashTerm    uint64
	newTerm         uint64
	reelectionMs    float64
	convergenceMs   float64
	convergedCount  int
	expectedCount   int
	serviceRestored bool
}

func main() {
	n := flag.Int("n", 5, "numero di consensus node nel cluster")
	trials := flag.Int("trials", 5, "numero di trial indipendenti (crash+ripristino) da eseguire")
	basePort := flag.Int("base-port", 57000, "porta base: il proxy usa base-port, i nodi base-port+1..+n")
	binDir := flag.String("bin-dir", "bin", "cartella con i binari gia compilati (consensus-node, proxy, gen-cluster-config)")
	configDirBase := flag.String("config-dir", "configs/scale/ft", "cartella dove generare le config del cluster")
	dataDirBase := flag.String("data-dir-base", "data/ft", "cartella base per i data_dir dei nodi (una sottocartella per trial)")
	logDirBase := flag.String("log-dir-base", "/tmp/ft-logs", "cartella base per i log dei nodi (una sottocartella per trial)")
	out := flag.String("out", "experiments/results/fault-tolerance.csv", "file CSV di output")
	settleWait := flag.Duration("settle-wait", 3*time.Second, "attesa per la convergenza iniziale del cluster, prima del crash")
	measureWait := flag.Duration("measure-wait", 5*time.Second, "finestra di osservazione dopo il crash, per catturare rielezione e convergenza")
	flag.Parse()

	if *n < 3 {
		log.Fatal("-n deve essere >= 3 (serve una maggioranza non banale)")
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		log.Fatalf("creazione cartella di output %q: %v", filepath.Dir(*out), err)
	}

	var results []trialResult
	for t := 1; t <= *trials; t++ {
		fmt.Printf("--- Trial %d/%d ---\n", t, *trials)
		r, err := runTrial(t, *n, *basePort, *binDir, *configDirBase, *dataDirBase, *logDirBase, *settleWait, *measureWait)
		if err != nil {
			log.Fatalf("trial %d fallito: %v", t, err)
		}
		results = append(results, r)
		fmt.Printf("  leader caduto=%s (term %d) -> nuovo leader=%s (term %d): rielezione=%.1fms convergenza=%.1fms (%d/%d superstiti) servizio_ripristinato=%t\n",
			r.crashedLeader, r.preCrashTerm, r.newLeader, r.newTerm, r.reelectionMs, r.convergenceMs, r.convergedCount, r.expectedCount, r.serviceRestored)
	}

	if err := writeCSV(*out, results); err != nil {
		log.Fatalf("scrittura CSV %s: %v", *out, err)
	}

	printSummary(results)
	fmt.Printf("Risultati completi salvati in %s\n", *out)
}

func runTrial(trial, n, basePort int, binDir, configDirBase, dataDirBase, logDirBase string, settleWait, measureWait time.Duration) (trialResult, error) {
	configDir := filepath.Join(configDirBase, fmt.Sprintf("n%d", n))
	dataDir := filepath.Join(dataDirBase, fmt.Sprintf("n%d", n), fmt.Sprintf("trial%d", trial))
	logDir := filepath.Join(logDirBase, fmt.Sprintf("n%d", n), fmt.Sprintf("trial%d", trial))

	if err := os.RemoveAll(dataDir); err != nil {
		return trialResult{}, fmt.Errorf("pulizia data dir: %w", err)
	}
	if err := os.RemoveAll(logDir); err != nil {
		return trialResult{}, fmt.Errorf("pulizia log dir: %w", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return trialResult{}, fmt.Errorf("creazione log dir: %w", err)
	}

	genCmd := exec.Command(filepath.Join(binDir, "gen-cluster-config"),
		"-n", strconv.Itoa(n),
		"-base-port", strconv.Itoa(basePort),
		"-out-dir", configDir,
		"-data-dir", dataDir,
	)
	genCmd.Stdout = os.Stdout
	genCmd.Stderr = os.Stderr
	if err := genCmd.Run(); err != nil {
		return trialResult{}, fmt.Errorf("gen-cluster-config: %w", err)
	}

	var nodes []nodeProc
	cleanup := func() {
		for _, np := range nodes {
			if np.cmd.Process != nil {
				_ = np.cmd.Process.Kill()
				_, _ = np.cmd.Process.Wait()
			}
		}
	}
	defer cleanup()

	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("node%d", i)
		logPath := filepath.Join(logDir, id+".log")
		f, err := os.Create(logPath)
		if err != nil {
			return trialResult{}, fmt.Errorf("creazione log %s: %w", logPath, err)
		}
		defer f.Close()

		cmd := exec.Command(filepath.Join(binDir, "consensus-node"), "-config", filepath.Join(configDir, id+".yaml"))
		cmd.Stdout = f
		cmd.Stderr = f
		if err := cmd.Start(); err != nil {
			return trialResult{}, fmt.Errorf("avvio %s: %w", id, err)
		}
		nodes = append(nodes, nodeProc{id: id, addr: fmt.Sprintf("localhost:%d", basePort+i), cmd: cmd, logPath: logPath})
	}

	proxyLogPath := filepath.Join(logDir, "proxy.log")
	proxyLogFile, err := os.Create(proxyLogPath)
	if err != nil {
		return trialResult{}, fmt.Errorf("creazione log proxy: %w", err)
	}
	defer proxyLogFile.Close()
	proxyCmd := exec.Command(filepath.Join(binDir, "proxy"), "-config", filepath.Join(configDir, "proxy.yaml"))
	proxyCmd.Stdout = proxyLogFile
	proxyCmd.Stderr = proxyLogFile
	if err := proxyCmd.Start(); err != nil {
		return trialResult{}, fmt.Errorf("avvio proxy: %w", err)
	}
	defer func() {
		if proxyCmd.Process != nil {
			_ = proxyCmd.Process.Kill()
			_, _ = proxyCmd.Process.Wait()
		}
	}()

	time.Sleep(settleWait)

	proxyAddr := fmt.Sprintf("localhost:%d", basePort)

	leaderID, err := discoverLeader(nodes)
	if err != nil {
		return trialResult{}, fmt.Errorf("scoperta del leader iniziale: %w", err)
	}

	var leaderLogPath string
	for _, np := range nodes {
		if np.id == leaderID {
			leaderLogPath = np.logPath
		}
	}

	preCrashTerm, err := lastElectedTerm(leaderLogPath)
	if err != nil {
		return trialResult{}, fmt.Errorf("lettura term pre-crash del leader %s: %w", leaderID, err)
	}

	// scrittura di controllo prima del crash: cosi la verifica dopo il
	// ripristino accerta che il nuovo leader serva davvero il cluster, non
	// solo che risponda
	if err := putViaProxy(proxyAddr, fmt.Sprintf("ft-trial-%d-before", trial), "before-crash"); err != nil {
		return trialResult{}, fmt.Errorf("scrittura pre-crash via proxy: %w", err)
	}

	var crashedCmd *exec.Cmd
	for _, np := range nodes {
		if np.id == leaderID {
			crashedCmd = np.cmd
		}
	}
	if crashedCmd == nil {
		return trialResult{}, fmt.Errorf("nodo leader %s non trovato tra i processi avviati", leaderID)
	}

	crashTime := time.Now()
	if err := crashedCmd.Process.Kill(); err != nil {
		return trialResult{}, fmt.Errorf("crash del leader %s: %w", leaderID, err)
	}
	_, _ = crashedCmd.Process.Wait()

	time.Sleep(measureWait)

	survivors := make([]nodeProc, 0, n-1)
	for _, np := range nodes {
		if np.id != leaderID {
			survivors = append(survivors, np)
		}
	}

	newLeader, newTerm, reelectionTime, err := findReelection(survivors, preCrashTerm)
	if err != nil {
		return trialResult{}, fmt.Errorf("nessuna rielezione osservata entro la finestra di misura: %w", err)
	}

	convergenceTime, convergedCount := findConvergence(survivors, newLeader, newTerm)
	expectedCount := len(survivors) - 1 // i superstiti tranne il nuovo leader stesso, che non "riconosce" se stesso

	serviceRestored := putViaProxy(proxyAddr, fmt.Sprintf("ft-trial-%d-after", trial), "after-crash") == nil

	return trialResult{
		trial:           trial,
		nodes:           n,
		crashedLeader:   leaderID,
		newLeader:       newLeader,
		preCrashTerm:    preCrashTerm,
		newTerm:         newTerm,
		reelectionMs:    reelectionTime.Sub(crashTime).Seconds() * 1000,
		convergenceMs:   convergenceTime.Sub(crashTime).Seconds() * 1000,
		convergedCount:  convergedCount,
		expectedCount:   expectedCount,
		serviceRestored: serviceRestored,
	}, nil
}

// discoverLeader interroga i nodi via GetStatus finche uno risponde con un
// leader_id non vuoto — e' il modo "diretto" (via RPC, non via log) di
// scoprire lo stato iniziale, riservando il parsing dei log alla sola
// misura temporale post-crash che e' l'oggetto dell'esperimento.
func discoverLeader(nodes []nodeProc) (string, error) {
	for attempt := 0; attempt < 10; attempt++ {
		for _, np := range nodes {
			leaderID, err := queryLeader(np.addr)
			if err == nil && leaderID != "" {
				return leaderID, nil
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return "", fmt.Errorf("nessun nodo riporta un leader dopo diversi tentativi")
}

func queryLeader(addr string) (string, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	client := raftpb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.GetStatus(ctx, &raftpb.GetStatusRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetLeaderId(), nil
}

// lastElectedTerm legge il log del leader e restituisce il term della sua
// ULTIMA autoproclamazione "eletto Leader" — il term sotto cui stava
// operando nel momento in cui e' stato ucciso.
func lastElectedTerm(logPath string) (uint64, error) {
	lines, err := readLines(logPath)
	if err != nil {
		return 0, err
	}
	var lastTerm uint64
	found := false
	for _, line := range lines {
		m := electedRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		term, err := strconv.ParseUint(m[3], 10, 64)
		if err != nil {
			continue
		}
		lastTerm = term
		found = true
	}
	if !found {
		return 0, fmt.Errorf("nessuna riga \"eletto Leader\" trovata in %s", logPath)
	}
	return lastTerm, nil
}

// findReelection cerca, tra i log dei superstiti, la prima autoproclamazione
// "eletto Leader" per un term successivo a preCrashTerm — il nuovo Leader
// legittimo emerso dopo il crash. Se piu' nodi hanno provato l'elezione
// (split vote) prima di uno vincitore, questo prende comunque il primo che
// e' *riuscito* a dichiararsi Leader.
func findReelection(survivors []nodeProc, preCrashTerm uint64) (string, uint64, time.Time, error) {
	type candidate struct {
		nodeID string
		term   uint64
		ts     time.Time
	}
	var best *candidate

	for _, np := range survivors {
		lines, err := readLines(np.logPath)
		if err != nil {
			continue
		}
		for _, line := range lines {
			m := electedRe.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			term, err := strconv.ParseUint(m[3], 10, 64)
			if err != nil || term <= preCrashTerm {
				continue
			}
			ts, err := time.ParseInLocation(logTimeLayout, m[1], time.Local)
			if err != nil {
				continue
			}
			if best == nil || ts.Before(best.ts) {
				best = &candidate{nodeID: m[2], term: term, ts: ts}
			}
		}
	}
	if best == nil {
		return "", 0, time.Time{}, fmt.Errorf("nessun nodo si e' dichiarato Leader per un term > %d", preCrashTerm)
	}
	return best.nodeID, best.term, best.ts, nil
}

// findConvergence cerca, tra i log dei superstiti (esclude il nuovo leader,
// che non "riconosce" se stesso), la prima riga "riconosciuto leader
// <newLeader> per il term <newTerm>" di ciascuno, e restituisce l'istante
// del PIU' TARDIVO tra questi — il momento in cui anche l'ultimo nodo del
// cluster rimasto si e' allineato sul nuovo Leader.
func findConvergence(survivors []nodeProc, newLeader string, newTerm uint64) (time.Time, int) {
	var latest time.Time
	converged := 0

	for _, np := range survivors {
		if np.id == newLeader {
			continue
		}
		lines, err := readLines(np.logPath)
		if err != nil {
			continue
		}
		for _, line := range lines {
			m := recognizedRe.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			recognizedLeader := m[3]
			term, err := strconv.ParseUint(m[4], 10, 64)
			if err != nil || recognizedLeader != newLeader || term != newTerm {
				continue
			}
			ts, err := time.ParseInLocation(logTimeLayout, m[1], time.Local)
			if err != nil {
				continue
			}
			converged++
			if ts.After(latest) {
				latest = ts
			}
			break // prima occorrenza per questo nodo, basta
		}
	}
	return latest, converged
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func putViaProxy(addr, key, value string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := kvstorepb.NewKVStoreServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, &kvstorepb.PutRequest{Key: key, Value: value})
	return err
}

func writeCSV(path string, results []trialResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintln(f, "trial,nodes,crashed_leader,new_leader,pre_crash_term,new_term,reelection_ms,convergence_ms,converged_count,expected_count,service_restored")
	for _, r := range results {
		fmt.Fprintf(f, "%d,%d,%s,%s,%d,%d,%.3f,%.3f,%d,%d,%t\n",
			r.trial, r.nodes, r.crashedLeader, r.newLeader, r.preCrashTerm, r.newTerm,
			r.reelectionMs, r.convergenceMs, r.convergedCount, r.expectedCount, r.serviceRestored)
	}
	return nil
}

func printSummary(results []trialResult) {
	if len(results) == 0 {
		return
	}
	var reelect, converge []float64
	for _, r := range results {
		reelect = append(reelect, r.reelectionMs)
		converge = append(converge, r.convergenceMs)
	}
	sort.Float64s(reelect)
	sort.Float64s(converge)

	fmt.Println("--- Riepilogo ---")
	fmt.Printf("Rielezione:   n=%d media=%.1fms min=%.1fms max=%.1fms\n", len(reelect), mean(reelect), reelect[0], reelect[len(reelect)-1])
	fmt.Printf("Convergenza:  n=%d media=%.1fms min=%.1fms max=%.1fms\n", len(converge), mean(converge), converge[0], converge[len(converge)-1])
}

func mean(v []float64) float64 {
	var sum float64
	for _, x := range v {
		sum += x
	}
	return sum / float64(len(v))
}
