package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// We only allow one measurement at a time.

var kafkaStats *exec.Cmd
var natsStats *exec.Cmd
var sar *exec.Cmd

var kafkaFile *os.File
var natsFile *os.File
var hostFile *os.File

var resultDir string

func startMeasurement(w http.ResponseWriter, r *http.Request) {
	if kafkaStats != nil || natsStats != nil {
		http.Error(w, "measurement already running", 400)
		return
	}

	container := r.URL.Query().Get("container")

	if container != "kafka" &&
		container != "nats" &&
		container != "both" {

		http.Error(w, "container must be kafka, nats or both", 400)
		return
	}

	startTime := time.Now()

	resultDir = filepath.Join(
		"measurements",
		container+"-"+startTime.Format("20060102-150405"),
	)

	if err := os.MkdirAll(resultDir, 0755); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	var err error

	// -------------------------
	// Kafka measurement
	// -------------------------

	if container == "kafka" || container == "both" {
		kafkaFile, err = os.Create(
			filepath.Join(resultDir, "kafka.csv"),
		)
		if err != nil {
			cleanup()
			http.Error(w, err.Error(), 500)
			return
		}

		fmt.Fprintln(
			kafkaFile,
			"timestamp,name,cpu,mem_usage,mem_percent,net_io,block_io,pids",
		)

		kafkaStats, err = startDockerStats(
			"kafka",
			kafkaFile,
		)
		if err != nil {
			cleanup()
			http.Error(w, err.Error(), 500)
			return
		}
	}

	// -------------------------
	// NATS measurement
	// -------------------------

	if container == "nats" || container == "both" {
		natsFile, err = os.Create(
			filepath.Join(resultDir, "nats.csv"),
		)
		if err != nil {
			cleanup()
			http.Error(w, err.Error(), 500)
			return
		}

		fmt.Fprintln(
			natsFile,
			"timestamp,name,cpu,mem_usage,mem_percent,net_io,block_io,pids",
		)

		natsStats, err = startDockerStats(
			"nats",
			natsFile,
		)
		if err != nil {
			cleanup()
			http.Error(w, err.Error(), 500)
			return
		}
	}

	// -------------------------
	// Host measurement
	// -------------------------

	hostFile, err = os.Create(
		filepath.Join(resultDir, "host.txt"),
	)
	if err != nil {
		cleanup()
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(
		hostFile,
		"measurement_started=%s\n\n",
		startTime.Format(time.RFC3339Nano),
	)

	sar = exec.Command(
		"sar",
		"-u",
		"-r",
		"-d",
		"1",
	)

	sar.Stdout = hostFile
	sar.Stderr = hostFile

	if err := sar.Start(); err != nil {
		cleanup()
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(
		w,
		"started %s measurement\nstart: %s\nresults: %s\n",
		container,
		startTime.Format(time.RFC3339Nano),
		resultDir,
	)
}

func stopMeasurement(w http.ResponseWriter, r *http.Request) {
	if kafkaStats == nil && natsStats == nil {
		http.Error(w, "no measurement running", 400)
		return
	}

	stopTime := time.Now()

	if kafkaStats != nil {
		_ = kafkaStats.Process.Kill()
		_ = kafkaStats.Wait()
	}

	if natsStats != nil {
		_ = natsStats.Process.Kill()
		_ = natsStats.Wait()
	}

	if sar != nil {
		_ = sar.Process.Kill()
		_ = sar.Wait()
	}

	if hostFile != nil {
		fmt.Fprintf(
			hostFile,
			"\nmeasurement_stopped=%s\n",
			stopTime.Format(time.RFC3339Nano),
		)
	}

	dir := resultDir

	cleanup()

	fmt.Fprintf(
		w,
		"measurement stopped\nstop: %s\nresults: %s\n",
		stopTime.Format(time.RFC3339Nano),
		dir,
	)
}

func cleanup() {
	if kafkaFile != nil {
		kafkaFile.Close()
	}

	if natsFile != nil {
		natsFile.Close()
	}

	if hostFile != nil {
		hostFile.Close()
	}

	kafkaStats = nil
	natsStats = nil
	sar = nil

	kafkaFile = nil
	natsFile = nil
	hostFile = nil

	resultDir = ""
}

func startDockerStats(container string, outputFile *os.File) (*exec.Cmd, error) {
	dockerScript := fmt.Sprintf(`
while true; do
	docker stats --no-stream \
		--format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' \
		%s
		sleep 0.3
done
`, container)

	cmd := exec.Command("bash", "-c", dockerScript)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	cmd.Stderr = outputFile

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	go func() {
		scanner := bufio.NewScanner(stdout)

		for scanner.Scan() {
			timestamp := time.Now().Format(time.RFC3339Nano)

			fmt.Fprintf(
				outputFile,
				"%s,%s\n",
				timestamp,
				scanner.Text(),
			)
		}

		if err := scanner.Err(); err != nil {
			fmt.Println("docker stats error:", err)
		}
	}()

	return cmd, nil
}

func main() {
	os.MkdirAll("measurements", 0755)

	http.HandleFunc("/start", startMeasurement)
	http.HandleFunc("/stop", stopMeasurement)

	fmt.Println("Measurement API: http://127.0.0.1:7070")
	fmt.Println("Kafka: curl 'http://127.0.0.1:7070/start?container=kafka'")
	fmt.Println("NATS:  curl 'http://127.0.0.1:7070/start?container=nats'")
	fmt.Println("Stop:  curl 'http://127.0.0.1:7070/stop'")

	http.ListenAndServe("94.130.136.176:7070", nil)
}
