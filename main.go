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
var dockerStats *exec.Cmd
var sar *exec.Cmd
var containerFile *os.File
var hostFile *os.File
var resultDir string

func startMeasurement(w http.ResponseWriter, r *http.Request) {
	if dockerStats != nil {
		http.Error(w, "measurement already running", 400)
		return
	}

	container := r.URL.Query().Get("container")
	if container != "kafka" && container != "nats" {
		http.Error(w, "good try", 400)
		return
	}

	startTime := time.Now()
	resultDir = filepath.Join("measurements", container+"-"+startTime.Format("20060102-150405"))
	os.MkdirAll(resultDir, 0755)

	containerFile, _ = os.Create(filepath.Join(resultDir, "container.csv"))
	hostFile, _ = os.Create(filepath.Join(resultDir, "host.txt"))

	fmt.Fprintln(containerFile, "timestamp,name,cpu,mem_usage,mem_percent,net_io,block_io,pids")
	fmt.Fprintf(hostFile, "measurement_started=%s\n\n", startTime.Format(time.RFC3339Nano))

	// Take one docker stats sample every second and add our own timestamp.
	// The container name is safe because we only accept "kafka" or "nats" above.

	// Start continuous Docker stats stream.
	var err error

	dockerStats, err = startDockerStats(container, containerFile)
	if err != nil {
		cleanup()
		http.Error(w, err.Error(), 500)
		return
	}

	// Measure CPU, RAM, network and disk of the complete server every second.
	sar = exec.Command("sar", "-u", "-r", "-d", "1")
	sar.Stdout = hostFile
	sar.Stderr = hostFile

	if err := sar.Start(); err != nil {
		_ = dockerStats.Process.Kill()
		_ = dockerStats.Wait()
		cleanup()
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(w, "started %s measurement\nstart: %s\nresults: %s\n",
		container, startTime.Format(time.RFC3339Nano), resultDir)
}

func stopMeasurement(w http.ResponseWriter, r *http.Request) {
	if dockerStats == nil {
		http.Error(w, "no measurement running", 400)
		return
	}

	stopTime := time.Now()

	_ = dockerStats.Process.Kill()
	_ = sar.Process.Kill()
	_ = dockerStats.Wait()
	_ = sar.Wait()

	fmt.Fprintf(hostFile, "\nmeasurement_stopped=%s\n", stopTime.Format(time.RFC3339Nano))

	dir := resultDir
	cleanup()

	fmt.Fprintf(w, "measurement stopped\nstop: %s\nresults: %s\n",
		stopTime.Format(time.RFC3339Nano), dir)
}

func cleanup() {
	if containerFile != nil {
		containerFile.Close()
	}
	if hostFile != nil {
		hostFile.Close()
	}

	dockerStats = nil
	sar = nil
	containerFile = nil
	hostFile = nil
}

func startDockerStats(container string, outputFile *os.File) (*exec.Cmd, error) {
	cmd := exec.Command(
		"docker",
		"stats",
		"--format",
		"{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}",
		container,
	)

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
