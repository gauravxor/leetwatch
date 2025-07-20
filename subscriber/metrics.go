package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/process"
)

func getActiveClients() int64 {
	var numConns int64 = 0
	activeClients.Range(func(_, _ any) bool {
		numConns++
		return true
	})
	return numConns
}

func startMetricsLogger(tag string) {
	go func() {
		f, err := os.Create(fmt.Sprintf("%s_metrics.csv", tag))
		if err != nil {
			panic(err)
		}
		defer f.Close()

		fmt.Fprintln(f, "Timestamp,Connections,MemMB,CPUPercent")

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		proc, _ := process.NewProcess(int32(os.Getpid()))

		proc.CPUPercent()

		for range ticker.C {
			memInfo, err := proc.MemoryInfo()
			var memMB float64
			if err == nil {
				memMB = float64(memInfo.RSS) / 1024 / 1024 // RSS in MB
			}

			numConns := getActiveClients()

			cpuPercent, err := proc.CPUPercent()
			if err != nil {
				cpuPercent = 0
			}
			fmt.Printf("Conns: %d, Goroutines: %d, CPU: %.2f%%\n",
				numConns, runtime.NumGoroutine(), cpuPercent)

			fmt.Fprintf(f, "%s,%d,%.2f,%.2f\n",
				time.Now().Format(time.RFC3339),
				numConns,
				memMB,
				cpuPercent,
			)
		}
	}()
}
