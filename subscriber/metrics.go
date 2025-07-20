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

func startMetricsLogger() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		proc, _ := process.NewProcess(int32(os.Getpid()))
		proc.CPUPercent() // warm-up call

		for range ticker.C {
			numConns := getActiveClients()

			cpuPercent, err := proc.CPUPercent()
			if err != nil {
				cpuPercent = 0
			}

			fmt.Printf("Conns: %d, Goroutines: %d, CPU: %.2f%%\n",
				numConns, runtime.NumGoroutine(), cpuPercent)
		}
	}()
}
