package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	yuniKornURL string
	scrapeInterval time.Duration
	once sync.Once
)

func initConfig() {
	once.Do(func() {
		yuniKornURL = os.Getenv("YUNIKORN_SERVICE_URL")
		if yuniKornURL == "" {
			yuniKornURL = "http://localhost:9080"
		}

		interval := os.Getenv("SCRAPE_INTERVAL")
		if interval == "" {
			scrapeInterval = 15 * time.Second
		} else {
			var err error
			scrapeInterval, err = time.ParseDuration(interval)
			if err != nil {
				log.Printf("Invalid SCRAPE_INTERVAL, using default 15s: %v", err)
				scrapeInterval = 15 * time.Second
			}
		}
	})
}

var (
	// Metric: Application information with all details
	appMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_app",
			Help: "Application information with queue, state, resources, and wait time. Value is 1.",
		},
		[]string{"queue", "state", "app_id", "cpu", "memory_bytes", "executors", "wait_seconds"},
	)

	scrapeErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "yunikorn_exporter_scrape_errors_total",
			Help: "Total number of scrape errors",
		},
		[]string{"endpoint"},
	)
)

// YuniKorn API structures
type YuniKornApp struct {
	ApplicationID     string                 `json:"applicationID"`
	QueueName         string                 `json:"queueName"`
	Partition         string                 `json:"partition"`
	ApplicationState  string                 `json:"applicationState"`
	SubmissionTime    int64                  `json:"submissionTime"`
	StartTime         int64                  `json:"startTime"`
	UsedResource      YuniKornResources      `json:"usedResource"`
	AllocatedResource YuniKornResources      `json:"allocatedResource"`
}

type YuniKornResources struct {
	VCore int64 `json:"vcore"`
	Memory int64 `json:"memory"`
	Pods  int64 `json:"pods"`
}

func init() {
	initConfig()
	prometheus.MustRegister(appMetric)
	prometheus.MustRegister(scrapeErrors)

	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9300"
	}

	partition := os.Getenv("YUNIKORN_PARTITION")
	if partition == "" {
		partition = "default"
	}

	// Log startup
	log.Printf("Starting YuniKorn App Exporter on port %s", port)
	log.Printf("YuniKorn URL: %s", yuniKornURL)
	log.Printf("Partition: %s", partition)
	log.Printf("Scrape Interval: %s", scrapeInterval)

	// Initial scrape
	log.Println("Performing initial metric scrape...")
	if err := scrapeMetrics(partition); err != nil {
		log.Printf("Initial scrape error: %v", err)
	} else {
		log.Println("Initial scrape successful")
	}

	// Background scraping
	go func() {
		ticker := time.NewTicker(scrapeInterval)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("Scraping metrics at %s", time.Now().Format(time.RFC3339))
			if err := scrapeMetrics(partition); err != nil {
				log.Printf("Scrape error: %v", err)
			}
		}
	}()

	// HTTP server
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "YuniKorn App Exporter")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Metrics: /metrics")
		fmt.Fprintf(w, "YuniKorn URL: %s\n", yuniKornURL)
		fmt.Fprintf(w, "Partition: %s\n", partition)
		fmt.Fprintf(w, "Scrape Interval: %s\n", scrapeInterval)
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Available Metrics:")
		fmt.Fprintln(w, "  yunikorn_app{queue, state, app_id, cpu, memory_bytes, executors, wait_seconds}")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Label descriptions:")
		fmt.Fprintln(w, "  queue        - Queue name")
		fmt.Fprintln(w, "  state        - Application state (Running, Accepted, etc)")
		fmt.Fprintln(w, "  app_id       - Application ID")
		fmt.Fprintln(w, "  cpu          - Allocated CPU cores in millicores (1000 = 1 core)")
		fmt.Fprintln(w, "  memory_bytes - Allocated memory in bytes")
		fmt.Fprintln(w, "  executors    - Number of pods/executors")
		fmt.Fprintln(w, "  wait_seconds - Time since submission in seconds")
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})

	log.Printf("Server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func scrapeMetrics(partition string) error {
	// Clear old metrics
	appMetric.Reset()

	// Fetch applications from /ws/v1/partition/default/applications/active
	appsURL := fmt.Sprintf("%s/ws/v1/partition/%s/applications/active", yuniKornURL, partition)
	log.Printf("Fetching applications from: %s", appsURL)

	resp, err := http.Get(appsURL)
	if err != nil {
		scrapeErrors.WithLabelValues("apps").Inc()
		return fmt.Errorf("failed to fetch applications: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		scrapeErrors.WithLabelValues("apps").Inc()
		return fmt.Errorf("applications API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		scrapeErrors.WithLabelValues("apps").Inc()
		return fmt.Errorf("failed to read response: %w", err)
	}

	var apps []YuniKornApp
	if err := json.Unmarshal(body, &apps); err != nil {
		scrapeErrors.WithLabelValues("apps").Inc()
		return fmt.Errorf("failed to parse applications: %w", err)
	}

	log.Printf("Found %d applications", len(apps))

	// Process each application
	for _, app := range apps {
		queue := app.QueueName
		appID := app.ApplicationID
		state := app.ApplicationState

		// Only track RUNNING and pending-like states
		if isTrackableState(state) {
			// Get allocated resources (use AllocatedResource, fallback to UsedResource)
			cpu := app.AllocatedResource.VCore
			memory := app.AllocatedResource.Memory
			executors := app.AllocatedResource.Pods

			if cpu == 0 && memory == 0 && executors == 0 {
				cpu = app.UsedResource.VCore
				memory = app.UsedResource.Memory
				executors = app.UsedResource.Pods
			}

			// Calculate wait time in seconds
			waitSeconds := float64(0)
			if app.SubmissionTime > 0 {
				waitSeconds = float64(time.Now().UnixNano()-app.SubmissionTime) / 1e9
				if waitSeconds < 0 {
					waitSeconds = 0
				}
			}

			// Convert to strings for labels
			cpuStr := fmt.Sprintf("%d", cpu)
			memoryStr := fmt.Sprintf("%d", memory)
			executorsStr := fmt.Sprintf("%d", executors)
			waitStr := fmt.Sprintf("%.0f", waitSeconds)

			// Set app metric (value is always 1)
			appMetric.WithLabelValues(queue, state, appID, cpuStr, memoryStr, executorsStr, waitStr).Set(1)

			log.Printf("App: %s, queue: %s, state: %s, cpu: %dm, memory: %d bytes, executors: %d, wait: %.0f seconds",
				appID, queue, state, cpu, memory, executors, waitSeconds)
		}
	}

	return nil
}

// isTrackableState checks if we should track this application state
func isTrackableState(state string) bool {
	trackableStates := map[string]bool{
		"New":       true,
		"Accepted":  true,
		"Waiting":   true,
		"Submitted": true,
		"Pending":   true,
		"Running":   true,
	}
	return trackableStates[state]
}
