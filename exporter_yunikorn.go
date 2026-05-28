package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// Default configuration values
	defaultYuniKornURL     = "http://localhost:9080"
	defaultPort            = "9300"
	defaultPartition       = "default"
	defaultScrapeInterval  = 15 * time.Second

	// Queue constants
	unassignedQueue = "unassigned"
	memoryResource  = "memory"
	cpuResource     = "cpu"

	// Time constants
	avgAppDuration      = 300.0 // 5 minutes
	maxPredictedWait    = 3600.0 // 1 hour
	assumedWaitPerApp   = 60.0   // 1 minute

	// Rejection message max length
	maxReasonLength = 100
)

var (
	yuniKornURL     string
	scrapeInterval  time.Duration
	scrapeIntervalM sync.RWMutex
	once            sync.Once
)

func initConfig() {
	once.Do(func() {
		yuniKornURL = getEnvOrDefault("YUNIKORN_SERVICE_URL", defaultYuniKornURL)
		scrapeInterval = parseScrapeInterval()
	})
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseScrapeInterval() time.Duration {
	interval := os.Getenv("SCRAPE_INTERVAL")
	if interval == "" {
		return defaultScrapeInterval
	}

	parsed, err := time.ParseDuration(interval)
	if err != nil {
		log.Printf("Invalid SCRAPE_INTERVAL '%s', using default %s: %v", interval, defaultScrapeInterval, err)
		return defaultScrapeInterval
	}
	return parsed
}

var (
	appMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_app",
			Help: "Application information with queue, state, and resources. Value is 1.",
		},
		[]string{"queue", "state", "app_id", "cpu", "memory_bytes", "executors"},
	)

	appWaitTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_app_wait_time_seconds",
			Help: "Time application spent waiting in queue before starting (submission to start time). For apps not yet started, shows current wait time.",
		},
		[]string{"queue", "state", "app_id"},
	)

	appResourceUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_app_resource_usage",
			Help: "Application resource usage in bytes/cores",
		},
		[]string{"queue", "state", "app_id", "resource_type"},
	)

	queueResourceUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_queue_resource_usage_ratio",
			Help: "Queue resource usage ratio (0.0 to 1.0) for memory and CPU",
		},
		[]string{"queue", "resource_type"},
	)

	queueCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_queue_capacity",
			Help: "Queue capacity information (max resources, running apps, etc)",
		},
		[]string{"queue", "metric_type"},
	)

	queueAppCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_queue_app_count",
			Help: "Number of applications in queue by state (Pending, Running, Accepted, etc)",
		},
		[]string{"queue", "state"},
	)

	rejectedAppCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_rejected_apps_total",
			Help: "Total number of rejected applications",
		},
		[]string{"queue", "reason"},
	)

	rejectedAppDetails = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_rejected_app_info",
			Help: "Rejected application information with timestamp, queue, and reason. Value is 1 for each rejected app.",
		},
		[]string{"app_id", "queue", "reason", "timestamp_seconds"},
	)

	queuePredictedWaitTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "yunikorn_queue_predicted_wait_seconds",
			Help: "Predicted wait time for new apps in queue (in seconds). Based on avg app duration and queue position.",
		},
		[]string{"queue"},
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
	ApplicationID     string           `json:"applicationID"`
	QueueName         string           `json:"queueName"`
	Partition         string           `json:"partition"`
	ApplicationState  string           `json:"applicationState"`
	SubmissionTime    int64            `json:"submissionTime"`
	StartTime         int64            `json:"startTime"`
	UsedResource      YuniKornResources `json:"usedResource"`
	AllocatedResource YuniKornResources `json:"allocatedResource"`
	RejectedMessage   string           `json:"rejectedMessage"`
}

type YuniKornResources struct {
	VCore int64 `json:"vcore"`
	Memory int64 `json:"memory"`
	Pods  int64 `json:"pods"`
}

type YuniKornQueue struct {
	QueueName            string           `json:"queuename"`
	ParentQueue          string           `json:"parentQueue"`
	IsLeaf               bool             `json:"isLeaf"`
	Status               string           `json:"status"`
	MaxResource          YuniKornResources `json:"maxResource"`
	UsedResource          YuniKornResources `json:"usedResource"`
	AllocatedResource    YuniKornResources `json:"allocatedResource"`
	MaxRunningApps       int              `json:"maxRunningApps"`
	MaxApps              int              `json:"maxApps"`
	RunningApps          int              `json:"runningApps"`
	AllocatedContainers  int              `json:"allocatedContainers"`
	Children             []YuniKornQueue   `json:"children"`
}

func init() {
	initConfig()
	registerMetrics()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func registerMetrics() {
	prometheus.MustRegister(
		appMetric,
		appWaitTime,
		appResourceUsage,
		queueResourceUsage,
		queueCapacity,
		queueAppCount,
		rejectedAppCount,
		rejectedAppDetails,
		queuePredictedWaitTime,
		scrapeErrors,
	)
}

func main() {
	port := getEnvOrDefault("PORT", defaultPort)
	partition := getEnvOrDefault("YUNIKORN_PARTITION", defaultPartition)

	logStartupInfo(port, partition)

	// Initial scrape
	if err := performInitialScrape(partition); err != nil {
		log.Printf("Initial scrape error: %v", err)
	}

	// Start background scraping
	startBackgroundScraping(partition)

	// Start HTTP server
	startHTTPServer(port)
}

func logStartupInfo(port, partition string) {
	log.Printf("Starting YuniKorn App Exporter")
	log.Printf("  Port: %s", port)
	log.Printf("  YuniKorn URL: %s", yuniKornURL)
	log.Printf("  Partition: %s", partition)
	log.Printf("  Scrape Interval: %s", scrapeInterval)
}

func performInitialScrape(partition string) error {
	log.Println("Performing initial metric scrape...")
	if err := scrapeMetrics(partition); err != nil {
		return err
	}
	log.Println("Initial scrape successful")
	return nil
}

func startBackgroundScraping(partition string) {
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
}

func startHTTPServer(port string) {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", rootHandler(port))
	http.HandleFunc("/healthz", healthzHandler)

	log.Printf("Server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func rootHandler(port string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "YuniKorn App Exporter")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Metrics: /metrics")
		fmt.Fprintln(w, "Health: /healthz")
		fmt.Fprintf(w, "YuniKorn URL: %s\n", yuniKornURL)
		fmt.Fprintf(w, "Port: %s\n", port)
		fmt.Fprintf(w, "Scrape Interval: %s\n", scrapeInterval)
		fmt.Fprintln(w, "")
		printAvailableMetrics(w)
	}
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "OK")
}

func printAvailableMetrics(w http.ResponseWriter) {
	metrics := []struct{
		name string
		labels string
	}{
		{"yunikorn_app", "queue, state, app_id, cpu, memory_bytes, executors"},
		{"yunikorn_app_wait_time_seconds", "queue, state, app_id"},
		{"yunikorn_app_resource_usage", "queue, state, app_id, resource_type"},
		{"yunikorn_queue_app_count", "queue, state"},
		{"yunikorn_queue_capacity", "queue, metric_type"},
		{"yunikorn_queue_resource_usage_ratio", "queue, resource_type"},
		{"yunikorn_queue_predicted_wait_seconds", "queue"},
		{"yunikorn_rejected_apps_total", "queue, reason"},
	}

	fmt.Fprintln(w, "Available Metrics:")
	for _, m := range metrics {
		fmt.Fprintf(w, "  %s{%s}\n", m.name, m.labels)
	}
}

func scrapeMetrics(partition string) error {
	resetAllMetrics()

	if err := scrapeApplications(partition); err != nil {
		return err
	}

	if err := scrapeRejectedApps(partition); err != nil {
		log.Printf("Warning: failed to scrape rejected apps: %v", err)
		scrapeErrors.WithLabelValues("rejected_apps").Inc()
	}

	if err := scrapeQueueMetrics(partition); err != nil {
		log.Printf("Warning: failed to scrape queue metrics: %v", err)
		scrapeErrors.WithLabelValues("queues").Inc()
	}

	return nil
}

func resetAllMetrics() {
	appMetric.Reset()
	appWaitTime.Reset()
	appResourceUsage.Reset()
	queueResourceUsage.Reset()
	queueCapacity.Reset()
	queueAppCount.Reset()
	rejectedAppCount.Reset()
	rejectedAppDetails.Reset()
	queuePredictedWaitTime.Reset()
}

func scrapeApplications(partition string) error {
	appsURL := fmt.Sprintf("%s/ws/v1/partition/%s/applications/active", yuniKornURL, partition)
	log.Printf("Fetching applications from: %s", appsURL)

	apps, err := fetchAPI[[]YuniKornApp](appsURL, "apps")
	if err != nil {
		return err
	}

	log.Printf("Found %d applications", len(apps))
	processApplications(apps)
	return nil
}

func fetchAPI[T any](url, endpoint string) (T, error) {
	var result T

	resp, err := http.Get(url)
	if err != nil {
		scrapeErrors.WithLabelValues(endpoint).Inc()
		return result, fmt.Errorf("failed to fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		scrapeErrors.WithLabelValues(endpoint).Inc()
		return result, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		scrapeErrors.WithLabelValues(endpoint).Inc()
		return result, fmt.Errorf("failed to read response: %w", err)
	}

	if err := json.Unmarshal(body, &result); err != nil {
		scrapeErrors.WithLabelValues(endpoint).Inc()
		return result, fmt.Errorf("failed to parse response: %w", err)
	}

	return result, nil
}

func processApplications(apps []YuniKornApp) {
	queueStateCounts := make(map[string]map[string]int)

	for _, app := range apps {
		processApplication(&app, queueStateCounts)
	}

	setQueueAppCountMetrics(queueStateCounts)
}

func processApplication(app *YuniKornApp, queueStateCounts map[string]map[string]int) {
	queue := app.QueueName
	appID := app.ApplicationID
	state := app.ApplicationState

	// Initialize queue state counter
	if _, exists := queueStateCounts[queue]; !exists {
		queueStateCounts[queue] = make(map[string]int)
	}
	queueStateCounts[queue][state]++

	if !isTrackableState(state) {
		return
	}

	// Get allocated resources
	cpu := app.AllocatedResource.VCore
	memory := app.AllocatedResource.Memory
	executors := app.AllocatedResource.Pods

	// Fallback to used resources if allocated is empty
	if cpu == 0 && memory == 0 && executors == 0 {
		cpu = app.UsedResource.VCore
		memory = app.UsedResource.Memory
		executors = app.UsedResource.Pods
	}

	// Set resource usage metrics
	appResourceUsage.WithLabelValues(queue, state, appID, "memory_bytes").Set(float64(memory))
	appResourceUsage.WithLabelValues(queue, state, appID, "cpu").Set(float64(cpu) / 1000.0)
	appResourceUsage.WithLabelValues(queue, state, appID, "pods").Set(float64(executors))

	// Calculate and set wait time
	waitSeconds := calculateWaitTime(app)
	appWaitTime.WithLabelValues(queue, state, appID).Set(waitSeconds)

	// Set app info metric
	cpuStr := fmt.Sprintf("%d", cpu)
	memoryStr := fmt.Sprintf("%d", memory)
	executorsStr := fmt.Sprintf("%d", executors)
	appMetric.WithLabelValues(queue, state, appID, cpuStr, memoryStr, executorsStr).Set(1)

	logApplicationInfo(appID, queue, state, cpu, memory, executors, waitSeconds)
}

func calculateWaitTime(app *YuniKornApp) float64 {
	if app.SubmissionTime <= 0 {
		return 0
	}

	var endTime int64
	if app.StartTime > 0 {
		endTime = app.StartTime
	} else {
		endTime = time.Now().UnixNano()
	}

	waitSeconds := float64(endTime - app.SubmissionTime) / 1e9
	if waitSeconds < 0 {
		waitSeconds = 0
	}
	return waitSeconds
}

func logApplicationInfo(appID, queue, state string, cpu, memory, executors int64, waitSeconds float64) {
	log.Printf("App: %s, queue: %s, state: %s, cpu: %dm, memory: %d bytes, executors: %d, wait: %.2f seconds",
		appID, queue, state, cpu, memory, executors, waitSeconds)
}

func setQueueAppCountMetrics(queueStateCounts map[string]map[string]int) {
	for queue, states := range queueStateCounts {
		for state, count := range states {
			queueAppCount.WithLabelValues(queue, state).Set(float64(count))
		}
	}
}

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

func scrapeQueueMetrics(partition string) error {
	queuesURL := fmt.Sprintf("%s/ws/v1/partition/%s/queues", yuniKornURL, partition)
	log.Printf("Fetching queues from: %s", queuesURL)

	rootQueue, err := fetchAPI[YuniKornQueue](queuesURL, "queues")
	if err != nil {
		return err
	}

	// Get active apps for wait time calculation
	appsURL := fmt.Sprintf("%s/ws/v1/partition/%s/applications/active", yuniKornURL, partition)
	apps, err := fetchAPI[[]YuniKornApp](appsURL, "apps")
	if err != nil {
		apps = []YuniKornApp{} // Continue with empty apps list
	}

	processQueueRecursive(&rootQueue, apps)
	return nil
}

func processQueueRecursive(queue *YuniKornQueue, apps []YuniKornApp) {
	if queue == nil {
		return
	}

	shouldProcess := queue.IsLeaf || queue.MaxResource.Memory > 0 || queue.MaxResource.VCore > 0

	if shouldProcess {
		setQueueMetrics(queue, apps)
	}

	// Process children recursively
	for i := range queue.Children {
		processQueueRecursive(&queue.Children[i], apps)
	}
}

func setQueueMetrics(queue *YuniKornQueue, apps []YuniKornApp) {
	// Calculate and set resource usage ratios
	if queue.MaxResource.Memory > 0 {
		memoryRatio := safeDivide(
			float64(queue.UsedResource.Memory),
			float64(queue.MaxResource.Memory),
		)
		queueResourceUsage.WithLabelValues(queue.QueueName, memoryResource).Set(memoryRatio)
	}

	if queue.MaxResource.VCore > 0 {
		cpuRatio := safeDivide(
			float64(queue.UsedResource.VCore),
			float64(queue.MaxResource.VCore),
		)
		queueResourceUsage.WithLabelValues(queue.QueueName, cpuResource).Set(cpuRatio)
	}

	// Set capacity metrics
	queueCapacity.WithLabelValues(queue.QueueName, "max_memory_bytes").Set(float64(queue.MaxResource.Memory))
	queueCapacity.WithLabelValues(queue.QueueName, "max_vcore").Set(float64(queue.MaxResource.VCore))
	queueCapacity.WithLabelValues(queue.QueueName, "used_memory_bytes").Set(float64(queue.UsedResource.Memory))
	queueCapacity.WithLabelValues(queue.QueueName, "used_vcore").Set(float64(queue.UsedResource.VCore))
	queueCapacity.WithLabelValues(queue.QueueName, "allocated_pods").Set(float64(queue.AllocatedContainers))

	// Calculate and set predicted wait time
	predictedWait := calculatePredictedWaitTime(queue, apps)
	queuePredictedWaitTime.WithLabelValues(queue.QueueName).Set(predictedWait)

	memoryPercent := safeDivide(float64(queue.UsedResource.Memory), float64(queue.MaxResource.Memory)) * 100
	cpuPercent := safeDivide(float64(queue.UsedResource.VCore), float64(queue.MaxResource.VCore)) * 100

	log.Printf("Queue: %s, memory_usage: %.2f%%, cpu_usage: %.2f%%, predicted_wait: %.0f seconds",
		queue.QueueName, memoryPercent, cpuPercent, predictedWait)
}

func safeDivide(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	result := numerator / denominator
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return 0
	}
	return result
}

func calculatePredictedWaitTime(queue *YuniKornQueue, apps []YuniKornApp) float64 {
	var runningApps, pendingApps int
	var totalRunTime float64

	for _, app := range apps {
		if app.QueueName != queue.QueueName {
			continue
		}

		if app.ApplicationState == "Running" {
			runningApps++
			if app.StartTime > 0 {
				elapsed := float64(time.Now().UnixNano()-app.StartTime) / 1e9
				remaining := avgAppDuration - elapsed
				if remaining > 0 {
					totalRunTime += remaining
				}
			}
		} else if isTrackableState(app.ApplicationState) && app.StartTime == 0 {
			pendingApps++
		}
	}

	// If no limits set, use simple estimation
	if queue.MaxResource.VCore == 0 || queue.MaxResource.Memory == 0 {
		return float64(pendingApps) * assumedWaitPerApp
	}

	// Calculate capacity usage
	capacityUsage := float64(queue.UsedResource.VCore) / float64(queue.MaxResource.VCore)
	if capacityUsage > 1.0 {
		capacityUsage = 1.0
	}

	// Predicted wait formula
	denominator := 1.0 - capacityUsage + 0.01
	predictedWait := (totalRunTime / denominator) + (float64(pendingApps) * avgAppDuration)

	if predictedWait > maxPredictedWait {
		predictedWait = maxPredictedWait
	}

	return predictedWait
}

func scrapeRejectedApps(partition string) error {
	rejectedURL := fmt.Sprintf("%s/ws/v1/partition/%s/applications/rejected", yuniKornURL, partition)
	log.Printf("Fetching rejected applications from: %s", rejectedURL)

	rejectedApps, err := fetchAPI[[]YuniKornApp](rejectedURL, "rejected_apps")
	if err != nil {
		return err
	}

	log.Printf("Found %d rejected applications", len(rejectedApps))
	processRejectedApps(rejectedApps)
	return nil
}

func processRejectedApps(rejectedApps []YuniKornApp) {
	for _, app := range rejectedApps {
		processRejectedApp(&app)
	}
}

func processRejectedApp(app *YuniKornApp) {
	queue := normalizeQueueName(app.QueueName)
	appID := app.ApplicationID
	reason := extractRejectionReason(app)

	// Increment rejected app count
	rejectedAppCount.WithLabelValues(queue, reason).Inc()

	// Set rejected app detail metric
	timestampSeconds := getSubmissionTimestamp(app)
	rejectedAppDetails.WithLabelValues(appID, queue, reason, fmt.Sprintf("%.0f", timestampSeconds)).Set(1)

	log.Printf("Rejected app: %s, queue: %s, reason: %s, timestamp: %.0f",
		appID, queue, reason, timestampSeconds)
}

func normalizeQueueName(queue string) string {
	if queue == "" {
		return unassignedQueue
	}
	return queue
}

func extractRejectionReason(app *YuniKornApp) string {
	if app.RejectedMessage != "" {
		reason := app.RejectedMessage
		if len(reason) > maxReasonLength {
			reason = reason[:maxReasonLength] + "..."
		}
		return reason
	}
	if app.ApplicationState != "" {
		return app.ApplicationState
	}
	return "unknown"
}

func getSubmissionTimestamp(app *YuniKornApp) float64 {
	if app.SubmissionTime > 0 {
		return float64(app.SubmissionTime) / 1e9
	}
	return float64(time.Now().Unix())
}
