package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	watchNamespaces []string
	port            string
	scrapeInterval  time.Duration
	once            sync.Once
)

func initConfig() {
	once.Do(func() {
		watchNs := os.Getenv("WATCH_NAMESPACES")
		if watchNs == "" {
			watchNamespaces = []string{"default"}
		} else {
			watchNamespaces = strings.Split(watchNs, ",")
		}

		port = os.Getenv("PORT")
		if port == "" {
			port = "9301"
		}

		interval := os.Getenv("SCRAPE_INTERVAL")
		if interval == "" {
			scrapeInterval = 30 * time.Second
		} else {
			var err error
			scrapeInterval, err = time.ParseDuration(interval)
			if err != nil {
				log.Printf("Invalid SCRAPE_INTERVAL, using default 30s: %v", err)
				scrapeInterval = 30 * time.Second
			}
		}
	})
}

// ===== Spark Metrics =====
var (
	sparkProcessingTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_app_processing_time_seconds",
			Help: "Total processing time of Spark job in seconds",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace"},
	)

	sparkPendingDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_app_pending_duration_seconds",
			Help: "Total duration in pending state in seconds",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace"},
	)

	sparkAppStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_app_status",
			Help: "Spark job status (1=completed, 0=failed, -1=running)",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace", "state"},
	)

	sparkAppCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_app_count_total",
			Help: "Total number of processed applications (completed or failed)",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace", "status"},
	)

	// NEW: Resource metrics
	sparkResourceCores = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_app_resource_cores_total",
			Help: "Total requested CPU cores (driver + executors)",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace", "component"},
	)

	sparkResourceMemory = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_app_resource_memory_bytes_total",
			Help: "Total requested memory in bytes (driver + executors)",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace", "component"},
	)

	sparkExecutorCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_app_executor_count",
			Help: "Number of executors requested",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace"},
	)

	sparkFailureCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_app_failure_count_total",
			Help: "Total number of failed Spark applications",
		},
		[]string{"app_name", "app_type", "provision_id", "queue", "service_id", "service_name", "namespace"},
	)

	sparkScrapeErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_exporter_scrape_errors_total",
			Help: "Total number of Spark exporter scrape errors",
		},
		[]string{"operation"},
	)
)

func init() {
	initConfig()

	prometheus.MustRegister(sparkProcessingTime)
	prometheus.MustRegister(sparkPendingDuration)
	prometheus.MustRegister(sparkAppStatus)
	prometheus.MustRegister(sparkAppCount)
	prometheus.MustRegister(sparkResourceCores)
	prometheus.MustRegister(sparkResourceMemory)
	prometheus.MustRegister(sparkExecutorCount)
	prometheus.MustRegister(sparkFailureCount)
	prometheus.MustRegister(sparkScrapeErrors)

	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// ===== Data Structures =====
type CustomLabels struct {
	AppName     string
	AppType     string
	ProvisionID string
	Queue       string
	ServiceID   string
	ServiceName string
	Namespace   string
}

type ResourceInfo struct {
	DriverCores      float64
	DriverMemory     int64
	ExecutorCores    float64
	ExecutorMemory   int64
	ExecutorCount    int
	TotalCores       float64
	TotalMemory      int64
}

type SparkMetricsData struct {
	Labels          CustomLabels
	Resources       ResourceInfo
	ProcessingTime  float64
	PendingDuration float64
	Status          string
	ExecutionAttempts int
	LastUpdateTime  time.Time
	Counted         bool
	Failed          bool
}

var sparkStore = make(map[string]*SparkMetricsData)
var sparkStoreMutex sync.RWMutex

// ===== Helper Functions =====
func extractLabels(obj *unstructured.Unstructured) CustomLabels {
	labels := obj.GetLabels()
	return CustomLabels{
		AppName:     getLabel(labels, "app-name"),
		AppType:     getLabel(labels, "app_type"),
		ProvisionID: getLabel(labels, "provision_id"),
		Queue:       getLabel(labels, "queue"),
		ServiceID:   getLabel(labels, "service-id"),
		ServiceName: getLabel(labels, "service-name"),
		Namespace:   obj.GetNamespace(),
	}
}

func getLabel(labels map[string]string, key string) string {
	if labels == nil {
		return "unknown"
	}
	if val, ok := labels[key]; ok {
		return val
	}
	return "unknown"
}

func getSparkStatus(obj *unstructured.Unstructured) (state string, submissionTime, terminationTime string, err error) {
	status, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status")
	if err != nil || !found {
		return "", "", "", fmt.Errorf("status not found")
	}

	statusMap, ok := status.(map[string]interface{})
	if !ok {
		return "", "", "", fmt.Errorf("invalid status format")
	}

	if appState, ok := statusMap["applicationState"].(map[string]interface{}); ok {
		if s, ok := appState["state"].(string); ok {
			state = s
		}
	}

	if t, ok := statusMap["lastSubmissionAttemptTime"].(string); ok {
		submissionTime = t
	}

	if t, ok := statusMap["terminationTime"].(string); ok {
		terminationTime = t
	}

	return state, submissionTime, terminationTime, nil
}

func extractResourceInfo(obj *unstructured.Unstructured) ResourceInfo {
	info := ResourceInfo{}

	// Driver resources
	if dc, ok := obj.Object["spec"].(map[string]interface{})["driver"].(map[string]interface{})["cores"]; ok {
		info.DriverCores = parseFloat(dc)
	}
	if dm, ok := obj.Object["spec"].(map[string]interface{})["driver"].(map[string]interface{})["memory"]; ok {
		info.DriverMemory = parseMemory(dm.(string))
	}

	// Executor resources
	if ec, ok := obj.Object["spec"].(map[string]interface{})["executor"].(map[string]interface{})["cores"]; ok {
		info.ExecutorCores = parseFloat(ec)
	}
	if em, ok := obj.Object["spec"].(map[string]interface{})["executor"].(map[string]interface{})["memory"]; ok {
		info.ExecutorMemory = parseMemory(em.(string))
	}
	if ei, ok := obj.Object["spec"].(map[string]interface{})["executor"].(map[string]interface{})["instances"]; ok {
		info.ExecutorCount = parseInt(ei)
	}

	// Calculate total resources
	info.TotalCores = info.DriverCores + (info.ExecutorCores * float64(info.ExecutorCount))
	info.TotalMemory = info.DriverMemory + (info.ExecutorMemory * int64(info.ExecutorCount))

	return info
}

func parseFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}

func parseInt(v interface{}) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int64:
		return int(val)
	case int:
		return val
	case string:
		i, err := strconv.Atoi(val)
		if err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
}

func parseMemory(memStr string) int64 {
	memStr = strings.TrimSpace(memStr)

	// Convert to bytes
	memStr = strings.ToLower(memStr)

	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(memStr, "mi") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(memStr, "mi")
	} else if strings.HasSuffix(memStr, "gi") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(memStr, "gi")
	} else if strings.HasSuffix(memStr, "m") {
		multiplier = 1000 * 1000
		numStr = strings.TrimSuffix(memStr, "m")
	} else if strings.HasSuffix(memStr, "g") {
		multiplier = 1000 * 1000 * 1000
		numStr = strings.TrimSuffix(memStr, "g")
	} else {
		numStr = memStr
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0
	}

	return int64(num * float64(multiplier))
}

func calculateProcessingTime(submissionTime, terminationTime string) float64 {
	if submissionTime == "" || terminationTime == "" {
		return 0
	}

	submission, err := time.Parse(time.RFC3339Nano, submissionTime)
	if err != nil {
		return 0
	}

	termination, err := time.Parse(time.RFC3339Nano, terminationTime)
	if err != nil {
		return 0
	}

	return termination.Sub(submission).Seconds()
}

func isTerminalState(state string) bool {
	return state == "COMPLETED" || state == "FAILED"
}

// ===== Metrics Update =====
func updateMetrics(obj *unstructured.Unstructured) {
	name := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	state, submissionTime, terminationTime, err := getSparkStatus(obj)
	if err != nil {
		sparkScrapeErrors.WithLabelValues("get_status").Inc()
		return
	}

	labels := extractLabels(obj)
	resources := extractResourceInfo(obj)

	sparkStoreMutex.Lock()
	defer sparkStoreMutex.Unlock()

	metrics, exists := sparkStore[name]
	if !exists {
		metrics = &SparkMetricsData{
			Labels:         labels,
			LastUpdateTime: time.Now(),
		}
		sparkStore[name] = metrics
	}

	// Update resources and status
	metrics.Resources = resources
	metrics.Status = state
	metrics.LastUpdateTime = time.Now()

	labelValues := []string{
		labels.AppName,
		labels.AppType,
		labels.ProvisionID,
		labels.Queue,
		labels.ServiceID,
		labels.ServiceName,
		labels.Namespace,
	}

	// Update resource metrics
	sparkResourceCores.WithLabelValues(append(labelValues, "driver")...).Set(metrics.Resources.DriverCores)
	sparkResourceCores.WithLabelValues(append(labelValues, "executors")...).Set(metrics.Resources.ExecutorCores * float64(metrics.Resources.ExecutorCount))
	sparkResourceCores.WithLabelValues(append(labelValues, "total")...).Set(metrics.Resources.TotalCores)

	sparkResourceMemory.WithLabelValues(append(labelValues, "driver")...).Set(float64(metrics.Resources.DriverMemory))
	sparkResourceMemory.WithLabelValues(append(labelValues, "executors")...).Set(float64(metrics.Resources.ExecutorMemory * int64(metrics.Resources.ExecutorCount)))
	sparkResourceMemory.WithLabelValues(append(labelValues, "total")...).Set(float64(metrics.Resources.TotalMemory))

	sparkExecutorCount.WithLabelValues(labelValues...).Set(float64(metrics.Resources.ExecutorCount))

	// Handle terminal state
	if isTerminalState(state) {
		metrics.ProcessingTime = calculateProcessingTime(submissionTime, terminationTime)
		metrics.PendingDuration = 2.0 // Placeholder

		sparkProcessingTime.WithLabelValues(labelValues...).Set(metrics.ProcessingTime)
		sparkPendingDuration.WithLabelValues(labelValues...).Set(metrics.PendingDuration)

		statusValue := -1.0
		if state == "COMPLETED" {
			statusValue = 1.0
		} else if state == "FAILED" {
			statusValue = 0.0

			// Increment failure count if not already counted as failed
			if !metrics.Failed {
				sparkFailureCount.WithLabelValues(labelValues...).Inc()
				metrics.Failed = true
			}
		}
		sparkAppStatus.WithLabelValues(append(labelValues, state)...).Set(statusValue)

		if !metrics.Counted {
			sparkAppCount.WithLabelValues(append(labelValues, state)...).Inc()
			metrics.Counted = true
		}

		log.Printf("Updated metrics for %s: state=%s, processing_time=%.2fs, cores=%.2f, memory=%d bytes, executors=%d",
			name, state, metrics.ProcessingTime, metrics.Resources.TotalCores, metrics.Resources.TotalMemory, metrics.Resources.ExecutorCount)
	}
}

func handleSparkEvent(eventType watch.EventType, obj *unstructured.Unstructured) {
	name := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())

	switch eventType {
	case watch.Added, watch.Modified:
		updateMetrics(obj)
	case watch.Deleted:
		sparkStoreMutex.Lock()
		defer sparkStoreMutex.Unlock()
		if metrics, exists := sparkStore[name]; exists {
			labelValues := []string{
				metrics.Labels.AppName,
				metrics.Labels.AppType,
				metrics.Labels.ProvisionID,
				metrics.Labels.Queue,
				metrics.Labels.ServiceID,
				metrics.Labels.ServiceName,
				metrics.Labels.Namespace,
			}

			// Delete all metrics
			sparkProcessingTime.DeleteLabelValues(labelValues...)
			sparkPendingDuration.DeleteLabelValues(labelValues...)
			sparkAppStatus.DeleteLabelValues(append(labelValues, metrics.Status)...)
			sparkAppCount.DeleteLabelValues(append(labelValues, metrics.Status)...)

			// Delete resource metrics with component label
			for _, component := range []string{"driver", "executors", "total"} {
				sparkResourceCores.DeleteLabelValues(append(labelValues, component)...)
				sparkResourceMemory.DeleteLabelValues(append(labelValues, component)...)
			}
			sparkExecutorCount.DeleteLabelValues(labelValues...)

			delete(sparkStore, name)
			log.Printf("Deleted metrics for %s", name)
		}
	}
}

func watchSparkNamespace(ctx context.Context, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, namespace string, stopCh <-chan struct{}) {
	watcher, err := dynamicClient.Resource(gvr).Namespace(namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		sparkScrapeErrors.WithLabelValues("watch").Inc()
		return
	}
	defer watcher.Stop()

	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return
			}
			if obj, ok := event.Object.(*unstructured.Unstructured); ok {
				handleSparkEvent(event.Type, obj)
			}
		case <-stopCh:
			return
		}
	}
}

func scanSparkApps(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, namespace string) {
	list, err := dynamicClient.Resource(gvr).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		sparkScrapeErrors.WithLabelValues("list").Inc()
		return
	}

	for _, item := range list.Items {
		handleSparkEvent(watch.Added, &item)
	}
}

// ===== Main =====
func main() {
	log.Printf("Starting SparkApplication Exporter v2")
	log.Printf("Namespaces: %v", watchNamespaces)
	log.Printf("Port: %s", port)

	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("Failed to create Kubernetes config: %v", err)
		}
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create dynamic client: %v", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sparkAppGVR := schema.GroupVersionResource{
		Group:    "sparkoperator.k8s.io",
		Version:  "v1beta2",
		Resource: "sparkapplications",
	}

	for _, ns := range watchNamespaces {
		go watchSparkNamespace(ctx, dynamicClient, sparkAppGVR, ns, stopCh)
	}

	for _, ns := range watchNamespaces {
		scanSparkApps(dynamicClient, sparkAppGVR, ns)
	}
	log.Printf("Spark watchers started for %d namespaces", len(watchNamespaces))

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "SparkApplication Exporter v2")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Metrics: /metrics")
		fmt.Fprintf(w, "Namespaces: %v\n", watchNamespaces)
		fmt.Fprintf(w, "Port: %s\n", port)
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Available Metrics:")
		fmt.Fprintln(w, "  spark_app_processing_time_seconds{...}")
		fmt.Fprintln(w, "  spark_app_pending_duration_seconds{...}")
		fmt.Fprintln(w, "  spark_app_status{..., state}")
		fmt.Fprintln(w, "  spark_app_count_total{..., status}")
		fmt.Fprintln(w, "  spark_app_resource_cores_total{..., component}")  // NEW
		fmt.Fprintln(w, "  spark_app_resource_memory_bytes_total{..., component}")  // NEW
		fmt.Fprintln(w, "  spark_app_executor_count{...}")  // NEW
		fmt.Fprintln(w, "  spark_app_failure_count_total{...}")  // NEW
		fmt.Fprintf(w, "\nTracked Spark apps: %d\n", len(sparkStore))
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})

	go func() {
		log.Printf("Server listening on :%s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}
