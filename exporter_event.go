package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	watchNamespaces    []string
	port               string
	labelResolution    bool
	podNamePrefix      string
	containerNamePrefix string
	once               sync.Once
	dynamicClient      dynamic.Interface
	kubeClient         *kubernetes.Clientset
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
			port = "9302"
		}

		labelResolution = os.Getenv("LABEL_RESOLUTION") != "false"

		podNamePrefix = os.Getenv("POD_NAME_PREFIX")
		containerNamePrefix = os.Getenv("CONTAINER_NAME_PREFIX")
	})
}

// ===== Image Pull Metrics =====
var (
	sparkImagePullDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_app_image_pull_duration_seconds",
			Help: "Docker image pull duration in seconds for Spark applications",
		},
		[]string{
			"app_name", "app_type", "provision_id", "queue",
			"service_id", "service_name", "namespace",
			"pod_name", "container_name", "image",
		},
	)

	sparkImagePullSuccess = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_app_image_pull_success_total",
			Help: "Total number of successful image pulls for Spark applications",
		},
		[]string{
			"app_name", "app_type", "provision_id", "queue",
			"service_id", "service_name", "namespace",
			"image",
		},
	)

	sparkImagePullFailure = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_app_image_pull_failure_total",
			Help: "Total number of failed image pulls for Spark applications",
		},
		[]string{
			"app_name", "app_type", "provision_id", "queue",
			"service_id", "service_name", "namespace",
			"image", "reason",
		},
	)

	scrapeErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_event_exporter_scrape_errors_total",
			Help: "Total number of event exporter scrape errors",
		},
		[]string{"operation"},
	)
)

func init() {
	initConfig()
	prometheus.MustRegister(sparkImagePullDuration)
	prometheus.MustRegister(sparkImagePullSuccess)
	prometheus.MustRegister(sparkImagePullFailure)
	prometheus.MustRegister(scrapeErrors)
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

type PullState struct {
	PullingTime    time.Time
	ImageName      string
	ContainerName  string
	PodName        string
	Namespace      string
	SparkAppLabels CustomLabels
}

type EventStore struct {
	sync.RWMutex
	states map[string]*PullState  // key: podUID
}

var eventStore = &EventStore{
	states: make(map[string]*PullState),
}

// ===== Helper Functions =====
func getSparkAppLabels(namespace, podName string) CustomLabels {
	// Try to get CR name from Pod's owner reference
	sparkAppName := extractSparkAppNameFromOwner(namespace, podName)
	if sparkAppName == "" {
		sparkAppName = extractSparkAppName(podName)
	}

	labels := CustomLabels{
		AppName:     sparkAppName,
		AppType:     "unknown",
		ProvisionID: "unknown",
		Queue:       "unknown",
		ServiceID:   "unknown",
		ServiceName: "unknown",
		Namespace:   namespace,
	}

	if !labelResolution || dynamicClient == nil {
		return labels
	}

	// Get SparkApplication CR to extract labels
	sparkAppGVR := schema.GroupVersionResource{
		Group:    "sparkoperator.k8s.io",
		Version:  "v1beta2",
		Resource: "sparkapplications",
	}

	obj, err := dynamicClient.Resource(sparkAppGVR).
		Namespace(namespace).
		Get(context.Background(), sparkAppName, metav1.GetOptions{})

	if err != nil {
		log.Printf("Failed to get SparkApplication %s/%s: %v", namespace, sparkAppName, err)
		scrapeErrors.WithLabelValues("get_cr").Inc()
		return labels
	}

	crLabels := obj.GetLabels()
	labels.AppName = getLabel(crLabels, "app-name")
	labels.AppType = getLabel(crLabels, "app_type")
	labels.ProvisionID = getLabel(crLabels, "provision_id")
	labels.Queue = getLabel(crLabels, "queue")
	labels.ServiceID = getLabel(crLabels, "service-id")
	labels.ServiceName = getLabel(crLabels, "service-name")

	return labels
}

func extractSparkAppNameFromOwner(namespace, podName string) string {
	// Get Pod to check owner reference
	pod, err := kubeClient.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		return ""
	}

	// Check owner references for SparkApplication
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "SparkApplication" {
			return owner.Name
		}
	}

	return ""
}

func extractSparkAppName(podName string) string {
	// Fallback: Extract name from pod name
	// Driver pod: my-app-driver -> my-app
	// Executor pod: my-app-exec-1 -> my-app
	// Executor pod with hash: my-app-exec-1-<hash> -> my-app

	// Remove -driver suffix
	if strings.HasSuffix(podName, "-driver") {
		return strings.TrimSuffix(podName, "-driver")
	}

	// Remove -exec-<number> suffix (with optional hash suffix)
	if idx := strings.Index(podName, "-exec-"); idx > 0 {
		name := podName[:idx]
		// Remove any additional -<hash> suffix from exec pods
		if lastHyphen := strings.LastIndex(name, "-"); lastHyphen > 0 {
			// Check if there's a hash after the exec number
			parts := strings.Split(podName[idx+5:], "-") // Skip "-exec-"
			if len(parts) > 1 && len(parts[1]) > 5 { // Hash is typically > 5 chars
				return name[:lastHyphen]
			}
		}
		return name
	}

	return podName
}

func extractImageName(message string) string {
	// Message format: "Pulling image gcr.io/spark:v3.3.0"
	re := regexp.MustCompile(`Pulling image\s+([^\s]+)`)
	matches := re.FindStringSubmatch(message)
	if len(matches) > 1 {
		return matches[1]
	}
	return "unknown"
}

func extractContainerName(message string) string {
	// Message format: "Pulling image gcr.io/spark:v3.3.0 for container spark-kubernetes-driver"
	re := regexp.MustCompile(`for container\s+([^\s]+)`)
	matches := re.FindStringSubmatch(message)
	if len(matches) > 1 {
		return matches[1]
	}
	return "unknown"
}

func isSparkAppPod(event *corev1.Event) bool {
	// Check if pod name indicates Spark application
	podName := event.InvolvedObject.Name
	return strings.HasSuffix(podName, "-driver") ||
		strings.Contains(podName, "-exec-")
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

// ===== Event Handlers =====
func handlePullingEvent(event *corev1.Event, podUID string) {
	eventStore.Lock()
	defer eventStore.Unlock()

	labels := getSparkAppLabels(event.InvolvedObject.Namespace, event.InvolvedObject.Name)

	// Use FirstTimestamp for accurate pull duration calculation
	pullingTime := event.FirstTimestamp.Time
	if pullingTime.IsZero() {
		pullingTime = event.EventTime.Time
	}

	eventStore.states[podUID] = &PullState{
		PullingTime:    pullingTime,
		ImageName:      extractImageName(event.Message),
		ContainerName:  extractContainerName(event.Message),
		PodName:        event.InvolvedObject.Name,
		Namespace:      event.InvolvedObject.Namespace,
		SparkAppLabels: labels,
	}

	log.Printf("[Pulling] pod=%s/%s, image=%s, container=%s",
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		extractImageName(event.Message),
		extractContainerName(event.Message))
}

func handlePulledEvent(event *corev1.Event, podUID string) {
	eventStore.Lock()
	defer eventStore.Unlock()

	state, exists := eventStore.states[podUID]
	if !exists {
		log.Printf("[Pulled] No matching Pulling event for podUID=%s", podUID)
		return
	}

	// Use FirstTimestamp for accurate pull duration calculation
	pulledTime := event.FirstTimestamp.Time
	if pulledTime.IsZero() {
		pulledTime = event.EventTime.Time
	}

	// Calculate pull duration
	pullDuration := pulledTime.Sub(state.PullingTime).Seconds()

	// Update metrics
	labelValues := []string{
		state.SparkAppLabels.AppName,
		state.SparkAppLabels.AppType,
		state.SparkAppLabels.ProvisionID,
		state.SparkAppLabels.Queue,
		state.SparkAppLabels.ServiceID,
		state.SparkAppLabels.ServiceName,
		state.Namespace,
		state.PodName,
		state.ContainerName,
		state.ImageName,
	}

	sparkImagePullDuration.WithLabelValues(labelValues...).Set(pullDuration)

	sparkImagePullSuccess.WithLabelValues(
		state.SparkAppLabels.AppName,
		state.SparkAppLabels.AppType,
		state.SparkAppLabels.ProvisionID,
		state.SparkAppLabels.Queue,
		state.SparkAppLabels.ServiceID,
		state.SparkAppLabels.ServiceName,
		state.Namespace,
		state.ImageName,
	).Inc()

	log.Printf("[Pulled] pod=%s/%s, image=%s, duration=%.2fs",
		state.Namespace,
		state.PodName,
		state.ImageName,
		pullDuration)

	// Clean up state
	delete(eventStore.states, podUID)
}

func handlePullFailedEvent(event *corev1.Event, podUID string) {
	eventStore.Lock()
	defer eventStore.Unlock()

	// Get labels (may have Pulling state or not)
	var labels CustomLabels
	var imageName string

	if state, exists := eventStore.states[podUID]; exists {
		labels = state.SparkAppLabels
		imageName = state.ImageName
		delete(eventStore.states, podUID)
	} else {
		labels = getSparkAppLabels(event.InvolvedObject.Namespace, event.InvolvedObject.Name)
		imageName = extractImageName(event.Message)
	}

	// Update failure metric
	sparkImagePullFailure.WithLabelValues(
		labels.AppName,
		labels.AppType,
		labels.ProvisionID,
		labels.Queue,
		labels.ServiceID,
		labels.ServiceName,
		labels.Namespace,
		imageName,
		event.Reason,
	).Inc()

	log.Printf("[Failed] pod=%s/%s, image=%s, reason=%s",
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		imageName,
		event.Reason)
}

func handleEvent(event *corev1.Event) {
	// Filter by pod name prefix if configured
	if podNamePrefix != "" && !strings.HasPrefix(event.InvolvedObject.Name, podNamePrefix) {
		return
	}

	// Filter only Spark application pods
	if !isSparkAppPod(event) {
		return
	}

	// Filter by container name prefix if configured
	if containerNamePrefix != "" {
		containerName := extractContainerName(event.Message)
		if !strings.HasPrefix(containerName, containerNamePrefix) {
			return
		}
	}

	// Filter only image pull related events
	if event.Reason != "Pulling" && event.Reason != "Pulled" &&
		event.Reason != "Failed" && event.Reason != "ErrImagePull" &&
		event.Reason != "ImagePullBackOff" {
		return
	}

	podUID := string(event.InvolvedObject.UID)

	switch event.Reason {
	case "Pulling":
		handlePullingEvent(event, podUID)

	case "Pulled":
		handlePulledEvent(event, podUID)

	case "Failed", "ErrImagePull", "ImagePullBackOff":
		handlePullFailedEvent(event, podUID)
	}
}

func startEventWatcher(ctx context.Context, namespace string, stopCh <-chan struct{}) {
	log.Printf("Starting event watcher for namespace: %s", namespace)

	// Watch for Pod events
	watcher, err := kubeClient.CoreV1().Events(namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.kind=Pod",
		Watch:         true,
	})
	if err != nil {
		log.Printf("Failed to create event watcher for %s: %v", namespace, err)
		scrapeErrors.WithLabelValues("watch").Inc()
		return
	}
	defer watcher.Stop()

	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				log.Printf("Event watcher for %s closed, restarting...", namespace)
				return
			}

			if e, ok := event.Object.(*corev1.Event); ok {
				handleEvent(e)
			}

		case <-stopCh:
			log.Printf("Stopping event watcher for %s", namespace)
			return
		}
	}
}

func scanExistingEvents(ctx context.Context, namespace string) {
	log.Printf("Scanning existing events in %s", namespace)

	events, err := kubeClient.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.kind=Pod",
		Limit:         100,
	})
	if err != nil {
		log.Printf("Failed to list events in %s: %v", namespace, err)
		scrapeErrors.WithLabelValues("list").Inc()
		return
	}

	log.Printf("Found %d events in %s", len(events.Items), namespace)

	for _, event := range events.Items {
		handleEvent(&event)
	}
}

// ===== Main =====
func main() {
	log.Printf("Starting Spark Image Pull Event Exporter")
	log.Printf("Namespaces: %v", watchNamespaces)
	log.Printf("Port: %s", port)
	log.Printf("Label resolution: %v", labelResolution)
	if podNamePrefix != "" {
		log.Printf("Pod name prefix filter: %s", podNamePrefix)
	}
	if containerNamePrefix != "" {
		log.Printf("Container name prefix filter: %s", containerNamePrefix)
	}

	// Create Kubernetes config
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

	// Create clients
	kubeClient, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	dynamicClient, err = dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create dynamic client: %v", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start event watchers for each namespace
	for _, ns := range watchNamespaces {
		// Scan existing events first
		scanExistingEvents(ctx, ns)

		// Start watcher
		go startEventWatcher(ctx, ns, stopCh)
	}

	log.Printf("Event watchers started for %d namespaces", len(watchNamespaces))

	// HTTP server
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Spark Image Pull Event Exporter")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Metrics: /metrics")
		fmt.Fprintf(w, "Namespaces: %v\n", watchNamespaces)
		fmt.Fprintf(w, "Port: %s\n", port)
		fmt.Fprintf(w, "Label resolution: %v\n", labelResolution)
		if podNamePrefix != "" {
			fmt.Fprintf(w, "Pod name prefix: %s\n", podNamePrefix)
		}
		if containerNamePrefix != "" {
			fmt.Fprintf(w, "Container name prefix: %s\n", containerNamePrefix)
		}
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Available Metrics:")
		fmt.Fprintln(w, "  spark_app_image_pull_duration_seconds{...}")
		fmt.Fprintln(w, "  spark_app_image_pull_success_total{...}")
		fmt.Fprintln(w, "  spark_app_image_pull_failure_total{...}")
		fmt.Fprintf(w, "\nTracking states: %d\n", len(eventStore.states))
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

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}
