package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

type SparkRequest struct {
	ProvisionID string `json:"provision_id"`
	ServiceID   string `json:"service_id"`
	Category    string `json:"category"`
	Region      string `json:"region"`
	UID         string `json:"uid"`
	Arguments   string `json:"arguments"`
}

// Hardcoded kubeconfig content (base64 encoded to avoid escaping issues)
// 실제 kubeconfig 내용이 여기에 하드코딩됩니다
const kubeconfigContent = `apiVersion: v1
clusters:
- cluster:
    certificate-authority: /root/.minikube/ca.crt
    server: https://192.168.49.2:8443
  name: minikube
contexts:
- context:
    cluster: minikube
    user: minikube
    namespace: default
  name: minikube
current-context: minikube
kind: Config
preferences: {}
users:
- name: minikube
  user:
    client-certificate: /root/.minikube/profiles/minikube/client.crt
    client-key: /root/.minikube/profiles/minikube/client.key`

func sendSparkRequest(apiURL string) error {
	// 요청 생성
	sparkReq := SparkRequest{
		ProvisionID: "0002_wfbm",
		ServiceID:   "regression_data",
		Category:    "batch",
		Region:      "ic",
		UID:         "001",
		Arguments:   "jobid:regression_data area:local yparam:target_yield",
	}

	// JSON 변환
	jsonData, err := json.Marshal(sparkReq)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %v", err)
	}

	// HTTP 요청 전송
	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	// 응답 처리
	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)

	log.Printf("Response (HTTP %d): %s", resp.StatusCode, string(body[:n]))

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Printf("Starting Spark requester v2...")

	// API URL (hynix-service)
	apiURL := "http://hynix-service.default.svc.cluster.local:8080/api/v1/spark/create"

	// Kubernetes 클러스터 내에서 실행 중인지 확인
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		log.Printf("Running inside Kubernetes cluster")

		// 자신의 Pod 정보 확인
		podName := os.Getenv("POD_NAME")
		if podName == "" {
			hostname, _ := os.Hostname()
			podName = hostname
		}

		log.Printf("Pod Name: %s", podName)

		// 완료 후 Job 상태 업데이트
		defer func() {
			log.Printf("Job completed, Pod will terminate")
		}()
	} else {
		log.Printf("Running outside Kubernetes cluster")
		// 로컬 개발 환경
		// kubeconfig 파일 생성
		tmpFile := "/tmp/kubeconfig"
		err := os.WriteFile(tmpFile, []byte(kubeconfigContent), 0600)
		if err != nil {
			log.Fatalf("Error writing kubeconfig: %v", err)
		}
		defer os.Remove(tmpFile)
	}

	// Spark 요청 전송
	log.Printf("Sending Spark request to: %s", apiURL)
	err := sendSparkRequest(apiURL)
	if err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}

	log.Printf("Spark request sent successfully!")
	log.Printf("Job completed. Exiting...")

	// 정상 종료
	os.Exit(0)
}
