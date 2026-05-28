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
	log.Printf("Starting Spark requester v3...")

	// API URL (hynix-service)
	apiURL := "http://hynix-service.default.svc.cluster.local:8080/api/v1/spark/create"

	// Spark 요청 전송
	log.Printf("Sending Spark request to: %s", apiURL)
	err := sendSparkRequest(apiURL)
	if err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}

	log.Printf("Spark request sent successfully!")
}
