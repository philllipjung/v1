package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type SparkRequest struct {
	ProvisionID string `json:"provision_id"`
	ServiceID   string `json:"service_id"`
	Category    string `json:"category"`
	Region      string `json:"region"`
	UID         string `json:"uid"`
	Arguments   string `json:"arguments"`
}

func main() {
	apiURL := "http://localhost:8080/api/v1/spark/create"

	// 3개 요청: uid 001, 002, 003
	//uids := []string{"001", "002", "003"}
	uids := []string{"001"}
	log.Printf("Starting Spark request sender...")
	log.Printf("Total requests: %d", len(uids))
	log.Printf("Interval: 20 seconds")
	log.Printf("")

	for i, uid := range uids {
		reqNum := i + 1
		log.Printf("[%d/%d] Sending request with uid=%s", reqNum, len(uids), uid)

		// 요청 생성
		sparkReq := SparkRequest{
			ProvisionID: "0002_wfbm",
			ServiceID:   "regression_data",
			Category:    "batch",
			Region:      "ic",
			UID:         uid,
			Arguments:   "jobid:regression_data area:local yparam:target_yield",
		}

		// JSON 변환
		jsonData, err := json.Marshal(sparkReq)
		if err != nil {
			log.Printf("Error marshaling JSON: %v", err)
			continue
		}

		// HTTP 요청 전송
		resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("Error sending request: %v", err)
			continue
		}

		// 응답 처리
		body := make([]byte, 1024)
		n, _ := resp.Body.Read(body)
		resp.Body.Close()

		log.Printf("Response (HTTP %d): %s", resp.StatusCode, string(body[:n]))

		// 마지막 요청이 아니면 대기
		if reqNum < len(uids) {
			log.Printf("Waiting 20 seconds before next request...")
			log.Printf("")
			time.Sleep(20 * time.Second)
		}
	}

	log.Printf("")
	log.Printf("All requests completed!")
}
