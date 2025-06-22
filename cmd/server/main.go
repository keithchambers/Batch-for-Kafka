package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	kafka "github.com/segmentio/kafka-go"
)

const maxUploadBytes = 1 << 30 // 1 GB

type Model struct {
	ID     string          `json:"id"`
	Name   string          `json:"name"`
	Schema json.RawMessage `json:"schema"`
}

type RejectedRow struct {
	JobID     string    `json:"job_id"`
	RowNumber int       `json:"row_number"`
	RawData   string    `json:"raw_data"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	modelsMu sync.RWMutex
	models   = map[string]Model{}
	jobsMu   sync.RWMutex
	jobs     = map[string]*JobStatus{}
)

type JobState string

const (
	StatePending        JobState = "PENDING"
	StateRunning        JobState = "RUNNING"
	StateSuccess        JobState = "SUCCESS"
	StatePartialSuccess JobState = "PARTIAL_SUCCESS"
	StateFailed         JobState = "FAILED"
	StateCancelled      JobState = "CANCELLED"
)

type JobStatus struct {
	JobID   string   `json:"job_id"`
	ModelID string   `json:"model_id"`
	State   JobState `json:"state"`
	Totals  struct {
		Rows   int `json:"rows"`
		OK     int `json:"ok"`
		Errors int `json:"errors"`
	} `json:"totals"`
	Timings struct {
		WaitingMS    int64 `json:"waiting_ms"`
		ProcessingMS int64 `json:"processing_ms"`
	} `json:"timings"`
	UpdatedAt time.Time `json:"updated_at"`
	StartedAt time.Time `json:"started_at"`
	Cancelled bool      `json:"-"`
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	rand.Seed(time.Now().UnixNano())
	r := mux.NewRouter()

	r.HandleFunc("/models", listModels).Methods("GET")
	r.HandleFunc("/models", createModel).Methods("POST")
	r.HandleFunc("/models/{id}", getModel).Methods("GET")
	r.HandleFunc("/models/{id}", updateModel).Methods("PUT")
	r.HandleFunc("/models/{id}", deleteModel).Methods("DELETE")
	r.HandleFunc("/jobs", createJob).Methods("POST")
	r.HandleFunc("/jobs", listJobs).Methods("GET")
	r.HandleFunc("/jobs/{id}", getJob).Methods("GET")
	r.HandleFunc("/jobs/{id}", cancelJob).Methods("DELETE")
	r.HandleFunc("/jobs/{id}/rejected", rejectedRows).Methods("GET")
	r.HandleFunc("/healthz", healthCheck).Methods("GET")

	port := getenv("PORT", "8000")
	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

// ------------------ model handlers ------------------

func listModels(w http.ResponseWriter, r *http.Request) {
	modelsMu.RLock()
	defer modelsMu.RUnlock()
	var list []Model
	for _, m := range models {
		list = append(list, m)
	}
	writeJSON(w, http.StatusOK, list)
}

func createModel(w http.ResponseWriter, r *http.Request) {
	var m Model
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		badRequest(w, "INVALID_JSON", err.Error())
		return
	}
	if m.ID == "" {
		m.ID = randomID()
	}
	modelsMu.Lock()
	models[m.ID] = m
	modelsMu.Unlock()
	writeJSON(w, http.StatusCreated, m)
}

func getModel(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	modelsMu.RLock()
	defer modelsMu.RUnlock()
	if m, ok := models[id]; ok {
		writeJSON(w, http.StatusOK, m)
	} else {
		notFound(w, "MODEL_NOT_FOUND", "model not found")
	}
}

func updateModel(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	var updated Model
	if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
		badRequest(w, "INVALID_JSON", err.Error())
		return
	}
	modelsMu.Lock()
	defer modelsMu.Unlock()
	if _, ok := models[id]; !ok {
		notFound(w, "MODEL_NOT_FOUND", "model not found")
		return
	}
	updated.ID = id
	models[id] = updated
	writeJSON(w, http.StatusOK, updated)
}

func deleteModel(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	modelsMu.Lock()
	defer modelsMu.Unlock()
	if _, ok := models[id]; !ok {
		notFound(w, "MODEL_NOT_FOUND", "model not found")
		return
	}
	delete(models, id)
	w.WriteHeader(http.StatusNoContent)
}

// ------------------ job handlers ------------------

func createJob(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxUploadBytes); err != nil {
		badRequest(w, "INVALID_MULTIPART", err.Error())
		return
	}
	modelID := r.FormValue("model_id")
	if modelID == "" {
		badRequest(w, "MISSING_MODEL_ID", "model_id is required")
		return
	}
	modelsMu.RLock()
	if _, ok := models[modelID]; !ok {
		modelsMu.RUnlock()
		badRequest(w, "MODEL_NOT_FOUND", "model not found")
		return
	}
	modelsMu.RUnlock()

	file, header, err := r.FormFile("file")
	if err != nil {
		badRequest(w, "MISSING_FILE", err.Error())
		return
	}
	defer file.Close()

	if header.Size > maxUploadBytes {
		badRequest(w, "FILE_TOO_LARGE", "file exceeds 1GiB limit")
		return
	}

	buf := make([]byte, 4)
	if _, err := io.ReadFull(file, buf); err != nil {
		badRequest(w, "READ_ERROR", err.Error())
		return
	}
	// Reset reader to beginning
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		internalError(w, err)
		return
	}

	var fileType string
	if string(buf) == "PAR1" {
		fileType = "parquet"
	} else if strings.Contains(filepath.Ext(header.Filename), ".csv") || buf[0] != 0x50 { // simple check
		fileType = "csv"
	} else {
		badRequest(w, "UNSUPPORTED_FILE_TYPE", "only .csv or .parquet files are allowed")
		return
	}

	jobID := randomID()
	js := &JobStatus{
		JobID:     jobID,
		ModelID:   modelID,
		State:     StatePending,
		UpdatedAt: time.Now(),
	}
	jobsMu.Lock()
	jobs[jobID] = js
	jobsMu.Unlock()

	go processJob(js, file, fileType) // async

	writeJSON(w, http.StatusAccepted, map[string]string{"job_id": jobID})
}

func processJob(js *JobStatus, f multipart.File, kind string) {
	start := time.Now()
	js.State = StateRunning
	js.StartedAt = time.Now()
	js.UpdatedAt = time.Now()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:19092"), ",")

	// Create main topic writer with auto-creation
	mainTopic := "batch_" + js.JobID
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        mainTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: 1,
		Async:        false,
	})
	defer writer.Close()

	// Create DLQ topic writer with auto-creation
	dlqTopic := "batch_" + js.JobID + "_dlq"
	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: 1,
		Async:        false,
	})
	defer dlqWriter.Close()

	// Create topics if they don't exist
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		log.Printf("Failed to connect to Kafka: %v", err)
		js.State = StateFailed
		js.UpdatedAt = time.Now()
		return
	}
	defer conn.Close()

	// Create main topic
	mainTopicConfig := kafka.TopicConfig{
		Topic:             mainTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "cleanup.policy", ConfigValue: "delete"},
			{ConfigName: "retention.ms", ConfigValue: "604800000"}, // 7 days
		},
	}

	// Create DLQ topic
	dlqTopicConfig := kafka.TopicConfig{
		Topic:             dlqTopic,
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "cleanup.policy", ConfigValue: "delete"},
			{ConfigName: "retention.ms", ConfigValue: "604800000"}, // 7 days
		},
	}

	err = conn.CreateTopics(mainTopicConfig, dlqTopicConfig)
	if err != nil {
		log.Printf("Failed to create topics (may already exist): %v", err)
		// Continue anyway - topics might already exist
	}

	// Helper function to send rejected row to DLQ
	sendToDLQ := func(rowNum int, rawData string, errorMsg string) {
		rejectedRow := RejectedRow{
			JobID:     js.JobID,
			RowNumber: rowNum,
			RawData:   rawData,
			Error:     errorMsg,
			Timestamp: time.Now(),
		}

		payload, err := json.Marshal(rejectedRow)
		if err != nil {
			log.Printf("Failed to marshal rejected row: %v", err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = dlqWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(js.JobID),
			Value: payload,
		})
		if err != nil {
			log.Printf("Failed to write to DLQ: %v", err)
		}
	}

	rl := csv.NewReader(f)
	rowNumber := 0

	for {
		rowNumber++
		rec, err := rl.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			js.Totals.Errors++
			// Convert row to string for DLQ
			rawData := ""
			if rec != nil {
				rawData = strings.Join(rec, ",")
			}
			sendToDLQ(rowNumber, rawData, err.Error())
			continue
		}

		js.Totals.Rows++

		// Try to send to main topic
		payload, err := json.Marshal(rec)
		if err != nil {
			js.Totals.Errors++
			sendToDLQ(rowNumber, strings.Join(rec, ","), "JSON marshal error: "+err.Error())
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(js.JobID),
			Value: payload,
		})

		if err != nil {
			js.Totals.Errors++
			sendToDLQ(rowNumber, strings.Join(rec, ","), "Kafka write error: "+err.Error())
			continue
		}

		js.Totals.OK++
	}

	js.Timings.ProcessingMS = time.Since(start).Milliseconds()

	// Determine final state
	if js.Totals.Errors > 0 && js.Totals.OK > 0 {
		js.State = StatePartialSuccess
	} else if js.Totals.Errors > 0 {
		js.State = StateFailed
	} else {
		js.State = StateSuccess
	}

	js.UpdatedAt = time.Now()

	log.Printf("Job %s completed: %d rows, %d ok, %d errors",
		js.JobID, js.Totals.Rows, js.Totals.OK, js.Totals.Errors)
}

func listJobs(w http.ResponseWriter, r *http.Request) {
	jobsMu.RLock()
	defer jobsMu.RUnlock()
	var list []*JobStatus
	for _, j := range jobs {
		list = append(list, j)
	}
	writeJSON(w, http.StatusOK, list)
}

func getJob(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	jobsMu.RLock()
	defer jobsMu.RUnlock()
	if j, ok := jobs[id]; ok {
		writeJSON(w, http.StatusOK, j)
	} else {
		notFound(w, "JOB_NOT_FOUND", "job not found")
	}
}

func cancelJob(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	jobsMu.Lock()
	defer jobsMu.Unlock()
	if j, ok := jobs[id]; ok {
		j.State = StateCancelled
		j.Cancelled = true
		j.UpdatedAt = time.Now()
		writeJSON(w, http.StatusAccepted, j)
	} else {
		notFound(w, "JOB_NOT_FOUND", "job not found")
	}
}

func rejectedRows(w http.ResponseWriter, r *http.Request) {
	jobId := mux.Vars(r)["id"]

	// Check if job exists
	jobsMu.RLock()
	if _, ok := jobs[jobId]; !ok {
		jobsMu.RUnlock()
		notFound(w, "JOB_NOT_FOUND", "job not found")
		return
	}
	jobsMu.RUnlock()

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:19092"), ",")

	// Create reader for DLQ topic with unique group ID
	dlqTopic := "batch_" + jobId + "_dlq"
	groupID := "rejected-rows-reader-" + jobId + "-" + randomID()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       dlqTopic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	var rejectedRows []RejectedRow
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Read all available messages from DLQ
	for {
		select {
		case <-ctx.Done():
			// Timeout reached, return what we have
			writeJSON(w, http.StatusOK, rejectedRows)
			return
		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				// No more messages or error - return what we have
				if len(rejectedRows) == 0 {
					writeJSON(w, http.StatusOK, []RejectedRow{})
				} else {
					writeJSON(w, http.StatusOK, rejectedRows)
				}
				return
			}

			var rejectedRow RejectedRow
			if err := json.Unmarshal(msg.Value, &rejectedRow); err != nil {
				log.Printf("Failed to unmarshal rejected row: %v", err)
				reader.CommitMessages(ctx, msg)
				continue
			}

			rejectedRows = append(rejectedRows, rejectedRow)
			reader.CommitMessages(ctx, msg)
		}
	}
}

// ------------------ helpers ------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, code, msg string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{
		"error":   code,
		"message": msg,
	})
}

func notFound(w http.ResponseWriter, code, msg string) {
	writeJSON(w, http.StatusNotFound, map[string]string{
		"error":   code,
		"message": msg,
	})
}

func internalError(w http.ResponseWriter, err error) {
	log.Println("internal error:", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{
		"error":   "INTERNAL_ERROR",
		"message": err.Error(),
	})
}

func randomID() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
