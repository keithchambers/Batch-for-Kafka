package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var apiURL string

func main() {
	root := &cobra.Command{
		Use:   "batch",
		Short: "Batch ingestion CLI",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if apiURL == "" {
				apiURL = getenv("BATCH_API_URL", "http://localhost:8000")
			}
		},
	}
	root.PersistentFlags().StringVar(&apiURL, "api", "", "Batch ingestion API URL")

	// model commands
	modelCmd := &cobra.Command{Use: "model", Short: "Model operations"}
	modelCmd.AddCommand(cmdModelList(), cmdModelDescribe(), cmdModelCreate(), cmdModelUpdate(), cmdModelDelete())
	root.AddCommand(modelCmd)

	// job commands
	jobCmd := &cobra.Command{Use: "job", Short: "Job operations"}
	jobCmd.AddCommand(cmdJobList(), cmdJobCreate(), cmdJobStatus(), cmdJobCancel(), cmdJobRejected())
	root.AddCommand(jobCmd)

	_ = root.Execute()
}

// ---------------- model commands ----------------

func cmdModelList() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List models",
		RunE: func(cmd *cobra.Command, args []string) error {
			return httpGet("/models")
		},
	}
}

func cmdModelDescribe() *cobra.Command {
	return &cobra.Command{
		Use:   "describe <model_id>",
		Short: "Describe a model",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return httpGet("/models/" + args[0])
		},
	}
}

func cmdModelCreate() *cobra.Command {
	return &cobra.Command{
		Use:   "create <name> <schema_file>",
		Short: "Create model",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			schemaFile := args[1]
			schema, err := os.ReadFile(schemaFile)
			if err != nil {
				return err
			}
			body, _ := json.Marshal(map[string]interface{}{
				"name":   name,
				"schema": json.RawMessage(schema),
			})
			return httpPost("/models", body)
		},
	}
}

func cmdModelUpdate() *cobra.Command {
	return &cobra.Command{
		Use:   "update <model_id> <schema_file>",
		Short: "Update model",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			schema, err := os.ReadFile(args[1])
			if err != nil {
				return err
			}
			body, _ := json.Marshal(map[string]interface{}{
				"schema": json.RawMessage(schema),
			})
			return httpPut("/models/"+id, body)
		},
	}
}

func cmdModelDelete() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <model_id>",
		Short: "Delete model",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return httpDelete("/models/" + args[0])
		},
	}
}

// ---------------- job commands ----------------

func cmdJobList() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			return httpGet("/jobs")
		},
	}
}

func cmdJobCreate() *cobra.Command {
	return &cobra.Command{
		Use:   "create <model_id> <file>",
		Short: "Create job",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			modelID, filePath := args[0], args[1]
			body := &bytes.Buffer{}
			w := multipart.NewWriter(body)
			_ = w.WriteField("model_id", modelID)
			fw, err := w.CreateFormFile("file", filepath.Base(filePath))
			if err != nil {
				return err
			}
			f, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err = io.Copy(fw, f); err != nil {
				return err
			}
			w.Close()

			req, _ := http.NewRequest("POST", apiURL+"/jobs", body)
			req.Header.Set("Content-Type", w.FormDataContentType())
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
			fmt.Println()
			return nil
		},
	}
}

func cmdJobStatus() *cobra.Command {
	return &cobra.Command{
		Use:   "status <job_id>",
		Short: "Job status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return httpGet("/jobs/" + args[0])
		},
	}
}

func cmdJobCancel() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <job_id>",
		Short: "Cancel job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return httpDelete("/jobs/" + args[0])
		},
	}
}

func cmdJobRejected() *cobra.Command {
	return &cobra.Command{
		Use:   "rejected <job_id>",
		Short: "List rejected rows",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return httpGet("/jobs/" + args[0] + "/rejected")
		},
	}
}

// ---------------- HTTP helpers ----------------

func httpGet(path string) error {
	resp, err := http.Get(apiURL + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
	return nil
}

func httpPost(path string, body []byte) error {
	resp, err := http.Post(apiURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
	return nil
}

func httpPut(path string, body []byte) error {
	req, _ := http.NewRequest("PUT", apiURL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
	return nil
}

func httpDelete(path string) error {
	req, _ := http.NewRequest("DELETE", apiURL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println()
	return nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
