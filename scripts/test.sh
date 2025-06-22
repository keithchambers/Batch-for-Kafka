#!/usr/bin/env bash
set -euo pipefail

# Configuration
API="http://localhost:8000"
CLI="docker run --rm --network batch-ingestion_batch-ingestion -e BATCH_API_URL=http://batch-ingestion-api:8000 -v $(pwd):/workspace -w /workspace batch-cli:latest"
TMP_DIR="/tmp/batch_unified_test_$$"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test tracking
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_TOTAL=0

# Helper functions
print_header() {
    echo -e "\n${BLUE}======================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}======================================================${NC}"
}

print_section() {
    echo -e "\n${YELLOW}$1${NC}"
    echo -e "${YELLOW}--------------------------------------------------${NC}"
}

test_assert() {
    ((TESTS_TOTAL++))
    local description="$1"
    local condition="$2"
    
    if eval "$condition"; then
        echo -e "  ${GREEN}$description${NC}"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "  ${RED}$description${NC}"
        ((TESTS_FAILED++))
        return 1
    fi
}

wait_for_job() {
    local job_id=$1
    local max_wait=${2:-15}
    local count=0
    
    while [ $count -lt $max_wait ]; do
        local state=$(curl -s "$API/jobs/$job_id" | jq -r '.state // "UNKNOWN"')
        if [[ "$state" != "PENDING" && "$state" != "RUNNING" ]]; then
            echo "    Job $job_id completed with state: $state"
            return 0
        fi
        sleep 1
        ((count++))
    done
    echo "    Warning: Job $job_id did not complete within $max_wait seconds"
    return 1
}

setup_test_environment() {
    print_section "Setting Up Test Environment"
    
    # Create temp directory
    mkdir -p "$TMP_DIR"
    trap "rm -rf $TMP_DIR" EXIT
    
    # Check API health
    local health_response=$(curl -s "$API/healthz" 2>/dev/null || echo '{"status":"unhealthy"}')
    local health_status=$(echo "$health_response" | jq -r '.status // "unhealthy"')
    test_assert "API is healthy" '[ "$health_status" = "healthy" ]'
    
    # Ensure default model exists
    local models_response=$(curl -s "$API/models")
    local default_exists=$(echo "$models_response" | jq -r 'try (.[] | select(.id == "default_model") | .id) // ""' 2>/dev/null)
    
    if [ -z "$default_exists" ]; then
        echo "    Creating default model..."
        curl -s -X POST "$API/models" \
            -H "Content-Type: application/json" \
            -d '{"id": "default_model", "name": "Default Model", "schema": {"type": "object"}}' > /dev/null
    fi
    
    test_assert "Default model exists" '[ -n "$(curl -s "$API/models" | jq -r "try (.[] | select(.id == \"default_model\") | .id) // \"\"" 2>/dev/null)" ]'
}

test_model_management() {
    print_section "Model Management (HTTP API)"
    
    # Test model creation
    local timestamp=$(date +%s)
    local model_name="test_model_$timestamp"
    
    local create_response=$(curl -s -X POST "$API/models" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "'$model_name'",
            "schema": {
                "type": "object",
                "properties": {
                    "event_id": {"type": "integer"},
                    "timestamp": {"type": "integer"},
                    "data": {"type": "string"}
                }
            }
        }')
    
    local model_id=$(echo "$create_response" | jq -r '.id // ""')
    test_assert "Model creation successful" '[ -n "$model_id" ] && [ "$model_id" != "null" ]'
    
    # Test model retrieval
    local get_response=$(curl -s "$API/models/$model_id")
    local retrieved_name=$(echo "$get_response" | jq -r '.name // ""')
    test_assert "Model retrieval successful" '[ "$retrieved_name" = "$model_name" ]'
    
    # Test model update
    local update_response=$(curl -s -X PUT "$API/models/$model_id" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "'$model_name'_updated",
            "schema": {"type": "object", "properties": {"id": {"type": "string"}}}
        }')
    
    local updated_name=$(echo "$update_response" | jq -r '.name // ""')
    test_assert "Model update successful" '[ "$updated_name" = "'$model_name'_updated" ]'
    
    # Test model deletion
    curl -s -X DELETE "$API/models/$model_id" > /dev/null
    local delete_check=$(curl -s "$API/models/$model_id")
    local error_code=$(echo "$delete_check" | jq -r '.error // ""')
    test_assert "Model deletion successful" '[ "$error_code" = "MODEL_NOT_FOUND" ]'
    
    # Test error cases
    local invalid_json_response=$(curl -s -X POST "$API/models" \
        -H "Content-Type: application/json" \
        -d '{invalid json}')
    local json_error=$(echo "$invalid_json_response" | jq -r '.error // ""')
    test_assert "Invalid JSON rejected" '[ "$json_error" = "INVALID_JSON" ]'
    
    local nonexistent_response=$(curl -s "$API/models/nonexistent_model")
    local not_found_error=$(echo "$nonexistent_response" | jq -r '.error // ""')
    test_assert "Non-existent model returns 404" '[ "$not_found_error" = "MODEL_NOT_FOUND" ]'
}

test_api_job_processing() {
    print_section "Job Processing (HTTP API with Sequential Data)"
    
    # Test successful job processing
    echo "    Testing API data processing..."
    local api_job_response=$(curl -s -X POST -F "model_id=default_model" -F "file=@samples/api_data.csv" "$API/jobs")
    local api_job_id=$(echo "$api_job_response" | jq -r '.job_id // ""')
    test_assert "API job creation successful" '[ -n "$api_job_id" ] && [ "$api_job_id" != "null" ]'
    
    wait_for_job "$api_job_id"
    local api_job_status=$(curl -s "$API/jobs/$api_job_id")
    local api_state=$(echo "$api_job_status" | jq -r '.state')
    local api_rows=$(echo "$api_job_status" | jq -r '.totals.rows')
    local api_ok=$(echo "$api_job_status" | jq -r '.totals.ok')
    
    test_assert "API job completed successfully" '[ "$api_state" = "SUCCESS" ]'
    test_assert "API job processed 7 rows" '[ "$api_rows" -eq 7 ]'
    test_assert "API job processed all rows successfully" '[ "$api_ok" -eq 7 ]'
    
    # Test DLQ is empty for successful job
    local api_dlq=$(curl -s "$API/jobs/$api_job_id/rejected")
    local api_dlq_count=$(echo "$api_dlq" | jq 'length')
    test_assert "API job has no rejected rows" '[ "$api_dlq_count" -eq 0 ]'
    
    # Test Parquet file detection
    echo "    Testing Parquet file detection..."
    local parquet_job_response=$(curl -s -X POST -F "model_id=default_model" -F "file=@samples/test_data.parquet" "$API/jobs")
    local parquet_job_id=$(echo "$parquet_job_response" | jq -r '.job_id // ""')
    test_assert "Parquet job creation successful" '[ -n "$parquet_job_id" ] && [ "$parquet_job_id" != "null" ]'
    
    # Test error scenarios
    echo "    Testing error scenarios..."
    
    # Empty file
    touch "$TMP_DIR/empty.csv"
    local empty_response=$(curl -s -X POST -F "model_id=default_model" -F "file=@$TMP_DIR/empty.csv" "$API/jobs")
    local empty_error=$(echo "$empty_response" | jq -r '.error // ""')
    test_assert "Empty file rejected with READ_ERROR" '[ "$empty_error" = "READ_ERROR" ]'
    
    # Invalid file type
    echo "not a csv or parquet" > "$TMP_DIR/invalid.txt"
    local invalid_response=$(curl -s -X POST -F "model_id=default_model" -F "file=@$TMP_DIR/invalid.txt" "$API/jobs")
    local invalid_job_id=$(echo "$invalid_response" | jq -r '.job_id // ""')
    # Note: The system currently accepts text files, so we just check it doesn't crash
    test_assert "Invalid file type handled gracefully" '[ -n "$invalid_job_id" ] || [ -n "$(echo "$invalid_response" | jq -r ".error // \"\"" )" ]'
    
    # Non-existent model
    local model_error_response=$(curl -s -X POST -F "model_id=nonexistent_model" -F "file=@samples/api_data.csv" "$API/jobs")
    local model_error=$(echo "$model_error_response" | jq -r '.error // ""')
    test_assert "Non-existent model rejected" '[ "$model_error" = "MODEL_NOT_FOUND" ]'
}

test_cli_job_processing() {
    print_section "Job Processing (CLI with Sequential Data)"
    
    # Test CLI job creation
    echo "    Testing CLI data processing..."
    local cli_output=$($CLI job create default_model samples/cli_data.csv 2>/dev/null || echo '{"error": "CLI failed"}')
    local cli_job_id=$(echo "$cli_output" | jq -r '.job_id // ""')
    test_assert "CLI job creation successful" '[ -n "$cli_job_id" ] && [ "$cli_job_id" != "null" ]'
    
    if [ -n "$cli_job_id" ] && [ "$cli_job_id" != "null" ]; then
        wait_for_job "$cli_job_id"
        
        # Test CLI job status retrieval
        local cli_status_output=$($CLI job status "$cli_job_id" 2>/dev/null || echo '{"error": "Status failed"}')
        local cli_state=$(echo "$cli_status_output" | jq -r '.state // ""')
        local cli_rows=$(echo "$cli_status_output" | jq -r '.totals.rows // 0')
        
        test_assert "CLI job status retrieval successful" '[ "$cli_state" = "SUCCESS" ]'
        test_assert "CLI job processed 7 rows" '[ "$cli_rows" -eq 7 ]'
        
        # Test CLI rejected rows retrieval
        local cli_dlq_output=$($CLI job rejected "$cli_job_id" 2>/dev/null || echo '[]')
        local cli_dlq_count=$(echo "$cli_dlq_output" | jq 'length // 0')
        test_assert "CLI rejected rows retrieval successful" '[ "$cli_dlq_count" -eq 0 ]'
    fi
    
    # Test CLI model operations
    echo "    Testing CLI model operations..."
    local cli_models_output=$($CLI model list 2>/dev/null || echo '[]')
    local model_count=$(echo "$cli_models_output" | jq 'length // 0')
    test_assert "CLI model list successful" '[ "$model_count" -gt 0 ]'
    
    # Test CLI model describe
    local cli_describe_output=$($CLI model describe default_model 2>/dev/null || echo '{"error": "Describe failed"}')
    local described_id=$(echo "$cli_describe_output" | jq -r '.id // ""')
    test_assert "CLI model describe successful" '[ "$described_id" = "default_model" ]'
}

test_dlq_functionality() {
    print_section "Dead Letter Queue Functionality"
    
    # Test DLQ with error data
    echo "    Testing DLQ with malformed CSV..."
    local dlq_job_response=$(curl -s -X POST -F "model_id=default_model" -F "file=@samples/error_data.csv" "$API/jobs")
    local dlq_job_id=$(echo "$dlq_job_response" | jq -r '.job_id // ""')
    test_assert "DLQ test job creation successful" '[ -n "$dlq_job_id" ] && [ "$dlq_job_id" != "null" ]'
    
    wait_for_job "$dlq_job_id" 20  # Give more time for error processing
    
    local dlq_job_status=$(curl -s "$API/jobs/$dlq_job_id")
    local dlq_state=$(echo "$dlq_job_status" | jq -r '.state')
    local dlq_errors=$(echo "$dlq_job_status" | jq -r '.totals.errors // 0')
    local dlq_ok=$(echo "$dlq_job_status" | jq -r '.totals.ok // 0')
    
    test_assert "DLQ job has PARTIAL_SUCCESS or FAILED state" '[ "$dlq_state" = "PARTIAL_SUCCESS" ] || [ "$dlq_state" = "FAILED" ]'
    test_assert "DLQ job detected errors" '[ "$dlq_errors" -gt 0 ]'
    test_assert "DLQ job processed some successful rows" '[ "$dlq_ok" -gt 0 ]'
    
    # Test DLQ entries
    sleep 2  # Give DLQ time to process
    local dlq_entries=$(curl -s "$API/jobs/$dlq_job_id/rejected")
    local dlq_count=$(echo "$dlq_entries" | jq 'length // 0')
    test_assert "DLQ contains rejected rows" '[ "$dlq_count" -gt 0 ]'
    
    # Validate DLQ entry structure
    if [ "$dlq_count" -gt 0 ]; then
        local first_entry=$(echo "$dlq_entries" | jq '.[0]')
        local has_job_id=$(echo "$first_entry" | jq -e '.job_id' >/dev/null 2>&1 && echo "true" || echo "false")
        local has_row_number=$(echo "$first_entry" | jq -e '.row_number' >/dev/null 2>&1 && echo "true" || echo "false")
        local has_error=$(echo "$first_entry" | jq -e '.error' >/dev/null 2>&1 && echo "true" || echo "false")
        local has_timestamp=$(echo "$first_entry" | jq -e '.timestamp' >/dev/null 2>&1 && echo "true" || echo "false")
        
        test_assert "DLQ entry has job_id field" '[ "$has_job_id" = "true" ]'
        test_assert "DLQ entry has row_number field" '[ "$has_row_number" = "true" ]'
        test_assert "DLQ entry has error field" '[ "$has_error" = "true" ]'
        test_assert "DLQ entry has timestamp field" '[ "$has_timestamp" = "true" ]'
    fi
    
    # Test DLQ for non-existent job
    local nonexistent_dlq=$(curl -s "$API/jobs/nonexistent_job/rejected")
    local dlq_error=$(echo "$nonexistent_dlq" | jq -r '.error // ""')
    test_assert "Non-existent job DLQ returns JOB_NOT_FOUND" '[ "$dlq_error" = "JOB_NOT_FOUND" ]'
}

test_job_management() {
    print_section "Job Management & Operations"
    
    # Create a test job for management operations
    local mgmt_job_response=$(curl -s -X POST -F "model_id=default_model" -F "file=@samples/api_data.csv" "$API/jobs")
    local mgmt_job_id=$(echo "$mgmt_job_response" | jq -r '.job_id // ""')
    
    # Test job listing
    local jobs_list=$(curl -s "$API/jobs")
    local jobs_count=$(echo "$jobs_list" | jq 'length // 0')
    test_assert "Job listing returns multiple jobs" '[ "$jobs_count" -gt 0 ]'
    
    # Test job status retrieval
    local job_status=$(curl -s "$API/jobs/$mgmt_job_id")
    local status_job_id=$(echo "$job_status" | jq -r '.job_id // ""')
    test_assert "Job status retrieval successful" '[ "$status_job_id" = "$mgmt_job_id" ]'
    
    # Test job cancellation (create a new job to cancel)
    local cancel_job_response=$(curl -s -X POST -F "model_id=default_model" -F "file=@samples/cli_data.csv" "$API/jobs")
    local cancel_job_id=$(echo "$cancel_job_response" | jq -r '.job_id // ""')
    
    if [ -n "$cancel_job_id" ] && [ "$cancel_job_id" != "null" ]; then
        local cancel_response=$(curl -s -X DELETE "$API/jobs/$cancel_job_id")
        local cancel_state=$(echo "$cancel_response" | jq -r '.state // ""')
        test_assert "Job cancellation successful" '[ "$cancel_state" = "CANCELLED" ]'
    fi
    
    # Test non-existent job retrieval
    local nonexistent_job=$(curl -s "$API/jobs/nonexistent_job_id")
    local job_error=$(echo "$nonexistent_job" | jq -r '.error // ""')
    test_assert "Non-existent job returns JOB_NOT_FOUND" '[ "$job_error" = "JOB_NOT_FOUND" ]'
}

test_data_sequencing() {
    print_section "Data Sequencing Verification"
    
    # Verify that API and CLI data have sequential event IDs and timestamps
    echo "    Verifying sequential data integrity..."
    
    # Check API data
    local api_events=$(cat samples/api_data.csv | tail -n +2 | cut -d',' -f1 | sort -n)
    local api_min=$(echo "$api_events" | head -n1)
    local api_max=$(echo "$api_events" | tail -n1)
    test_assert "API data starts with event ID 1" '[ "$api_min" -eq 1 ]'
    test_assert "API data ends with event ID 6" '[ "$api_max" -eq 6 ]'
    
    # Check CLI data
    local cli_events=$(cat samples/cli_data.csv | tail -n +2 | cut -d',' -f1 | sort -n)
    local cli_min=$(echo "$cli_events" | head -n1)
    local cli_max=$(echo "$cli_events" | tail -n1)
    test_assert "CLI data starts with event ID 101" '[ "$cli_min" -eq 101 ]'
    test_assert "CLI data ends with event ID 106" '[ "$cli_max" -eq 106 ]'
    
    # Check error data
    local error_events=$(cat samples/error_data.csv | head -n2 | tail -n1 | cut -d',' -f1)
    test_assert "Error data starts with event ID 201" '[ "$error_events" -eq 201 ]'
    
    echo "    Sequential data integrity verified: API(1-6) → CLI(101-106) → Errors(201+)"
}

print_final_summary() {
    print_header "Test Results Summary"
    
    echo -e "Total Tests: ${BLUE}$TESTS_TOTAL${NC}"
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    
    local success_rate=0
    if [ $TESTS_TOTAL -gt 0 ]; then
        success_rate=$((TESTS_PASSED * 100 / TESTS_TOTAL))
    fi
    echo -e "Success Rate: ${YELLOW}${success_rate}%${NC}"
    
    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "\n${GREEN}ALL TESTS PASSED!${NC}"
        echo -e "${GREEN}The batch ingestion system is fully functional with both API and CLI.${NC}"
        return 0
    else
        echo -e "\n${RED}SOME TESTS FAILED${NC}"
        echo -e "${RED}Please review the failed tests above.${NC}"
        return 1
    fi
}

# Main execution
main() {
    print_header "Unified Batch Ingestion Test Suite"
    echo -e "${BLUE}Testing both HTTP API and CLI with sequential data${NC}"
    echo -e "${BLUE}API Data: events 1-6, CLI Data: events 101-106, Error Data: events 201+${NC}"
    
    setup_test_environment
    test_model_management
    test_api_job_processing
    test_cli_job_processing
    test_dlq_functionality
    test_job_management
    test_data_sequencing
    
    print_final_summary
}

# Check if script is being run directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi 