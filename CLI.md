# Command-Line Interface (CLI)

A rich CLI is provided for interacting with the batch ingestion service. Run commands via `batch <command>` or `./batch <command>` if the project has been built locally.

Ensure the API URL is set if the server is not running at the default http://localhost:8000:

```bash
export BATCH_API_URL=http://localhost:8000
```

## Model Commands

### model list
Lists available models.

```bash
./batch model list
```

### model describe <model_id>
Shows details for a specific model.

```bash
./batch model describe <model_id>
```

### model create <model_name> <path/to/schema.json>
Creates a new model from a name and a JSON schema file.

```bash
./batch model create my_new_model ./schemas/new_model_schema.json
```

### model update <model_id> <path/to/schema.json>
Updates the schema for an existing model.

```bash
./batch model update <model_id> ./schemas/updated_schema.json
```

### model delete <model_id>
Deletes a model.

```bash
./batch model delete <model_id>
```

## Job Commands

### job list
Lists existing jobs with their status.

```bash
./batch job list
```

Sample output:

```
JOB      MODEL       STATE           TOTAL   OK      ERRORS  PROGRESS                 WAITING  PROCESSSING
-------- ----------- --------------- ------- ------- ------- ------------------------ -------  -----------
a1b2c3d4 Fraud De..  SUCCESS           1,000   1,000       0 [#################] 100%   00:01        00:02
a2b3c4d5 Image Cl..  PARTIAL_SUCCESS  20,000  19,980      20 [###############X-]  99%   00:02        00:09
a3b4c5d6 Payment G.. RUNNING          50,000  30,500      12 [##########-------]  61%   00:03        00:40
a4b5c6d7 Anomaly D.. RUNNING           5,000   2,100       0 [#######----------]  42%   00:02        00:15
a5b6c7d8 model_123.. PENDING             100       0       0 [-----------------]   0%   00:04        00:00
a6b7c8d9 Alert Log.. PENDING           7,500       0       0 [-----------------]   0%   00:04        00:00
a7b8c9d0 Sentimen .. FAILED              800       0     800 [XXXXXXXXXXXXXXXXX]   0%   00:04        00:09
a8b9c0d1 Trend Pre.. CANCELLED         2,000     853       0 [########XXXXXXXXX]  46%   00:04        00:09
a9b0c1d2 Market Pr.. SUCCESS           3,200   3,200       0 [#################] 100%   00:02        00:03
```

*(The PARTIAL_SUCCESS state and timing fields are new requirements.)*

### job create <model_id> <path/to/data.csv>
Creates a new job for the given model and data file.

```bash
./batch job create model_123 data.csv
```

Sample output:

```
job a5b6c7d8 created.
```

### job status <job_id>
Shows the status of a specific job.

```bash
./batch job status a6b7c8d9
```

Sample output:

```
JOB      MODEL       STATE           TOTAL   OK      ERRORS  PROGRESS                 WAITING  PROCESSSING
-------- ----------- --------------- ------- ------- ------- ------------------------ -------  -----------
a5b6c7d8 model_123.. PENDING             100       0       0 [-----------------]   0%   00:04        00:00
```

*(Output format matches job list.)*

### job cancel <job_id>
Cancels a job.

```bash
./batch job cancel a5b6c7d8
```

Sample output:

```
job a5b6c7d8 cancelled
```

### job rejected <job_id>
Displays rows that were rejected during processing for a specific job.

```bash
./batch job rejected a5b6c7d8
```

Sample output:

```
ROW  EVENT_ID COLUMN      TYPE        ERROR               OBSERVED         MESSAGE
---- -------- ----------- ----------- ------------------- ---------------- ------------------------------------------------------------------------------------
1    1001abcd timestamp   TIMESTAMP   MISSING_COLUMN                       The required column 'timestamp' is missing. Please include a valid timestamp column.
2             event_id    STRING      MISSING_COLUMN                       The required column 'event_id' is missing. Each event must have a unique event_id.
3    1003cdef timestamp   TIMESTAMP   INVALID_TIMESTAMP   "yesterday"      The 'timestamp' value is invalid. Please provide a 32-bit Unix timestamp (integer).
4    1004def0 event_id    STRING      INVALID_EVENT_ID                     The 'event_id' value is invalid. Provide a non-empty string or integer identifier.
5    1005ef01 attr_float  FLOAT       NOT_A_FLOAT         "abc"            The value for 'attr_float' is not a float. Provide a numeric value (e.g. 3.14).
6    1006f012 attr_int    INTEGER     NOT_AN_INTEGER      "3.14"           The value for 'attr_int' is not an integer. Provide a whole number (e.g. 42).
7    1007abcd attr_bool   BOOLEAN     NOT_A_BOOLEAN       "maybe"          The value for 'attr_bool' is not a boolean. Use true, false, 1, or 0.
8    1008bcde attr_str    STRING      NOT_A_STRING        12345            The value for 'attr_str' is not a string. Provide a text value.
9    1009cdef attr_cat    CATEGORY    NOT_A_CATEGORY      "CA"             The value for 'attr_cat' is not a category. Provide a valid string category.
10   1010def0 attr_time   TIMESTAMP   NOT_A_TIMESTAMP     "2023-01-01"     The value for 'attr_time' is not a timestamp. Use a 32-bit Unix timestamp.
11   1011ef01 attr_vec    VECTOR      NOT_A_VECTOR        [1, "a", 3]      The value for 'attr_vec' is not a vector. Provide a list of numeric values.
12   1012f012 extra_col   STRING      EXTRA_COLUMN        "foo"            The column 'extra_col' is not in the schema. Remove or update your schema.
13   1013abcd attr_req    STRING      NULL_VALUE                           The column 'attr_req' is null or blank. Provide a valid value for this column.
14   1014bcde attr_x      FLOAT       UNSUPPORTED_TYPE    3.14             The column 'attr_x' uses unsupported type 'FLOAT'. Use a supported type.
``` 