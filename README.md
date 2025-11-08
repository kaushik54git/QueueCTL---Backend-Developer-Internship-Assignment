# **QueueCTL**

A CLI-based background job queue system built in Python. It features persistent SQLite storage, multi-worker support, automatic retries with exponential backoff, and a Dead Letter Queue (DLQ).

# **Working Demo**

<iframe width="560" height="315" src="https://github.com/kaushik54git/QueueCTL---Backend-Developer-Internship-Assignment/blob/main/demo.mp4" frameborder="0" allowfullscreen></iframe>

## **Features**

* **Job Queue Management**: Enqueue and manage background jobs.  
* **Multi-Worker Support**: Run multiple worker processes in parallel.  
* **Automatic Retry**: Failed jobs retry with exponential backoff.  
* **Dead Letter Queue**: Permanently failed jobs are moved to a DLQ.  
* **Persistent Storage**: Jobs survive system restarts using SQLite.  
* **CLI Interface**: A complete command-line interface.  
* **Configuration Management**: Configurable retry count and backoff.  
* **Graceful Shutdown**: Workers finish current jobs before stopping.

## **Installation**

### **Prerequisites**

* Python 3.7 or higher  
* SQLite3 (usually included with Python)

## **Quick Start**

\# 1\. Configure the system's max retries  
python \-m queuectl config set max-retries 5

\# 2\. Enqueue a job  
python \-m queuectl enqueue "sleep 5 && echo 'Job done\!'" \--id job-1

\# 3\. Start workers in one terminal  
python \-m queuectl worker start \--count 2

\# 4\. In another terminal, check the status  
python \-m queuectl status  
\# Output:  
\# QueueCTL Status:  
\# pending: 0  
\# processing: 1  
\# ...

\# 5\. List completed jobs  
python \-m queuectl list \--state completed

## **Command Reference**

### **Job Management**

\# Enqueue a new job  
python \-m queuectl enqueue "echo 'Hello World'"

\# Enqueue with a custom ID and retries  
python \-m queuectl enqueue "sleep 10" \--id job-123 \--max-retries 5

\# List all jobs  
python \-m queuectl list

\# List jobs by state (pending, processing, completed, failed, dead)  
python \-m queuectl list \--state pending

### **Worker & Status**

\# Start a single worker (runs until Ctrl+C)  
python \-m queuectl worker start

\# Start 3 workers in parallel  
python \-m queuectl worker start \--count 3

\# Get a system-wide status summary  
python \-m queuectl status

### **Dead Letter Queue (DLQ)**

\# List all permanently failed jobs  
python \-m queuectl dlq list

\# Retry a specific job from the DLQ  
python \-m queuectl dlq retry \<job-id\>

### **Configuration**

\# Set a config value  
python \-m queuectl config set max-retries 5  
python \-m queuectl config set backoff-base 3

\# Get a config value  
python \-m queuectl config get db-path

## **Job Lifecycle**

Jobs move through the following states:

1. **PENDING**: Queued, waiting for a worker.  
2. **PROCESSING**: Actively being run by a worker.  
3. **COMPLETED**: Successfully finished.  
4. **FAILED**: Failed, but will be retried with exponential backoff.  
5. **DEAD**: Permanently failed (max retries exceeded) and moved to the DLQ.

## **Configuration**

Settings are managed in queuectl\_config.json.

* max\_retries: Default retries for a new job.  
* backoff\_base: Base for exponential backoff (e.g., backoff\_base ^ attempts).  
* db\_path: Path to the SQLite database file.  
* log\_level: (DEBUG, INFO, WARNING, ERROR).

## **Testing**

Run the included test script:

chmod \+x test\_queuectl.py  
./test\_queuectl.py

## **Troubleshooting**

* **"Database locked" errors**: Check for other running queuectl processes or zombie workers.  
* **Jobs not processing**: Ensure workers are running (python \-m queuectl worker start).  
* **Enable Debug Logs**: python \-m queuectl config set log-level DEBUG

