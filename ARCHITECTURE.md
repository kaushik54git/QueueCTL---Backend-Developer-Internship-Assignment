# QueueCTL Architecture

## Overview

QueueCTL is a command-line interface (CLI)-based background job queue system in Python, which is equipped with features such as reliable processing, automatic retries, worker control, and storage that lasts beyond the execution time.

## Main Components

*Job Model  (Job class): A dataclass reflecting the job pattern (command, state, attempts, retries, etc.) and managing serialization.

*Storage Layer  (QueueStorage class): Employs SQLite for jobs that can be stored persistently and concurrently. It performs atomic state changes, job locking  (get_next_pending_job ), and gathering of statistics.

*Queue Manager  (JobQueue class): It is in charge of the work stages. It can run the command as a subprocess, use exponential backoff to retry, and in case of error, the affected part can be logged.

*Worker System  (Worker / WorkerManager ): The WorkerManager is the manager of the Worker threads. Every worker calls the queue, executes the work (through JobQueue ), and deals with shutdowns that are done in a friendly manner.

*CLI Interface  (main() ): Command parsing for the user interaction with enqueue , worker start/stop , status , list , dlq , and config .

*Configuration  (ConfigManager ) : The JSON file which is used for system configuration is managed by ConfigManager .

## Data Flow

### Job Enqueue Flow
Command from the user interface → Argument Parser → JobQueue.enqueue() → QueueStorage.save_job() → SQLite

### Job Processing Flow

Worker.poll() → QueueStorage.get_next_pending_job() → JobQueue.process_job() → subprocess.run() → Update job state → QueueStorage.save_job()


## Database and Concurrency
The application is based on one SQLite jobs table with indexes on state and created_at for speed.

Concurrency is a database-level feature here. Workers performing atomic BEGIN IMMEDIATE transactions locate and lock the next pending job, and then instantly change the job's state to PROCESSING . This means other workers won't be able to take the same job. Worker threads are different, and thus they don't have any shared state.

## Error Handling
*Errors in job execution (e.g., timeouts, lack of permissions) are recorded.

*In the case of failure, jobs are repeated by means of exponential backoff (for instance, backoff_base  attempts ).

*Once max_retries has been reached, the state of the job is set to DEAD , and the job is transferred to the Dead Letter Queue (DLQ) .

## System Design Considerations
*Performance: Indexed queries and an exponential backoff technique have been used to optimize the model and reduce the load caused by the database polling.

*Security: The sqlite database is protected by the file system permissions. The command execution (through shell=True ) is a little risky but the timeouts are there to protect against hanging processes.

*Scalability: Multiple stateless workers can be run to horizontally scale the system that's using QueueCTL. The scalability will, however, be limited by the single SQLite database's concurrency.

*Monitoring: System events for jobs and real-time CLI commands  (status , list ) can be used for tracking and inspection.

*Deployment: Queues can either be run locally in one process or as multiple containers (e.g., Docker) where workers utilize the volume that is shared for the SQLite ​‍​‌‍​‍‌​‍​‌‍​‍‌database.