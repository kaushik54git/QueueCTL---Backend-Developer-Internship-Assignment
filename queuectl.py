import json
import sqlite3
import subprocess
import threading
import time
import argparse
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import logging

class JobState(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    id: str
    command: str
    state: JobState
    attempts: int
    max_retries: int
    created_at: str
    updated_at: str
    last_error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'command': self.command,
            'state': self.state.value,
            'attempts': self.attempts,
            'max_retries': self.max_retries,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'last_error': self.last_error
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        return cls(
            id=data['id'],
            command=data['command'],
            state=JobState(data['state']),
            attempts=data['attempts'],
            max_retries=data['max_retries'],
            created_at=data['created_at'],
            updated_at=data['updated_at'],
            last_error=data.get('last_error')
        )


class QueueStorage:
    def __init__(self, db_path: str = "queuectl.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT
                )
            ''')
    
    def save_job(self, job: Job):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO jobs 
                (id, command, state, attempts, max_retries, created_at, updated_at, last_error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job.id, job.command, job.state.value, job.attempts,
                job.max_retries, job.created_at, job.updated_at, job.last_error
            ))
    
    def get_job(self, job_id: str) -> Optional[Job]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT * FROM jobs WHERE id = ?', (job_id,)
            )
            row = cursor.fetchone()
            if row:
                return Job.from_dict({
                    'id': row[0], 'command': row[1], 'state': row[2],
                    'attempts': row[3], 'max_retries': row[4],
                    'created_at': row[5], 'updated_at': row[6],
                    'last_error': row[7]
                })
            return None
    
    def get_jobs_by_state(self, state: JobState) -> List[Job]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT * FROM jobs WHERE state = ?', (state.value,)
            )
            return [Job.from_dict({
                'id': row[0], 'command': row[1], 'state': row[2],
                'attempts': row[3], 'max_retries': row[4],
                'created_at': row[5], 'updated_at': row[6],
                'last_error': row[7]
            }) for row in cursor.fetchall()]
    
    def get_next_pending_job(self) -> Optional[Job]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('BEGIN IMMEDIATE')
            cursor = conn.execute(
                'SELECT * FROM jobs WHERE state = ? ORDER BY created_at LIMIT 1',
                (JobState.PENDING.value,)
            )
            row = cursor.fetchone()
            if row:
                job = Job.from_dict({
                    'id': row[0], 'command': row[1], 'state': row[2],
                    'attempts': row[3], 'max_retries': row[4],
                    'created_at': row[5], 'updated_at': row[6],
                    'last_error': row[7]
                })
                conn.execute(
                    'UPDATE jobs SET state = ?, updated_at = ? WHERE id = ?',
                    (JobState.PROCESSING.value, datetime.utcnow().isoformat(), job.id)
                )
                return job
            return None
    
    def get_job_stats(self) -> Dict[str, int]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT state, COUNT(*) FROM jobs GROUP BY state'
            )
            stats = {state.value: 0 for state in JobState}
            for row in cursor.fetchall():
                stats[row[0]] = row[1]
            return stats


class JobQueue:    
    def __init__(self, storage: QueueStorage):
        self.storage = storage
        self.logger = self._setup_logging()
    
    def _setup_logging(self) -> logging.Logger:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger('QueueCTL')
    
    def enqueue(self, command: str, job_id: Optional[str] = None, 
                max_retries: int = 3) -> str:
        """Add a new job to the queue"""
        if job_id is None:
            job_id = str(uuid.uuid4())
        
        now = datetime.utcnow().isoformat()
        job = Job(
            id=job_id,
            command=command,
            state=JobState.PENDING,
            attempts=0,
            max_retries=max_retries,
            created_at=now,
            updated_at=now
        )
        
        self.storage.save_job(job)
        self.logger.info(f"Enqueued job {job_id}: {command}")
        return job_id
    
    def process_job(self, job: Job) -> bool:
        """Execute a job and return success status"""
        try:
            self.logger.info(f"Executing job {job.id}: {job.command}")
            
            # Execute the command
            result = subprocess.run(
                job.command, shell=True, capture_output=True, text=True, timeout=300
            )
            
            if result.returncode == 0:
                job.state = JobState.COMPLETED
                job.updated_at = datetime.utcnow().isoformat()
                self.storage.save_job(job)
                self.logger.info(f"Job {job.id} completed successfully")
                return True
            else:
                job.last_error = result.stderr or f"Exit code: {result.returncode}"
                job.attempts += 1
                
                if job.attempts >= job.max_retries:
                    job.state = JobState.DEAD
                    self.logger.error(f"Job {job.id} moved to DLQ after {job.attempts} attempts")
                else:
                    job.state = JobState.FAILED
                    self.logger.warning(f"Job {job.id} failed (attempt {job.attempts}/{job.max_retries})")
                
                job.updated_at = datetime.utcnow().isoformat()
                self.storage.save_job(job)
                return False
                
        except subprocess.TimeoutExpired:
            job.last_error = "Job execution timeout"
            job.attempts += 1
            job.state = JobState.FAILED if job.attempts < job.max_retries else JobState.DEAD
            job.updated_at = datetime.utcnow().isoformat()
            self.storage.save_job(job)
            return False
        except Exception as e:
            job.last_error = str(e)
            job.attempts += 1
            job.state = JobState.FAILED if job.attempts < job.max_retries else JobState.DEAD
            job.updated_at = datetime.utcnow().isoformat()
            self.storage.save_job(job)
            self.logger.error(f"Error processing job {job.id}: {e}")
            return False


class Worker:    
    def __init__(self, queue: JobQueue, worker_id: str):
        self.queue = queue
        self.worker_id = worker_id
        self.running = False
        self.logger = logging.getLogger(f'Worker-{worker_id}')
    
    def start(self):
        """Start the worker process"""
        self.running = True
        self.logger.info(f"Worker {self.worker_id} started")
        
        while self.running:
            try:
                # Get next pending job
                job = self.queue.storage.get_next_pending_job()
                
                if job:
                    # Process the job
                    self.queue.process_job(job)
                    
                    # Exponential backoff for failed jobs
                    if job.state == JobState.FAILED:
                        delay = 2 ** job.attempts
                        self.logger.info(f"Waiting {delay}s before retrying job {job.id}")
                        time.sleep(delay)
                else:
                    # No jobs available, wait a bit
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"Error in worker {self.worker_id}: {e}")
                time.sleep(1)
    
    def stop(self):
        """Stop the worker gracefully"""
        self.running = False
        self.logger.info(f"Worker {self.worker_id} stopped")


class WorkerManager:
    """Manages multiple worker processes"""
    
    def __init__(self, queue: JobQueue):
        self.queue = queue
        self.workers: List[threading.Thread] = []
        self.worker_instances: List[Worker] = []
        self.logger = logging.getLogger('WorkerManager')
    
    def start_workers(self, count: int = 1):
        """Start multiple worker processes"""
        for i in range(count):
            worker_id = f"worker-{i+1}"
            worker = Worker(self.queue, worker_id)
            self.worker_instances.append(worker)
            
            thread = threading.Thread(target=worker.start)
            thread.daemon = True
            thread.start()
            self.workers.append(thread)
            
            self.logger.info(f"Started {worker_id}")
    
    def stop_workers(self):
        """Stop all workers gracefully"""
        self.logger.info("Stopping all workers...")
        
        for worker in self.worker_instances:
            worker.stop()
        
        # Wait for all threads to finish
        for thread in self.workers:
            thread.join(timeout=10)
        
        self.workers.clear()
        self.worker_instances.clear()
        self.logger.info("All workers stopped")


class ConfigManager:
    """Configuration management for QueueCTL"""
    
    def __init__(self, config_file: str = "queuectl_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        default_config = {
            'max_retries': 3,
            'backoff_base': 2,
            'db_path': 'queuectl.db',
            'log_level': 'INFO'
        }
        
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                # Merge with defaults
                return {**default_config, **config}
        except Exception as e:
            print(f"Error loading config: {e}")
        
        return default_config
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def get(self, key: str) -> Any:
        """Get configuration value"""
        key_mapping = {
            'max-retries': 'max_retries',
            'backoff-base': 'backoff_base',
            'db-path': 'db_path',
            'log-level': 'log_level'
        }
        actual_key = key_mapping.get(key, key)
        return self.config.get(actual_key)
    
    def set(self, key: str, value: Any):
        """Set configuration value"""
        self.config[key] = value
        self.save_config()


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='QueueCTL - Background Job Queue System')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Config commands
    config_parser = subparsers.add_parser('config', help='Configuration management')
    config_subparsers = config_parser.add_subparsers(dest='config_action')
    
    config_set_parser = config_subparsers.add_parser('set', help='Set configuration value')
    config_set_parser.add_argument('key', help='Configuration key')
    config_set_parser.add_argument('value', help='Configuration value')
    
    config_get_parser = config_subparsers.add_parser('get', help='Get configuration value')
    config_get_parser.add_argument('key', help='Configuration key')
    
    # Enqueue command
    enqueue_parser = subparsers.add_parser('enqueue', help='Add a job to the queue')
    enqueue_parser.add_argument('job_command', help='Command to execute')
    enqueue_parser.add_argument('--id', help='Job ID (auto-generated if not specified)')
    enqueue_parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')
    
    # Worker commands
    worker_parser = subparsers.add_parser('worker', help='Worker management')
    worker_subparsers = worker_parser.add_subparsers(dest='worker_action')
    
    worker_start_parser = worker_subparsers.add_parser('start', help='Start workers')
    worker_start_parser.add_argument('--count', type=int, default=1, help='Number of workers to start')
    
    worker_stop_parser = worker_subparsers.add_parser('stop', help='Stop workers')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    
    # List jobs command
    list_parser = subparsers.add_parser('list', help='List jobs')
    list_parser.add_argument('--state', choices=['pending', 'processing', 'completed', 'failed', 'dead'],
                           help='Filter by job state')
    
    # DLQ commands
    dlq_parser = subparsers.add_parser('dlq', help='Dead Letter Queue management')
    dlq_subparsers = dlq_parser.add_subparsers(dest='dlq_action')
    
    dlq_list_parser = dlq_subparsers.add_parser('list', help='List DLQ jobs')
    dlq_retry_parser = dlq_subparsers.add_parser('retry', help='Retry a DLQ job')
    dlq_retry_parser.add_argument('job_id', help='Job ID to retry')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize components
    config = ConfigManager()
    storage = QueueStorage(config.get('db_path'))
    queue = JobQueue(storage)
    

    
    if args.command == 'config':
        if args.config_action == 'set':
            config.set(args.key, args.value)
            print(f"Set {args.key} = {args.value}")
        elif args.config_action == 'get':
            value = config.get(args.key)
            print(f"{args.key} = {value}")
    
    elif args.command == 'enqueue':
        job_id = queue.enqueue(args.job_command, args.id, args.max_retries)
        print(f"Enqueued job: {job_id}")
    
    elif args.command == 'worker':
        if args.worker_action == 'start':
            manager = WorkerManager(queue)
            manager.start_workers(args.count)
            print(f"Started {args.count} worker(s)")
            
            # Keep main thread alive
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nShutting down workers...")
                manager.stop_workers()
        
        elif args.worker_action == 'stop':
            # This would need inter-process communication in a real implementation
            print("Worker stop functionality requires process management")
    
    elif args.command == 'status':
        stats = storage.get_job_stats()
        print("QueueCTL Status:")
        print("-" * 40)
        for state, count in stats.items():
            print(f"{state.capitalize()}: {count}")
        print(f"Total: {sum(stats.values())}")
    
    elif args.command == 'list':
        if args.state:
            state = JobState(args.state)
            jobs = storage.get_jobs_by_state(state)
        else:
            jobs = []
            for state in JobState:
                jobs.extend(storage.get_jobs_by_state(state))
        
        if not jobs:
            print("No jobs found")
            return
        
        print(f"{'ID':<36} {'State':<12} {'Attempts':<10} {'Command':<30}")
        print("-" * 90)
        for job in jobs:
            print(f"{job.id:<36} {job.state.value:<12} {job.attempts}/{job.max_retries:<8} {job.command:<30}")
    
    elif args.command == 'dlq':
        if args.dlq_action == 'list':
            dead_jobs = storage.get_jobs_by_state(JobState.DEAD)
            if not dead_jobs:
                print("No jobs in DLQ")
                return
            
            print("Dead Letter Queue:")
            print(f"{'ID':<36} {'Attempts':<10} {'Last Error':<40}")
            print("-" * 90)
            for job in dead_jobs:
                error = job.last_error or "No error message"
                print(f"{job.id:<36} {job.attempts}/{job.max_retries:<8} {error:<40}")
        
        elif args.dlq_action == 'retry':
            job = storage.get_job(args.job_id)
            if not job:
                print(f"Job {args.job_id} not found")
                return
            
            if job.state != JobState.DEAD:
                print(f"Job {args.job_id} is not in DLQ (state: {job.state.value})")
                return
            
            # Reset job state
            job.state = JobState.PENDING
            job.attempts = 0
            job.last_error = None
            job.updated_at = datetime.utcnow().isoformat()
            storage.save_job(job)
            
            print(f"Retried job {args.job_id} (moved back to pending queue)")


if __name__ == '__main__':
    main()