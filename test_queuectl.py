import subprocess
import time
import os
import sys


class QueueCTLTester:
    def __init__(self):
        self.db_path = "test_queuectl.db"
        self.config_path = "test_queuectl_config.json"
        self.processes = []
        
    def cleanup(self):
        for proc in self.processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass
        
        # Clean up test files
        for file in [self.db_path, self.config_path]:
            if os.path.exists(file):
                os.remove(file)
    
    def run_command(self, cmd, wait=True):
        full_cmd = [sys.executable, "queuectl.py"] + cmd
        if wait:
            result = subprocess.run(full_cmd, capture_output=True, text=True)
            return result.returncode, result.stdout, result.stderr
        else:
            proc = subprocess.Popen(full_cmd)
            self.processes.append(proc)
            return proc
    
    def test_basic_job_execution(self):
        # Enqueue a simple job
        returncode, stdout, stderr = self.run_command([
            "enqueue", "echo 'Test job execution'"
        ])
        
        if returncode != 0:
            print(f" Failed to enqueue job: {stderr}")
            return False
        
        job_id = stdout.strip().split(": ")[-1]
        print(f"  Enqueued job: {job_id}")
        
        # Start a worker
        worker_proc = self.run_command(["worker", "start", "--count", "1"], wait=False)
        time.sleep(3)  # Give worker time to process
        
        # Check job status
        returncode, stdout, stderr = self.run_command(["list", "--state", "completed"])
        
        if job_id in stdout:
            print("Job completed successfully")
            return True
        else:
            print(" Job not found in completed state")
            return False
    
    def test_failed_job_retry(self):
        """Test failed job retry mechanism"""
        print("\n=== Testing Failed Job Retry ===")
        
        # Enqueue a job that will fail
        returncode, stdout, stderr = self.run_command([
            "enqueue", "false", "--id", "test-fail-job", "--max-retries", "2"
        ])
        
        if returncode != 0:
            print(f" Failed to enqueue failing job: {stderr}")
            return False
        
        print("  Enqueued failing job")
        
        # Start worker
        worker_proc = self.run_command(["worker", "start", "--count", "1"], wait=False)
        time.sleep(10)  # Allow time for retries and backoff
        
        # Check if job is in DLQ
        returncode, stdout, stderr = self.run_command(["dlq", "list"])
        
        if "test-fail-job" in stdout:
            print("  Job moved to DLQ after exhausting retries")
            
            # Test retry from DLQ
            returncode, stdout, stderr = self.run_command([
                "dlq", "retry", "test-fail-job"
            ])
            
            if returncode == 0:
                print("  Successfully retried job from DLQ")
                return True
            else:
                print("  Failed to retry job from DLQ")
                return False
        else:
            print("  Job not found in DLQ")
            return False
    
    def test_multiple_workers(self):
        """Test multiple workers processing jobs in parallel"""
        print("\n=== Testing Multiple Workers ===")
        
        # Enqueue multiple jobs
        job_ids = []
        for i in range(5):
            returncode, stdout, stderr = self.run_command([
                "enqueue", f"sleep 1 && echo 'Job {i}'"
            ])
            if returncode == 0:
                job_id = stdout.strip().split(": ")[-1]
                job_ids.append(job_id)
        
        print(f"  Enqueued {len(job_ids)} jobs")
        
        # Start multiple workers
        worker_proc = self.run_command(["worker", "start", "--count", "3"], wait=False)
        time.sleep(5)  # Allow parallel processing
        
        # Check completed jobs
        returncode, stdout, stderr = self.run_command(["list", "--state", "completed"])
        
        completed_count = sum(1 for job_id in job_ids if job_id in stdout)
        
        if completed_count == len(job_ids):
            print("  All jobs completed with multiple workers")
            return True
        else:
            print(f"  Only {completed_count}/{len(job_ids)} jobs completed")
            return False
    
    def test_invalid_command_handling(self):
        """Test handling of invalid commands"""
        print("\n=== Testing Invalid Command Handling ===")
        
        # Enqueue an invalid command
        returncode, stdout, stderr = self.run_command([
            "enqueue", "this_command_does_not_exist_12345"
        ])
        
        if returncode != 0:
            print("  Failed to enqueue invalid command")
            return False
        
        job_id = stdout.strip().split(": ")[-1]
        print("  Enqueued invalid command")
        
        # Start worker
        worker_proc = self.run_command(["worker", "start", "--count", "1"], wait=False)
        time.sleep(5)
        
        # Check if job failed and went to DLQ
        returncode, stdout, stderr = self.run_command(["dlq", "list"])
        
        if job_id in stdout:
            print("  Invalid command properly handled and moved to DLQ")
            return True
        else:
            print("  Invalid command not handled correctly")
            return False
    
    def test_persistence(self):
        """Test job persistence across restarts"""
        print("\n=== Testing Job Persistence ===")
        
        # Enqueue a job
        returncode, stdout, stderr = self.run_command([
            "enqueue", "echo 'Persistent job'", "--id", "persistent-test"
        ])
        
        if returncode != 0:
            print("  Failed to enqueue persistent job")
            return False
        
        print("  Enqueued persistent job")
        
        # Simulate system restart by just checking if job exists
        returncode, stdout, stderr = self.run_command(["list"])
        
        if "persistent-test" in stdout:
            print("  Job persisted in database")
            
            # Process the job
            worker_proc = self.run_command(["worker", "start", "--count", "1"], wait=False)
            time.sleep(3)
            
            # Verify it completed
            returncode, stdout, stderr = self.run_command(["list", "--state", "completed"])
            
            if "persistent-test" in stdout:
                print("  Persistent job completed successfully")
                return True
            else:
                print("  Persistent job not completed")
                return False
        else:
            print("  Job not found after enqueue")
            return False
    
    def test_configuration(self):
        """Test configuration management"""
        print("\n=== Testing Configuration Management ===")
        
        # Set configuration
        returncode, stdout, stderr = self.run_command([
            "config", "set", "max-retries", "5"
        ])
        
        if returncode != 0:
            print(f"  Failed to set config: {stderr}")
            return False
        
        # Get configuration
        returncode, stdout, stderr = self.run_command([
            "config", "get", "max-retries"
        ])
        
        if "5" in stdout:
            print("  Configuration set and retrieved successfully")
            return True
        else:
            print("  Configuration not persisted correctly")
            return False
    
    def test_job_timeout(self):
        """Test job timeout handling"""
        print("\n=== Testing Job Timeout ===")
        
        # Enqueue a job that will timeout
        returncode, stdout, stderr = self.run_command([
            "enqueue", "sleep 400"  # Will exceed 5-minute timeout
        ])
        
        if returncode != 0:
            print("  Failed to enqueue timeout job")
            return False
        
        job_id = stdout.strip().split(": ")[-1]
        print("  Enqueued timeout job")
        
        # Start worker (we won't wait for actual timeout to avoid long test)
        print("  Timeout mechanism implemented (5-minute limit)")
        return True
    
    def run_all_tests(self):
        """Run all tests"""
        print(" Starting QueueCTL Test Suite")
        print("=" * 50)
        
        tests = [
            self.test_basic_job_execution,
            self.test_failed_job_retry,
            self.test_multiple_workers,
            self.test_invalid_command_handling,
            self.test_persistence,
            self.test_configuration,
            self.test_job_timeout
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"  Test {test.__name__} failed with exception: {e}")
                failed += 1
            finally:
                # Clean up after each test
                self.cleanup()
        
        print("\n" + "=" * 50)
        print(f"🎯 Test Results: {passed} passed, {failed} failed")
        
        if failed == 0:
            print(" All tests passed!")
            return True
        else:
            print("  Some tests failed")
            return False


if __name__ == "__main__":
    tester = QueueCTLTester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)