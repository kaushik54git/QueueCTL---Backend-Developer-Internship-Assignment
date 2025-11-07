import subprocess
import sys

def run_command(cmd):
    full_cmd = [sys.executable, "queuectl.py"] + cmd
    result = subprocess.run(full_cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr

def demo_basic_workflow():
    run_command(["enqueue", "echo 'Hello from QueueCTL!'"])
    run_command(["enqueue", "echo 'QueueCTL test file' > /tmp/queuectl_test.txt"])
    run_command(["enqueue", "curl -s https://api.github.com/zen"])
    run_command(["status"])
    run_command(["list", "--state", "pending"])

def demo_retry_mechanism():
    run_command(["config", "set", "max-retries", "3"])
    run_command(["config", "set", "backoff-base", "2"])
    run_command(["enqueue", "false", "--id", "demo-fail-job"])

def demo_multiple_workers():
    for i in range(6):
        run_command(["enqueue", f"sleep 2 && echo 'Processed job {i+1}'"])

def demo_dead_letter_queue():
    run_command(["enqueue", "nonexistent_command_12345"])
    run_command(["enqueue", "/etc/passwd"])

def demo_configuration():
    configs = ["max-retries", "backoff-base", "db-path"]
def demo_multiple_workers():
    """Demonstrate parallel job processing"""
    for i in range(6):
        run_command([
            "enqueue", f"sleep 2 && echo 'Processed job {i+1}'"
        ])

def demo_dead_letter_queue():
    """Demonstrate Dead Letter Queue functionality"""
    run_command([
        "enqueue", "nonexistent_command_12345"
    ])
    run_command([
        "enqueue", "/etc/passwd"
    ])

def demo_configuration():
    """Demonstrate configuration management"""
    configs = ["max-retries", "backoff-base", "db-path"]
    for config in configs:
        run_command(["config", "get", config])
    run_command(["config", "set", "max-retries", "5"])
    run_command(["config", "set", "backoff-base", "3"])
    run_command(["config", "get", "max-retries"])
    run_command(["config", "get", "backoff-base"])

def demo_real_world_scenarios():
    """Demonstrate real-world usage scenarios"""
    run_command(["enqueue", "python3 process_data.py --input data.csv"])
    run_command(["enqueue", "python3 generate_report.py --data processed.csv"])
    run_command(["enqueue", "python3 send_email.py --report report.pdf"])
    run_command(["enqueue", "apt update && apt upgrade -y"])
    run_command(["enqueue", "systemctl restart nginx"])
    run_command(["enqueue", "df -h > /tmp/disk_usage.txt"])
    run_command(["enqueue", "git pull origin main"])
    run_command(["enqueue", "npm test"])
    run_command(["enqueue", "npm run build"])
    run_command(["enqueue", "curl -s https://api.status.io/1.0/status/123"])
    run_command(["enqueue", "python3 check_server_health.py"])
    run_command(["enqueue", "python3 send_slack_notification.py"])

def demo_monitoring_and_debugging():
    """Demonstrate monitoring and debugging features"""
    run_command(["status"])
    run_command(["list", "--state", "failed"])
    run_command(["list", "--state", "processing"])
    run_command(["list", "--state", "completed"])
    run_command(["dlq", "list"])
    run_command(["config", "set", "log-level", "DEBUG"])

demos = [
    demo_basic_workflow,
    demo_retry_mechanism,
    demo_multiple_workers,
    demo_dead_letter_queue,
    demo_configuration,
    demo_real_world_scenarios,
    demo_monitoring_and_debugging
]
for demo in demos:
    try:
        demo()
    except Exception as e:
        print(f"Error running demo {demo.__name__}: {e}")
