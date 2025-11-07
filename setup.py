#!/usr/bin/env python3
import os
from pathlib import Path

def main():
    scripts = ["queuectl.py", "test_queuectl.py", "examples.py"]
    for script in scripts:
        if os.path.exists(script):
            os.chmod(script, 0o755)
    examples_dir = Path("examples")
    examples_dir.mkdir(exist_ok=True)
    sample_jobs = [
        ("simple_echo.sh", "#!/bin/bash\necho 'Hello from QueueCTL!'"),
        ("create_file.sh", "#!/bin/bash\necho 'QueueCTL created this file' > /tmp/queuectl_demo.txt"),
        ("api_call.py", "#!/usr/bin/env python3\nimport requests\nresponse = requests.get('https://api.github.com/zen')\nprint(f'API Response: {response.text}')"),
    ]
    for filename, content in sample_jobs:
        filepath = examples_dir / filename
        with open(filepath, 'w') as f:
            f.write(content)
        os.chmod(filepath, 0o755)
    quick_start = """# Quick Start Guide

## Basic Usage

1. **Enqueue your first job:**
   ```bash
   ./queuectl.py enqueue "echo 'Hello World'"
   ```

2. **Start a worker:**
   ```bash
   ./queuectl.py worker start
   ```

3. **Check status:**
   ```bash
   ./queuectl.py status
   ```

4. **Run examples:**
   ```bash
   # Enqueue example jobs
   ./queuectl.py enqueue "./examples/simple_echo.sh"
   ./queuectl.py enqueue "./examples/create_file.sh"
   ./queuectl.py enqueue "./examples/api_call.py"
   ```

## Testing

Run the test suite:
```bash
./test_queuectl.py
```

## Development

The project structure:
- `queuectl.py` - Main application
- `test_queuectl.py` - Test suite
- `examples/` - Example job scripts
- `README.md` - Complete documentation
"""
    with open("QUICK_START.md", 'w') as f:
        f.write(quick_start)

if __name__ == "__main__":
    main()
