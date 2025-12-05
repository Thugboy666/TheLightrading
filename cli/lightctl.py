import os
import subprocess
import sys
from pathlib import Path

from core.config import settings

ROOT = Path(__file__).resolve().parent.parent


def start_api():
    env = {
        **os.environ,
        "THELIGHT_API_HOST": settings.API_HOST,
        "THELIGHT_API_PORT": str(settings.API_PORT),
    }
    subprocess.Popen(
        [sys.executable, "start.py"],
        cwd=str(ROOT),
        env=env,
    )

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m cli.lightctl [start]")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "start":
        start_api()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)

if __name__ == "__main__":
    main()
