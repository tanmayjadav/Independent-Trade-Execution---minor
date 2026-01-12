#!/usr/bin/env python3
"""
Launch script for trading system.
Designed to be called by cron/scheduled tasks.

Cron example (Linux/Mac):
    # Run 5 minutes before market open (09:10) every weekday
    10 9 * * 1-5 cd /path/to/project && python launch.py

Windows Task Scheduler:
    - Create task to run daily at 09:10
    - Action: python.exe
    - Arguments: launch.py
    - Start in: C:\path\to\project
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from main import main

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

