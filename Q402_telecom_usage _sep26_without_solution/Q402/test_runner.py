import os
import pytest

LOG_FILE = "test_report.log"

def _read_log():
    if not os.path.exists(LOG_FILE):
        return ""
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        return f.read()

def _count_results(log_text: str):
    passed = sum(1 for line in log_text.splitlines() if line.startswith("[PASS] Visible Test Case "))
    failed = sum(1 for line in log_text.splitlines() if line.startswith("[FAIL] Visible Test Case "))
    return passed, failed

if __name__ == "__main__":
    pytest.main([
        "-p", "no:terminal",
        "test_configurable_functions.py",
        "-s"
    ])

    log_text = _read_log()
    if log_text:
        print(log_text.rstrip())

    p, f = _count_results(log_text)
    print(f"Summary: {p} Passed, {f} Failed")
