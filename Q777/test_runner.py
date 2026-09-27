import os
import sys
import pytest

LOG_FILE = "test_report.log"

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

def validate_python_versions():
    """
    Ensure PySpark uses the same Python executable.
    """
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

def _count_results_from_log() -> tuple[int, int]:
    passed = failed = 0
    if not os.path.exists(LOG_FILE):
        return (0, 0)
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s.startswith("[PASS]"):
                passed += 1
            elif s.startswith("[FAIL]"):
                failed += 1
    return (passed, failed)

def print_log_results():
    if not os.path.exists(LOG_FILE):
        print("test_report.log not found.")
        return

    with open(LOG_FILE, "r", encoding="utf-8") as log:
        for line in log:
            s = line.rstrip("\n")
            if s.strip().startswith("[PASS]"):
                print(f"{GREEN}{s}{RESET}")
            elif s.strip().startswith("[FAIL]"):
                print(f"{RED}{s}{RESET}")
            else:
                print(s)

    passed, failed = _count_results_from_log()
    total = passed + failed
    print("-" * 60)
    print(f"Summary: {passed}/{total} visible test cases passed, {failed} failed.")

def run_tests():
    validate_python_versions()

    # Reset log
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    # Run pytest IN-PROCESS (faster)
    pytest.main([
        "test_configurable_functions.py",
        "-v",
        "-s",
        "--tb=short"
    ])

    print_log_results()

if __name__ == "__main__":
    run_tests()
