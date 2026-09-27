import os
import sys
import json
import pytest
from datetime import datetime

LOG_FILE = "test_report.log"
CONFIG_FILE = "test_config.json"

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def validate_python_versions():
    """
    Ensure PySpark uses the same Python executable.
    """
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

def _safe_load_config_count():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if isinstance(cfg, list):
            total = len(cfg)
            visible = sum(1 for t in cfg if bool(t.get("visible", True)))
            hidden = total - visible
            return total, visible, hidden
    except Exception:
        pass
    return None, None, None

def print_log_results():
    if not os.path.exists(LOG_FILE):
        print("test_report.log not found.")
        return

    total_cfg, visible_cfg, hidden_cfg = _safe_load_config_count()

    pass_total = fail_total = 0
    pass_visible = fail_visible = 0
    pass_hidden = fail_hidden = 0

    with open(LOG_FILE, "r", encoding="utf-8") as log:
        for line in log:
            s = line.strip()
            if not s:
                continue

            is_pass = "[PASS]" in s or " Passed:" in s
            is_fail = "[FAIL]" in s or " Failed:" in s

            # Colorize output
            if is_pass:
                print(f"{GREEN}{s}{RESET}")
                pass_total += 1
            elif is_fail:
                print(f"{RED}{s}{RESET}")
                fail_total += 1
            else:
                print(s)

            # Visible/Hidden tallies (best-effort)
            if is_pass or is_fail:
                if "Visible Test Case" in s:
                    if is_pass:
                        pass_visible += 1
                    else:
                        fail_visible += 1
                elif "Hidden Test Case" in s:
                    if is_pass:
                        pass_hidden += 1
                    else:
                        fail_hidden += 1

    # Summary
    total_ran = pass_total + fail_total
    parts = []
    if total_cfg is not None:
        parts.append(f"Config: {total_cfg} tests ({visible_cfg} visible, {hidden_cfg} hidden)")
    parts.append(f"Ran: {total_ran} | Passed: {pass_total} | Failed: {fail_total}")

    # Include visible/hidden breakdown if detected
    if (pass_visible + fail_visible + pass_hidden + fail_hidden) > 0:
        parts.append(
            f"Visible Passed/Failed: {pass_visible}/{fail_visible} | Hidden Passed/Failed: {pass_hidden}/{fail_hidden}"
        )

    summary = "Summary: " + " | ".join(parts)
    color = GREEN if fail_total == 0 and total_ran > 0 else (YELLOW if total_ran > 0 else RED)
    print(f"{color}{summary}{RESET}")

def run_tests():
    validate_python_versions()

    # Reset log for a clean run
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    # User-friendly header (also written by tests to the log)
    print(f"=== SmartCity Mobility Mega Assessment :: Test Run @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

    # Run pytest in-process
    pytest.main([
        "test_configurable_functions.py",
        "-v",
        "-s",
        "--tb=short"
    ])

    # Print log summary
    print_log_results()

if __name__ == "__main__":
    run_tests()
