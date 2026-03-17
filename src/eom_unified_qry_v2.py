import sys

from full_report_v2 import main as run_full_report_v2


if __name__ == "__main__":
    forwarded = [arg for arg in sys.argv[1:] if arg != "--report-type" and arg != "EOM"]
    run_full_report_v2(["--report-type", "EOM", *forwarded])
