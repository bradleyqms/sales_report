import sys

from full_report_v2 import main as run_full_report_v2


def _strip_report_type_args(args: list[str]) -> list[str]:
    forwarded: list[str] = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--report-type":
            i += 2
            continue
        if arg.startswith("--report-type="):
            i += 1
            continue
        forwarded.append(arg)
        i += 1
    return forwarded


if __name__ == "__main__":
    forwarded = _strip_report_type_args(sys.argv[1:])
    run_full_report_v2(["--report-type", "EOM", *forwarded])
