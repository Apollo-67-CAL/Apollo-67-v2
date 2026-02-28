import sys

from ingestion.tests.failure_injection import run as run_failure_injection_tests


def main() -> int:
    try:
        run_failure_injection_tests()
    except Exception as exc:
        print(f"FAIL: ingestion validation injection tests - {exc}")
        return 1
    print("PASS: ingestion validation injection tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
