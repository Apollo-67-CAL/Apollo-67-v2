import sys

from ingestion.tests.contract_tests import run as run_contract_tests


def main() -> int:
    try:
        run_contract_tests()
    except Exception as exc:
        print(f"FAIL: ingestion contract tests - {exc}")
        return 1
    print("PASS: ingestion contract tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
