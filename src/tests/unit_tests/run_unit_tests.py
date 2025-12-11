"""Run unit tests with coverage inside the Airflow worker container.

Example usage (from within the worker):

    python src/tests/unit_tests/run_unit_tests.py
"""

import pytest


def main() -> int:
    """Run pytest with coverage over unit tests."""
    args = [
        "src/tests/unit_tests",
        "--cov=src",
        "--cov-report=term-missing",
        "-m",
        "not integration",
    ]
    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
