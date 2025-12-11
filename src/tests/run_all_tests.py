"""Run both unit and integration tests with coverage inside the Airflow worker.

Example usage (from within the worker):

    python src/tests/run_all_tests.py
"""

import pytest


def main() -> int:
    """Run unit tests first, then integration tests, aggregating coverage."""
    # Run unit tests
    unit_args = [
        "src/tests/unit_tests",
        "--cov=src",
        "--cov-report=term-missing",
    ]
    unit_result = pytest.main(unit_args)
    if unit_result != 0:
        return unit_result

    # Run integration tests
    integration_args = [
        "src/tests/integration_tests",
        "--cov=src",
        "--cov-report=term-missing",
    ]
    return pytest.main(integration_args)


if __name__ == "__main__":
    raise SystemExit(main())
