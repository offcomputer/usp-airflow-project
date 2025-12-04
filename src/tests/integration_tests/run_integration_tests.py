"""Run integration tests for service clients inside the Airflow worker.

Example usage (from within the worker):

    python src/tests/integration_tests/run_integration_tests.py
"""

import pytest


def main() -> int:
    """Run all tests under the integration_tests folder with coverage."""
    args = [
        "src/tests/integration_tests",
        "--cov=src",
        "--cov-report=term-missing",
    ]
    return pytest.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
