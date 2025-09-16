"""
Trigger utility for template DAGs.

This script is intended to trigger runs of the template DAGs
(app_template_N) in Airflow. It ensures that required Airflow
Variables exist, unpauses the DAG if needed, and triggers sequential
runs with different configuration values.

Typical usage example:
    python run_template.py --dag 1
"""

import argparse
import uuid
import time
from itertools import product

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils.timezone import utcnow
from airflow.models import DagModel, Variable
from airflow.utils.session import create_session


def parse_args():
    """Parses command-line arguments.

    Returns:
        An argparse.Namespace containing the parsed arguments:
        --dag: The DAG number (1..10).
    """
    p = argparse.ArgumentParser()
    p.add_argument("--dag", type=int, required=True, help="N - DAG (1..10)")
    return p.parse_args()


def unpause_dag(dag_id: str):
    """Unpauses a DAG if it is paused.

    Args:
        dag_id: The ID of the DAG to unpause.
    """
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == dag_id).update(
            {DagModel.is_paused: False}
        )
        session.commit()


def ensure_variables():
    """Ensures all configuration Variables exist.

    Creates Variables for all 125 combinations of extractor,
    transformer, and loader counts (1..5 each), if they do not
    already exist.
    """
    combos = list(product(range(1, 6), repeat=3))
    for idx, (x, y, z) in enumerate(combos, start=1):
        key = str(idx)
        value = f"{x}-{y}-{z}"
        if not Variable.get(key, default_var=None):
            Variable.set(key, value)


def main():
    """Triggers runs of a selected template DAG.

    Steps:
        1. Parses the --dag argument.
        2. Ensures required Variables exist.
        3. Unpauses the DAG if necessary.
        4. Triggers 125 runs, one per configuration combination.

    Each run is triggered with a unique run_id and a conf dict
    containing {"var": <combination_index>}.
    """
    args = parse_args()
    dag_id = f"app_template_{args.dag}"

    ensure_variables()
    unpause_dag(dag_id)

    for idx in range(1, 126):  # 1..125
        run_id = f"manual__{uuid.uuid4()}"
        conf = {"var": str(idx)}

        trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf=conf,
            execution_date=utcnow(),
        )
        print(f"Triggered {dag_id} run_id={run_id} with var={idx}")

        time.sleep(2)


if __name__ == "__main__":
    main()
