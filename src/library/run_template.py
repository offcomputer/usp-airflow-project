import argparse
import uuid
import time

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils.timezone import utcnow
from airflow.models import DagModel
from airflow.utils.session import create_session

import library.helpers as helpers


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dag", type=int, required=True, help="DAG number (e.g., 1)")
    return p.parse_args()


def unpause_dag(dag_id: str):
    """Unpause a DAG so that it can actually run."""
    with create_session() as session:
        (
            session.query(DagModel)
            .filter(DagModel.dag_id == dag_id)
            .update({DagModel.is_paused: False})
        )
        session.commit()
    print(f"DAG {dag_id} unpaused.")


def main():
    args = parse_args()
    dag_id = f"app_template_{args.dag}"

    # Make sure DAG is active
    unpause_dag(dag_id)

    # Generate all task distribution combinations
    combinations = helpers.get_combinations()

    for idx, (x, y, z) in enumerate(combinations, start=1):
        print(f"\n[{idx}/{len(combinations)}] DAG={dag_id} -> extractors={x}, transformers={y}, loaders={z}")

        # Build the run-specific configuration (sent via conf only)
        config = {
            "extractors": x,
            "transformers": y,
            "loaders": z,
        }

        # Generate a unique run_id
        run_id = f"manual__{uuid.uuid4()}"

        # Trigger the DAG immediately with the given config
        trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf=config,
            execution_date=utcnow(),
        )

        print(f"Triggered {dag_id} run_id={run_id}")

        # Small pause to avoid execution_date collisions
        time.sleep(2)


if __name__ == "__main__":
    main()
