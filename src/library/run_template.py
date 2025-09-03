import argparse
import uuid
import time
from itertools import product

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils.timezone import utcnow
from airflow.models import DagModel, Variable
from airflow.utils.session import create_session


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dag", type=int, required=True, help="N - DAG (1..10)")
    return p.parse_args()


def unpause_dag(dag_id: str):
    with create_session() as session:
        session.query(DagModel).filter(DagModel.dag_id == dag_id).update(
            {DagModel.is_paused: False}
        )
        session.commit()


def ensure_variables():
    combos = list(product(range(1, 6), repeat=3))
    for idx, (x, y, z) in enumerate(combos, start=1):
        key = str(idx)
        value = f"{x}-{y}-{z}"
        if not Variable.get(key, default_var=None):
            Variable.set(key, value)


def main():
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
