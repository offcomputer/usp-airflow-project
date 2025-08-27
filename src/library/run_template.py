import argparse
import time
import uuid
from datetime import timedelta

from airflow.models import DagRun, DagModel
from airflow.utils.state import State
from airflow.utils.session import create_session
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils.timezone import utcnow
from airflow.exceptions import DagRunAlreadyExists
from sqlalchemy.exc import IntegrityError

import library.helpers as helpers


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--slot", type=int, default=0, help="Parallel slot index (0..N)")
    return p.parse_args()


def unpause_dag(dag_id: str):
    with create_session() as session:
        (
            session.query(DagModel)
            .filter(DagModel.dag_id == dag_id)
            .update({DagModel.is_paused: False})
        )
        session.commit()


def trigger_once(dag_id: str, run_conf: dict, offset_seconds: int):
    run_id = f"manual__{uuid.uuid4()}"
    execution_date = utcnow().replace(microsecond=0) + timedelta(seconds=offset_seconds)
    dag_run = trigger_dag(
        dag_id=dag_id,
        run_id=run_id,
        conf=run_conf,
        execution_date=execution_date,
    )
    print(f"Triggered {dag_id} run_id={run_id} exec_date={execution_date.isoformat()}")
    return run_id, execution_date


def run_dag_with_config(dag_id: str, run_conf: dict, slot: int, loop_idx: int):
    # Ensure the variable exists once; then updates won’t collide on INSERT
    if loop_idx == 0:
        helpers.create_config_variable("app_template_config", run_conf)

    # Concurrency-safe update (may still be called by each iteration)
    helpers.update_config_variable("app_template_config", run_conf)

    unpause_dag(dag_id)

    base_offset = slot + loop_idx
    last_err = None
    for attempt in range(5):
        try:
            run_id, exec_date = trigger_once(
                dag_id=dag_id,
                run_conf=run_conf,
                offset_seconds=base_offset + attempt,
            )
            break
        except (DagRunAlreadyExists, IntegrityError) as e:
            last_err = e
            time.sleep(0.3)
    else:
        raise last_err if last_err else RuntimeError("Failed to trigger DAG after retries")

    # Wait until completion
    while True:
        with create_session() as session:
            dr = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).first()
            if dr and dr.state in [State.SUCCESS, State.FAILED]:
                print(f"DAG run {run_id} completed with state: {dr.state}")
                break
        time.sleep(10)


def main():
    args = parse_args()
    dag_id = "app_template"
    base_config = helpers.get_variable("app_template_config")
    # Fall back to a default (avoid empty on first ever run)
    if not base_config:
        base_config = {"task_distribution": [(1, 1, 1)]}

    task_distribution = base_config.get("task_distribution", [])

    for loop_idx, (x, y, z) in enumerate(task_distribution):
        print(f"\n--- DAG: extractors={x}, transformers={y}, loaders={z} ---")
        updated_config = {
            **base_config,
            "extractors": x,
            "transformers": y,
            "loaders": z,
        }
        run_dag_with_config(dag_id, updated_config, slot=args.slot, loop_idx=loop_idx)


if __name__ == "__main__":
    main()
