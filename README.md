# `usp-airflow-project`

This branch contains the codebase developed as part of a research project exploring the design and evaluation of **parallelized ETL (Extract, Transform, Load) pipelines** using Apache Airflow. It investigates how Airflow DAGs can be structured to support modularity, scalability, and high throughput in workflows, especially in environments with heavy computational demands.

---

## Overview

The project aims to:

* Define reusable templates for building ETL DAGs with configurable degrees of parallelism.
* Evaluate performance of different task structures (number of parallel extractors / transformers / loaders, etc.).
* Provide infrastructure to collect execution logs and metrics for later analysis.
* Demonstrate how message-queue based communication and task orchestration can reduce workflow time while maintaining correctness and observability.

The repository includes:

* **Infrastructure code** (Docker, Postgres, Redis, etc.) to run the full stack locally.
* **Airflow DAG definitions** implementing the ETL workflows.
* **Shared modules / libraries** for extract, transform, load, logging, configuration.
* **Analysis notebooks** to process logs and visualize results.

---

## Reproducibility Instructions

These steps allow someone who clones the repository to run the environment locally, trigger the DAGs, collect logs, and analyze results.

### Prerequisites

* Docker
* Docker Compose

### Setup and Run

1. **Clone the repository:**

   ```bash
   git clone https://github.com/offcomputer/usp-airflow-project.git
   cd usp-airflow-project
   ```

2. **Prepare permissions for logs:**

   Run the helper script to ensure the `tmp/` folder has the correct permissions for both Airflow and Jupyter containers:

   ```bash
   ./utils/tmp_permission.sh
   ```

   > This step is required so that the `tmp/` folder created during DAG executions can be shared between services (Airflow writes logs, Jupyter reads them for analysis).

3. **Start all services:**

   ```bash
   docker-compose up -d
   ```

   This will launch:

   * **Postgres**: Airflow metadata database
   * **Redis**: message broker for Celery workers
   * **Airflow Scheduler & Webserver**: orchestration and UI
   * **Celery Workers**: parallel task execution
   * **JupyterLab**: for running analysis notebooks

4. **Access the Airflow UI:**

   * Open [http://localhost:8080](http://localhost:8080)
   * Log in with:

     * **User:** `admin`
     * **Password:** `admin`

5. **Trigger the experiment:**

   * In the Airflow UI, **enable and trigger the `run_dags` DAG**.
   * This DAG will automatically execute the **`dag_template` DAG**, which simulates the ETL pipeline.
   * A `tmp/` folder will be created to store execution logs.

6. **Analyze logs and results:**

   * Open **JupyterLab** at [http://localhost:8888](http://localhost:8888).
   * Navigate to the `analysis/` folder inside Jupyter.
   * Run the notebooks in order:

     1. `logs.ipynb`: parse execution logs and consolidate metrics.
     2. `results.ipynb`: generate plots and analyze performance across ETL configurations.

7. **Shutdown the environment when done:**

   ```bash
   docker-compose down
   ```

---

## Repository Structure

* **`infrastructure/`**: Docker and service configuration.
* **`application/`**: Airflow DAGs (`run_dags`, `dag_template`) and operators.
* **`libraries/`**: Reusable modules and templates.
* **`analysis/`**: Jupyter notebooks (`logs.ipynb`, `results.ipynb`) for log processing and results visualization.
* **`utils/`**: helper scripts (`tmp_permission.sh` ensures proper permissions for shared logs).

```
├── compose.yml
├── dev.env
├── .gitattributes
├── .gitignore
├── LICENSE
├── README.md
├── src
│   ├── apps
│   │   ├── sample
│   │   │   └── dags/dag_sample.py
│   │   ├── template
│   │   │   ├── dags
│   │   │   │   ├── dag_template.py
│   │   │   │   ├── run_dags.py
│   │   │   ├── etl/{extract.py, load.py, transform.py}
│   │   │   ├── tasks/etl_tasks.py
│   ├── library
│   │   ├── helpers.py
│   │   ├── run_template.py
│   ├── notebooks
│   │   ├── logs.ipynb
│   │   ├── results.ipynb
│   ├── patterns/{etl, services}
│   ├── services/{data_sink.py, data_source.py, message_broker.py}
├── utils
│   ├── repo_structure.sh
│   ├── tmp_permission.sh
```
