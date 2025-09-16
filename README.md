## Repo Structure

```
├── .
├── compose.yml
├── dev.env
├── .gitignore
├── LICENSE
├── README.md
├── src
│   ├── apps
│   │   ├── __init__.py
│   │   ├── sample
│   │   │   ├── dags
│   │   │   │   ├── dag_sample.py
│   │   ├── template
│   │   │   ├── config
│   │   │   │   ├── app_settings.json
│   │   │   ├── dags
│   │   │   │   ├── dag_template.py
│   │   │   │   ├── run_dags.py
│   │   │   ├── etl
│   │   │   │   ├── extract.py
│   │   │   │   ├── __init__.py
│   │   │   │   ├── load.py
│   │   │   │   ├── transform.py
│   │   │   ├── __init__.py
│   │   │   ├── tasks
│   │   │   │   ├── etl_tasks.py
│   │   │   │   ├── __init__.py
│   ├── library
│   │   ├── helpers.py
│   │   ├── __init__.py
│   │   ├── run_template.py
│   ├── notebooks
│   │   ├── logs.ipynb
│   │   ├── results.ipynb
│   ├── patterns
│   │   ├── etl
│   │   │   ├── extract.py
│   │   │   ├── __init__.py
│   │   │   ├── load.py
│   │   │   ├── transform.py
│   │   ├── __init__.py
│   │   ├── services
│   │   │   ├── data_sink.py
│   │   │   ├── data_source.py
│   │   │   ├── __init__.py
│   │   │   ├── message_broker.py
│   ├── services
│   │   ├── data_sink.py
│   │   ├── data_source.py
│   │   ├── __init__.py
│   │   ├── message_broker.py
├── utils
│   ├── repo_structure.sh
│   ├── tmp_permission.sh
```