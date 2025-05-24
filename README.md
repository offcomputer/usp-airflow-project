# usp-airflow-project

```
├── .
├── compose.yml
├── dev.env
├── .gitignore
├── LICENSE
├── README.md
├── src
│   ├── apps
│   │   ├── sample
│   │   │   ├── dags
│   │   │   │   ├── dag_sample.py
│   │   ├── template
│   │   │   ├── config
│   │   │   │   ├── app_settings.json
│   │   │   ├── dags
│   │   │   │   ├── dag_template.py
│   │   │   ├── etl
│   │   │   │   ├── extract.py
│   │   │   │   ├── __init__.py
│   │   │   │   ├── load.py
│   │   │   │   ├── transform.py
│   │   │   ├── tasks
│   │   │   │   ├── __init__.py
│   │   │   │   ├── tasks.py
│   ├── library
│   │   ├── helpers.py
│   │   ├── __init__.py
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
```