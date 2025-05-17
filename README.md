# usp-airflow-project

```
├── .
├── compose.yml
├── dev.env
├── .gitignore
├── LICENSE
├── README.md
├── src
│   ├── app_template
│   │   ├── airflow
│   │   │   ├── __init__.py
│   │   ├── control
│   │   │   ├── __init__.py
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
│   │   ├── tasks.py
│   ├── dag_sample.py
│   ├── dag_template.py
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
├── utils
│   ├── repo_structure.sh
```