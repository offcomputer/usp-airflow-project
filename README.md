# usp-airflow-project

```
├── .
├── compose.yml
├── dev.env
├── .gitignore
├── LICENSE
├── README.md
├── src
│   ├── app
│   │   ├── airflow
│   │   │   ├── __init__.py
│   │   ├── control
│   │   │   ├── __init__.py
│   │   ├── etl
│   │   │   ├── __init__.py
│   │   ├── __init__.py
│   │   ├── services
│   │   │   ├── __init__.py
│   ├── etl.py
│   ├── notebooks
│   │   ├── logs.ipynb
│   │   ├── results.ipynb
│   ├── sample.py
├── utils
│   ├── repo_structure.sh
```
(base) p@pytzen:~/lab/usp-airflow-project$ ./utils/repo_structure.sh 
```
├── .
├── compose.yml
├── dev.env
├── .gitignore
├── LICENSE
├── README.md
├── src
│   ├── app
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
│   │   │   ├── __init__.py
│   │   ├── tasks.py
│   ├── etl_dag.py
│   ├── notebooks
│   │   ├── logs.ipynb
│   │   ├── results.ipynb
│   ├── sample_dag.py
├── utils
│   ├── repo_structure.sh
```
(base) p@pytzen:~/lab/usp-airflow-project$ 
 *  History restored 

(base) p@pytzen:~/lab/usp-airflow-project$ ./utils/repo_structure.sh 
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