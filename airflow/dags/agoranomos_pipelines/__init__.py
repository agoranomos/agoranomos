"""Code that the DAGs of the agoranomos bundle share (ADR-0013).

Airflow puts the folder of a DAG bundle on sys.path, for parsing and for running tasks, so the DAG
files import this package by name. .airflowignore keeps the DAG processor out of it: it holds no
DAG.
"""
