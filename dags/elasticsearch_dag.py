from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from elasticsearch import Elasticsearch
from airflow.models import baseoperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from contextlib import closing
import json

from plugins.elasticsearch_plugin.operators.postgres_to_elastic import PostgresToElasticOperator

class ElasticHook(BaseHook):

    def __init__(self, conn_id='elasticsearch_default',*args,**kwargs):
        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id)

        conn_config = {}
        hosts = []

        if conn.host:
            hosts= conn.host.split(',')
        if conn.port:
            conn_config['port'] = int(conn.port)
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema

    def info(self):
        return self.es.info()

    def set_index(self,index):
        self.index = index

    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res

class PostgresToElasticOperator(baseoperator):

    def __init__(self,sql,index,postgres_conn_id='postgres_default',
        elastic_conn_id='elasticsearch_default', *args, **kwargs):
        super(PostgresToElasticOperator, self).__init__(*args,**kwargs)
        self.sql =sql
        self.index = index
        self.postgres_conn_id = postgres_conn_id
        self.elastic_conn_id = elastic_conn_id

    def execute(self, context):
        es = ElasticHook(conn_id=self.elastic_conn_id)
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with closing(pg.get_conn()) as conn:
            with closing(conn.cursos()) as cur:
                cur.itersize= 1000
                cur.execute(self.sql)
                for row in cur:
                    doc = json.dumps(row, indent=2)
                    es.add_doc(index=self.index,doc_type='external',body=doc)

default_args = {
    'start_date': datetime(2020,1,1)
}

def _print_es_info():
    hook = ElasticHook()
    print(hook.info())


with DAG(
    'elasticsearch_dag',
    schedule_interval='@daily',
    default_args = default_args,
    catchup=False
) as dag:

    print_es_info = PythonOperator(
        task_id='print_es_info',
        python_callable=_print_es_info
    )

    connections_to_es = PostgresToElasticOperator(
        task_id='connections_to_es',
        sql='SELECT * FROM connection',
        index='connections'
    )

    print_es_info >> connections_to_es
