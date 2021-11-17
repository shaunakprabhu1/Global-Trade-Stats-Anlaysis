import shutil

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from py2neo import Graph

prod_df = pd.read_csv('commodity_trade_statistics_data.csv')


def read_data():
    prod_df = pd.read_csv('commodity_trade_statistics_data.csv')


def clean_data():
    for i in prod_df.columns:
        print('Checking field %s for Nulls' % i)
        prod_df[i] = prod_df[i].fillna('No Data')


def add_index():
    prod_df["id"] = prod_df.index + 1


def write_csv():
    prod_df.head(10000).to_csv('cleanedData.csv', index=False)
    destination = '/Users/dileepholla/Library/Application Support/Neo4j Desktop/Application/relate-data/dbmss/dbms-e88658a5-5a5a-4681-9281-c6106b04fd76/import'
    shutil.move('cleaned.csv', destination)
    print('File Moved')


def neo4j_create_constraints():
    # Connect to Neo4j - enter your connection params here
    uri = "bolt://localhost:7687"
    username = "neo4j"
    pwd = "123456"

    session = Graph(uri, auth=(username, pwd))

    constraints = [
        'CREATE CONSTRAINT com IF NOT EXISTS ON (com:Commodity) ASSERT com.id IS UNIQUE;',
        'CREATE CONSTRAINT flow IF NOT EXISTS ON (flo:Flow) ASSERT flo.flow IS UNIQUE;',
        'CREATE CONSTRAINT cat IF NOT EXISTS ON (cat:Category) ASSERT cat.category IS UNIQUE;',
        'CREATE CONSTRAINT country IF NOT EXISTS ON (cou:Country) ASSERT cou.country_or_area IS UNIQUE;',
        'CREATE CONSTRAINT year IF NOT EXISTS ON(yea:Year) ASSERT yea.year IS UNIQUE;',
    ]

    for q in constraints:
        session.run(q)
        print('%s query run sucessfully' % q)


def load_data():
    uri = "bolt://localhost:7687"
    username = "neo4j"
    pwd = "123456"

    session = Graph(uri, auth=(username, pwd))

    query = '''LOAD CSV WITH HEADERS FROM "file:///cleaned.csv" AS row
    create (com:Commodity {CommodityName: row.commodity, CommodityCode: row.comm_code, Quantity: row.quantity, Weight: row.weight_kg, TradeValue: row.trade_usd, QuantityName: row.quantity_name})
    MERGE (cat: Category {category: row.category})
    MERGE (cou: Country {country: row.country_or_area})
    MERGE (flo: Flow {flow: row.flow})
    MERGE (yea: Year {year: row.year})
    MERGE (com)-[:BELONGS_TO]->(cat)
    MERGE (cou)-[:EXPORTS_IMPORTS_CAT]->(cat)
    MERGE (cou)-[:REPORTS]->(yea)
    MERGE (yea)-[:HAS_TYPE]->(flo)
    MERGE (flo)-[:TRADES]-> (com)
    MERGE (flo)-[:HAS_CATEGORIES]-> (cat);'''

    session.run(query)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
}

with DAG('DataCleaningIngestion',
         catchup=False,
         default_args=default_args,
         schedule_interval='@once',
         ) as dag:
    t0_start = PythonOperator(task_id='ReadData',
                              python_callable=read_data)
    t1_datacleaning = PythonOperator(task_id='DataCleaning',
                                     python_callable=clean_data,
                                     provide_context=True)
    t2_addid = PythonOperator(task_id='AddUniqueID',
                              python_callable=add_index,
                              provide_context=True)
    t3_writecsv = PythonOperator(task_id='CreateNewCSV',
                                 python_callable=write_csv,
                                 provide_context=True),
    t4_createcons = PythonOperator(task_id='CreateConstraints',
                                   python_callable=neo4j_create_constraints,
                                   provide_context=True)
    t5_loaddata = PythonOperator(task_id='LoadNeo4j',
                                 python_callable=load_data,
                                 provide_context=True)

t0_start >> t1_datacleaning >> t2_addid >> t3_writecsv >> t4_createcons >> t5_loaddata
