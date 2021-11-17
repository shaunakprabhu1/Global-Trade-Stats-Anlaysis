# Global-Trade-Stats-Anlaysis
Analysed UN’s Global Trade dataset with over 8.5 million rows. Tools used: Python, Airflow, Neo4j, Streamlit, plotly.

Performed data profiling, cleaning, and wrangling using Python.

Documented data flow by maintaining a data dictionary, data glossary as well as by using source to target mapping.

Created and presented an interactive dashboard in Streamlit using Python to display trade insights for countries.

## Folder Contents

### 1) data-ingestion

dataCleansing_loadNeo4j.ipynb: The initial jupyter notebook used to profile the data. 

team6_pipeline.py: Apache Airflow script to automate the process. Conisted of Reading data, Cleaning data, Adding unique key, creating new csv, creating new4j constraints and, loading data into database

### 2) metadata

export_metadata.py: Python script to connnect to neo4j DB and extract all metadata such as nodes, relationships, count.

### 3) viz

app.py: python script for reading data from database and connecting it to streamlit to show visualizations and analysis from data. 

