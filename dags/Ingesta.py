from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from datetime import datetime

# Ruta del archivo CSV en el contenedor de Airflow
CSV_PATH = "/opt/airflow/data_csv/ventas_insumos_programacion.csv"
TABLE_NAME = "ventas_insumos"
SCHEMA_NAME = "datawarehouse"

# Función para crear la tabla en PostgreSQL
def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    create_table_sql = f'''
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        fecha DATE NOT NULL,
        producto VARCHAR(255) NOT NULL,
        categoria VARCHAR(100) NOT NULL,
        cantidad INT NOT NULL,
        precio_unitario DECIMAL(10,2) NOT NULL,
        total_venta DECIMAL(10,2) NOT NULL,
        cliente VARCHAR(255) NOT NULL
    );
    '''
    
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Tabla {SCHEMA_NAME}.{TABLE_NAME} creada correctamente.")

# Función para cargar el CSV en PostgreSQL
def load_csv_to_postgres():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"El archivo {CSV_PATH} no existe")
    
    # Leer el archivo CSV con Pandas
    df = pd.read_csv(CSV_PATH)
    
    # Conectar con PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Insertar los datos en la tabla
    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists='append', index=False)
    print(f"Datos insertados en la tabla {SCHEMA_NAME}.{TABLE_NAME}")

# Definir los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 20),
    'retries': 1,
}

# Definir el DAG
dag = DAG(
    dag_id='cargar_csv_dw',
    default_args=default_args,
    schedule_interval=None,  # Se ejecutará manualmente
    catchup=False
)

# Tarea para crear la tabla en PostgreSQL
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

# Tarea para cargar el CSV a PostgreSQL
load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag
)

# Definir el orden de ejecución
create_table_task >> load_csv_task