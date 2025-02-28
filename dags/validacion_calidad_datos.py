from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime

# Definir los parámetros del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 20),
    'retries': 1,
}

dag = DAG(
    dag_id='validacion_calidad_datos',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Función para crear la tabla en el datamart
def crear_tabla_calidad():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS datamart;

    CREATE TABLE IF NOT EXISTS datamart.ventas_insumos_calidad (
        id SERIAL PRIMARY KEY,
        fecha DATE NOT NULL,
        producto VARCHAR(255) NOT NULL,
        categoria VARCHAR(100) NOT NULL,
        cantidad INT NOT NULL,
        precio_unitario DECIMAL(10,2) NOT NULL,
        total_venta DECIMAL(10,2) NOT NULL,
        cliente VARCHAR(255) NOT NULL,
        estado_calidad VARCHAR(20) NOT NULL,
        descripcion_error TEXT
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tabla datamart.ventas_insumos_calidad creada correctamente.")

# Función para validar la calidad de los datos
def validar_calidad_datos():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Cargar los datos de la tabla origen
    df = pd.read_sql("SELECT * FROM datawarehouse.ventas_insumos", engine)
    
    if df.empty:
        raise ValueError("No hay datos en la tabla datawarehouse.ventas_insumos")
    
    # Agregar columna de calidad y descripción de errores
    df['estado_calidad'] = 'OK'
    df['descripcion_error'] = ''

    # Eliminar duplicados
    df = df.drop_duplicates(subset=['fecha', 'producto', 'cliente'])
    
    # Validaciones
    errores = []
    for index, row in df.iterrows():
        error = []
        if pd.isnull(row['producto']) or pd.isnull(row['categoria']) or pd.isnull(row['cliente']):
            error.append("Campos críticos nulos")
        if row['cantidad'] <= 0:
            error.append("Cantidad negativa o cero")
        if row['precio_unitario'] <= 0 or row['total_venta'] <= 0:
            error.append("Precio o Total Venta negativo")
        if not isinstance(row['fecha'], datetime):
            error.append("Fecha mal formada")
        
        if error:
            df.at[index, 'estado_calidad'] = 'ERROR'
            df.at[index, 'descripcion_error'] = ', '.join(error)
    
    # Guardar en la tabla final del datamart
    df.to_sql('ventas_insumos_calidad', engine, schema='datamart', if_exists='replace', index=False)
    print("Datos procesados y almacenados en datamart.ventas_insumos_calidad")

# Operador para crear la tabla
crear_tabla_task = PythonOperator(
    task_id='crear_tabla_calidad',
    python_callable=crear_tabla_calidad,
    dag=dag
)

# Operador para validar los datos
validar_calidad_task = PythonOperator(
    task_id='validar_calidad_datos',
    python_callable=validar_calidad_datos,
    dag=dag
)

# Definir el orden de ejecución
crear_tabla_task >> validar_calidad_task
