# Proyecto proveedor de servicios móviles 

## Contenido del repo
Este repo contiene todos los archivos necesarios para la creación del pipeline. A continuación se procede a hacer una descripción de cada uno de los scripts/archivos.
## gsheets_to_bigquery_pipeline.py: 
Este código es un script de Python que utiliza la biblioteca Apache Beam y la API de Google Sheets para leer datos de varias hojas de cálculo, transformar esos datos en formato JSON para poder cargarlos a BigQuery. Aquí está el desglose de lo que hace cada parte del código:

1. Definición de variables:
   - `SCOPES`: Define el alcance de autenticación para acceder a las hojas de cálculo de Google.
   - `SERVICE_ACCOUNT_FILE`: Ruta al archivo de cuenta de servicio de Google que se utilizará para la autenticación.
   - `SPREADSHEET_ID`: ID de la hoja de cálculo de Google que contiene los datos.
   - `CLIENTS_RANGE`, `PRODUCTS_RANGE`, `PURCHASES_RANGE`: Rangos de celdas en las hojas de cálculo que se leerán.

2. Funciones:
   - `get_credentials()`: Obtiene las credenciales de autenticación utilizando la key en formato json de la cuenta de servicio y el alcance proporcionados.
   - `get_values()`: Utiliza las credenciales para obtener los valores de un rango específico en una hoja de cálculo.
   - `to_json_clients()`, `to_json_products()`, `to_json_purchases()`: Convierten filas de datos de las hojas de cálculo en objetos de diccionario JSON correspondientes a las tablas de destino.

3. flujo de datos:
   - Con un mismo pipeline de apache beam se crean 3 ramas para realizar la siguientes acciones:
     - Leer datos de las hojas de cálculo utilizando la función `get_values` y crear un PCollection.
     - Transformar las filas de datos utilizando las funciones `to_json_clients`, `to_json_products` y `to_json_purchases`.
     - Escribir los datos transformados en tablas de BigQuery utilizando la función `beam.io.WriteToBigQuery`.

## mobile-services-dag.py
Este código en Python se utiliza para definir un DAG en Airflow llamado 'mobile-services-dag'. Este DAG ejecuta un flujo de trabajo de Apache Beam en Dataflow (`gsheets_to_bigquery_pipeline.py`) para extraer y cargar los datos. Los datos cargados en bigQuery se transforman y se limpian con la consulta (`data_cleaning.sql`) y se almacenan las tablas resultado. A continuación, se describe el código paso a paso:

1. Se definen varias constantes que contienen rutas de Google Cloud Storage (GCS) y otros valores relevantes, como rutas a archivos y ubicaciones de almacenamiento.

2. Se inicia la construcción de la DAG con un bloque `with DAG(...)`. Se configuran los siguientes atributos de la DAG:
   - `default_args`: Configuración predeterminada para los argumentos de las tareas en la DAG, como el propietario, el número de intentos de reintentos, etc.
   - `description`: Una descripción de la DAG.
   - `schedule`: Frecuencia con la que se ejecutará la DAG (en este caso, una vez al día).
   - `start_date`: Fecha en la que se iniciará la ejecución de la DAG.
   - `template_searchpath`: Ruta para buscar plantillas.

3. Se definen dos tareas (`t1` y `t2`) en la DAG:
   - `t1`: Utiliza el operador `BeamRunPythonPipelineOperator` para ejecutar un flujo de trabajo de Apache Beam en Dataflow. Los parámetros incluyen el archivo Python a ejecutar en Dataflow (`GCS_PYTHON`), opciones de pipeline como ubicaciones temporales y de preparación, y configuración de Dataflow (nombre del trabajo, proyecto y ubicación).
   - `t2`: Utiliza el operador `BigQueryInsertJobOperator` para ejecutar una consulta en BigQuery y almacenar los resultados en una tabla. La consulta se lee desde un archivo externo ('data_cleaning.sql') y se ejecuta con la configuración proporcionada.

4. Se establece una relación de dependencia entre `t1` y `t2` utilizando el operador `>>`, lo que significa que `t2` depende de que `t1` se complete con éxito.

## data_cleaning.sql

Este código se encarga de transformar e insertar los datos a las tablas de la zona de resultados en BigQuery. A continuación, se procede a describir el código:

1. **Clientes:**
   - Borra todos los datos existentes en la tabla `myproject-387020.results_zone.clientes`.
   - Copia los datos transformados desde la tabla `myproject-387020.raw_zone.clientes` a la tabla mencionada.
   - Las transformaciones incluyen cambiar tipos de datos, limpiar y ajustar valores en las columnas. Por ejemplo, se convierten las edades, se eliminan espacios vacíos en la ocupación, se convierten salarios y se ajusta el estado civil.

2. **Compras:**
   - Borra todos los datos existentes en la tabla `myproject-387020.results_zone.compras`.
   - Inserta los datos transformados desde la tabla `myproject-387020.raw_zone.compras` a la tabla mencionada.
   - Algunas transformaciones incluyen cambiar tipos de datos y formatear fechas.

3. **Productos:**
   - Borra todos los datos existentes en la tabla `myproject-387020.results_zone.productos`.
   - Inserta los datos transformados desde la tabla `myproject-387020.raw_zone.productos` a la tabla mencionada.
   - Se realizan transformaciones simples como cambiar tipos de datos.

4. **Unificada:**
   - Borra todos los datos existentes en la tabla `myproject-387020.results_zone.unificada`.
   - Combina los datos de las tablas de compras (`t1`), clientes (`t2`), productos (`t3`) y paises (`t4`) usando operaciones de unión (LEFT JOIN).
   - Selecciona y organiza los datos de todas estas tablas en una tabla unificada.
   - Se realizan transformaciones en los nombres de columnas y se ajustan algunos valores de datos.
   - Los resultados finales incluyen información de compras, clientes, productos y países.