import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
#SERVICE_ACCOUNT_FILE = '/Users/josuecassab/Projects/mobile-services-pipeline/myproject-387020-7f94fd954d21.json'
SERVICE_ACCOUNT_FILE = '/home/airflow/gcs/data/myproject-387020-7f94fd954d21.json'
SPREADSHEET_ID="1AJY_-BxosSmbC2-sIQZn9hD5T6BE7QfzThddXId0Tyg"

CLIENTS_RANGE = "Clientes!A:N"
CLIENTS_TABLE = "myproject-387020.raw_zone.clientes"
CLIENTS_SCHEMA = "id:STRING,Nombre:STRING,Pais:STRING,edad:STRING,Ocupacion:STRING,Score:STRING,Salario_net_USD:STRING,Estado_Civil:STRING,\
Estado:STRING,Fecha_Inactividad:STRING,Genero:STRING,Device:STRING,Nivel_Educativo:STRING,Carrera:STRING"

PRODUCTS_RANGE = "Producto!A:F"
PRODUCTS_TABLE = "myproject-387020.raw_zone.productos"
PRODUCTS_SCHEMA = "id:STRING,nombre:STRING,ValorUSD:STRING,Cantidad_Datos_MB:STRING,Vigencia:STRING,Telefonia:STRING"

PURCHASES_RANGE = "Compras!A:F"
PURCHASES_TABLE = "myproject-387020.raw_zone.compras"
PURCHASES_SCHEMA = "id:STRING,cust_id:STRING,prod_id:STRING,Gasto:STRING,FechaCompra:STRING,Mediopago:STRING"


def get_credentials(scope, service_account_file):
    """
    Retrieves the credentials using the provided service account file and scope.
    """
    credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    return credentials

def get_values(spreadsheet_id, range_name):
    """
    Retrieves values from a specified range of a Google Sheets spreadsheet.
    """
    try:
        service = build('sheets', 'v4', credentials=get_credentials(SCOPES, SERVICE_ACCOUNT_FILE))

        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name).execute()
        rows = result.get('values', [])
        print(f"{len(rows)} rows retrieved")
        return rows[1:]
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error
    
def to_json_clients(item):
    """
    Converts a row of data from the 'Clientes' sheet into a dictionary object.
    """
    return {
        "id": item[0],
        "Nombre": item[1],
        "Pais": item[2],
        "edad": item[3],	
        "Ocupacion": item[4],
        "Score": item[5],
        "Salario_net_USD": item[6],
        "Estado_Civil": item[7],
        "Estado": item[8],
        "Fecha_Inactividad": item[9],
        "Genero": item[10],
        "Device": item[11],
        "Nivel_Educativo": item[12],
        "Carrera": item[13]
    }

def to_json_products(item):
    """
    Converts a row of data from the 'Producto' sheet into a dictionary object.
    """
    return {
        "id": item[0],	
        "nombre": item[1],
        "ValorUSD":item[2],
        "Cantidad_Datos_MB": item[3],
        "Vigencia": item[4],
        "Telefonia": item[5]
    }
def to_json_purchases(item):
    """
    Converts a row of data from the 'Compras' sheet into a dictionary object.
    """
    return {
        "id": item[0],	
        "cust_id": item[1],
        "prod_id": item[2],	
        "Gasto": item[3],
        "FechaCompra": item[4],
        "Mediopago": item[5]
    }

if __name__ == '__main__':
   
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args,
                                       job_name="mobile-services-pipe",
                                       temp_location='gs://myproject-387020-mobile-services/temp')
   
    with beam.Pipeline(options=pipeline_options) as pipe:
        
        pc_clients = (pipe  | 'Read data from "clientes" sheet' >>  beam.Create(get_values(SPREADSHEET_ID, CLIENTS_RANGE))
                            | 'Assemble Dictionary' >> beam.Map(to_json_clients)                                      
                            | "Create clients table" >> beam.io.WriteToBigQuery(
                                                        table=CLIENTS_TABLE,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        schema=CLIENTS_SCHEMA
                                                        )
                            )
        
        pc_products = (pipe | 'Read data from "producto" sheet' >>  beam.Create(get_values(SPREADSHEET_ID, PRODUCTS_RANGE))
                            | 'Assemble Dictionary products' >> beam.Map(to_json_products)
                            | "Create products table" >> beam.io.WriteToBigQuery(
                                                        table=PRODUCTS_TABLE,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        schema=PRODUCTS_SCHEMA
                                                        )
                            )
                                                
        pc_purchases = (pipe| 'Read data from "compras" sheet' >>  beam.Create(get_values(SPREADSHEET_ID, PURCHASES_RANGE))
                            | 'Assemble Dictionary purchases' >> beam.Map(to_json_purchases)
                            | "Create purchases table" >> beam.io.WriteToBigQuery(
                                                        table=PURCHASES_TABLE,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        schema=PURCHASES_SCHEMA
                                                        )
                            )