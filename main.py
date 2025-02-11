import os
from dotenv import load_dotenv
from commodity_tickets_processor import CommodityTicketsProcessor

def main():
    # Cargar variables de entorno
    load_dotenv()

    # Construir connection string desde variables de entorno
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"UID={os.getenv('SQL_USERNAME')};"
        f"PWD={os.getenv('SQL_PASSWORD')};"
    )

    # Configurar el procesador
    processor = CommodityTicketsProcessor(
        connection_string=connection_string,
        batch_size=100000,
        debug_mode=False
    )

    # Obtener la ruta de la carpeta XML
    xml_folder = 'merged_xmlFiles_tickets'
    # xml_folder = 'xmlFiles_commodity_tickets'

    try:
        # Procesar los archivos
        processor.process_files(xml_folder)
        print("Procesamiento completado exitosamente")
        
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")

if __name__ == "__main__":
    main()