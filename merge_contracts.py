import os
from lxml import etree
from pathlib import Path
from typing import List
import math
import logging
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('merge_contracts_process.log'),
        logging.StreamHandler()
    ]
)

def setup_output_directory(base_path: Path) -> Path:
    """Crear y retornar el directorio de salida con timestamp"""
    output_dir = base_path / 'merged_xmlFiles_contracts'
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

def merge_xml_files(files: List[Path], output_path: Path) -> bool:
    """
    Merge múltiples archivos XML en uno solo
    """
    try:
        if not files:
            logging.error("No se proporcionaron archivos para merge")
            return False

        parser = etree.XMLParser(remove_blank_text=True)
        # Usar el primer archivo como base
        tree1 = etree.parse(str(files[0]), parser)
        root1 = tree1.getroot()
        
        # Merge todos los archivos restantes
        for file_path in files[1:]:
            try:
                tree2 = etree.parse(str(file_path), parser)
                root2 = tree2.getroot()
                contracts2 = root2.findall('contract')
                
                for contract in contracts2:
                    root1.append(contract)
                    
            except Exception as e:
                logging.error(f"Error procesando archivo {file_path}: {str(e)}")
                continue

        # Crear y guardar el nuevo XML
        new_tree = etree.ElementTree(root1)
        new_tree.write(str(output_path), pretty_print=True,
                      xml_declaration=True, encoding='UTF-8')

        logging.info(f"Merge completado exitosamente. Resultado guardado en: {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error durante el proceso de merge: {str(e)}")
        return False

def process_xml_chunks(xml_files: List[Path], chunk_size: int, output_dir: Path) -> None:
    """
    Procesar archivos XML en chunks
    """
    total_files = len(xml_files)
    num_chunks = math.ceil(total_files / chunk_size)
    
    logging.info(f"Iniciando procesamiento de {total_files} archivos en {num_chunks} chunks")

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_files)
        current_chunk = xml_files[start_idx:end_idx]
        
        logging.info(f"Procesando chunk {i+1}/{num_chunks} ({len(current_chunk)} archivos)")
        
        output_file = output_dir / f'CONTRACTS_{i+1}.xml'
        
        if merge_xml_files(current_chunk, output_file):
            logging.info(f"Chunk {i+1} procesado exitosamente")
        else:
            logging.error(f"Error procesando chunk {i+1}")

def main():
    try:
        # Configuración inicial
        current_path = Path.cwd()
        xml_folder = current_path / 'xmlFiles_contracts'  # Cambiado el nombre de la carpeta
        chunk_size = 13

        # Verificar que la carpeta existe
        if not xml_folder.exists():
            logging.error(f"La carpeta {xml_folder} no existe")
            return

        # Obtener lista de archivos XML
        xml_files = sorted(list(xml_folder.glob('*.XML')))

        if not xml_files:
            logging.error("No se encontraron archivos XML")
            return

        # Crear directorio de salida
        output_dir = setup_output_directory(current_path)
        
        logging.info(f"Iniciando proceso de merge con {len(xml_files)} archivos")
        logging.info(f"Los archivos resultantes se guardarán en: {output_dir}")

        # Procesar los archivos en chunks
        process_xml_chunks(xml_files, chunk_size, output_dir)

        logging.info("Proceso completado")

    except Exception as e:
        logging.error(f"Error en el proceso principal: {str(e)}")

if __name__ == "__main__":
    main()