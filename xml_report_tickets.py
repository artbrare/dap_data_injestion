import os
from lxml import etree
from pathlib import Path
import logging
from datetime import datetime
from tqdm import tqdm
from typing import Set, Tuple, Dict

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('xml_count_process.log'),
        logging.StreamHandler()
    ]
)

def count_tickets_and_unique_ids(xml_file: Path) -> Tuple[int, int, Set[str], Set[str]]:
    """
    Counts the number of commodity tickets and collects unique IDs
    Returns: (total_count, non_deleted_count, all_unique_ids, non_deleted_unique_ids)
    """
    try:
        parser = etree.XMLParser(recover=True)
        tree = etree.parse(str(xml_file), parser)
        root = tree.getroot()
        tickets = root.findall('commodityticket')
        
        total_count = len(tickets)
        all_unique_ids = set()
        non_deleted_unique_ids = set()
        non_deleted_count = 0
        
        for ticket in tickets:
            unique_id = ticket.get('uniqueid')
            if unique_id:
                all_unique_ids.add(unique_id)
                
                # Check if ticket is not marked as deleted
                delete_attr = ticket.get('delete', '').lower()
                if delete_attr != 'true':
                    non_deleted_count += 1
                    non_deleted_unique_ids.add(unique_id)
        
        return total_count, non_deleted_count, all_unique_ids, non_deleted_unique_ids
        
    except Exception as e:
        logging.error(f"Error processing file {xml_file.name}: {str(e)}")
        return 0, 0, set(), set()

def process_folder(folder_path: Path) -> dict:
    """
    Process all XML files in the folder and count commodity tickets
    """
    results = {
        'total_count': 0,
        'non_deleted_count': 0,
        'processed_files': 0,
        'failed_files': 0,
        'file_counts': {},
        'file_non_deleted_counts': {},
        'unique_ids': set(),
        'non_deleted_unique_ids': set()
    }

    try:
        xml_files = list(folder_path.glob('*.XML')) + list(folder_path.glob('*.xml'))
        
        if not xml_files:
            logging.warning(f"No XML files found in {folder_path}")
            return results

        logging.info(f"Starting processing of {len(xml_files)} files")

        for xml_file in tqdm(xml_files, desc="Processing files"):
            try:
                total_count, non_deleted_count, all_unique_ids, non_deleted_unique_ids = count_tickets_and_unique_ids(xml_file)
                
                results['file_counts'][xml_file.name] = total_count
                results['file_non_deleted_counts'][xml_file.name] = non_deleted_count
                results['total_count'] += total_count
                results['non_deleted_count'] += non_deleted_count
                results['unique_ids'].update(all_unique_ids)
                results['non_deleted_unique_ids'].update(non_deleted_unique_ids)
                results['processed_files'] += 1
                
            except Exception as e:
                logging.error(f"Error in file {xml_file.name}: {str(e)}")
                results['failed_files'] += 1

    except Exception as e:
        logging.error(f"Error processing folder: {str(e)}")

    return results

def generate_report(results: dict, output_path: Path) -> None:
    """
    Generates a detailed report of the results
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = output_path / f'count_report_{timestamp}.txt'

    with open(report_file, 'w') as f:
        f.write("=== COMMODITY TICKETS COUNT REPORT ===\n\n")
        f.write(f"Date and time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total commodity tickets: {results['total_count']:,}\n")
        f.write(f"Non-deleted commodity tickets: {results['non_deleted_count']:,}\n")
        f.write(f"Total unique IDs: {len(results['unique_ids']):,}\n")
        f.write(f"Non-deleted unique IDs: {len(results['non_deleted_unique_ids']):,}\n")
        f.write(f"Processed files: {results['processed_files']}\n")
        f.write(f"Files with errors: {results['failed_files']}\n\n")
        
        f.write("=== DETAIL BY FILE ===\n")
        for filename in sorted(results['file_counts'].keys()):
            total = results['file_counts'][filename]
            non_deleted = results['file_non_deleted_counts'][filename]
            f.write(f"{filename}: {total:,} total tickets, {non_deleted:,} non-deleted tickets\n")

def main():
    try:
        current_path = Path.cwd()
        folder_name = 'merged_xmlFiles_tickets'
        xml_folder = current_path / folder_name

        if not xml_folder.exists():
            logging.error(f"Folder {xml_folder} does not exist")
            return

        logging.info(f"Starting count in folder: {xml_folder}")

        results = process_folder(xml_folder)

        generate_report(results, current_path)

        logging.info(f"""
        Process Summary:
        - Total commodity tickets: {results['total_count']:,}
        - Non-deleted commodity tickets: {results['non_deleted_count']:,}
        - Total unique IDs: {len(results['unique_ids']):,}
        - Non-deleted unique IDs: {len(results['non_deleted_unique_ids']):,}
        - Processed files: {results['processed_files']}
        - Files with errors: {results['failed_files']}
        """)

    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")

if __name__ == "__main__":
    main()