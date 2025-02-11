'python processor.py'

import os
import xml.etree.ElementTree as ET
from datetime import datetime, date
import logging
from typing import Dict, List, Any
import traceback
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import pyodbc
import pandas as pd
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from threading import Lock
from sqlalchemy import create_engine, event
import urllib


class CommodityTicketsProcessor:
    '''
    Clase para procesar archivos XML de tickets de commodities
    '''

    # 1. Métodos de Inicialización y Configuración
    def __init__(self, connection_string: str, batch_size: int = 1000, debug_mode: bool = False):
        self.connection_string = connection_string
        self.batch_size = batch_size
        self.debug_mode = debug_mode
        self.lock = Lock()
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.DEBUG if self.debug_mode else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_connection(self):
        return pyodbc.connect(self.connection_string)

    # 2. Métodos Principales de Procesamiento
    def get_processed_files_cache(self) -> set:
        """Lee el archivo de caché y retorna un set con los nombres de archivos procesados"""
        cache_file = 'processed_files_cache.txt'
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    return set(line.strip() for line in f if line.strip())
            return set()
        except Exception as e:
            self.logger.error(f"Error leyendo caché: {str(e)}")
            return set()

    def add_to_processed_files_cache(self, filename: str):
        """Agrega un nombre de archivo al caché"""
        cache_file = 'processed_files_cache.txt'
        try:
            with open(cache_file, 'a') as f:
                f.write(f"{filename}\n")
        except Exception as e:
            self.logger.error(f"Error escribiendo en caché: {str(e)}")

    def process_files(self, folder_path: str):
        """Procesa todos los archivos XML en la carpeta especificada"""
        self.logger.info(
            f"Iniciando procesamiento de archivos en: {folder_path}")

        # Verificar si la carpeta existe
        if not os.path.exists(folder_path):
            self.logger.error(f"La carpeta {folder_path} no existe")
            raise FileNotFoundError(f"La carpeta {folder_path} no existe")

        # Obtener lista de archivos XML y caché de procesados
        xml_files = [f for f in os.listdir(folder_path) if f.endswith('.XML')]
        processed_files = self.get_processed_files_cache()

        # Filtrar archivos ya procesados
        files_to_process = [f for f in xml_files if f not in processed_files]

        self.logger.info(f"Se encontraron {len(xml_files)} archivos XML")
        self.logger.info(f"Archivos ya procesados: {len(processed_files)}")
        self.logger.info(
            f"Archivos pendientes de procesar: {len(files_to_process)}")

        for index, filename in enumerate(files_to_process, 1):
            try:
                self.logger.info(
                    f"Procesando archivo {index}/{len(files_to_process)}: {filename}")
                file_path = os.path.join(folder_path, filename)
                self.process_single_file(file_path, filename)

                # Agregar al caché después de procesar exitosamente
                self.add_to_processed_files_cache(filename)
                self.logger.info(f"Archivo {filename} agregado al caché")

            except Exception as e:
                self.logger.error(f"Error procesando {filename}: {str(e)}")
                if self.debug_mode:
                    raise
                continue

        self.logger.info("Procesamiento de archivos completado")

    def process_single_file(self, file_path: str, filename: str):
        """Procesa un único archivo XML"""
        try:
            self.logger.info(
                f"Iniciando procesamiento del archivo: {filename}")
            file_size = os.path.getsize(
                file_path) / (1024 * 1024)  # Tamaño en MB
            self.logger.info(f"Tamaño del archivo: {file_size:.2f} MB")

            # Iniciar el proceso en la base de datos
            self.logger.debug("Registrando proceso en la base de datos...")
            process_id = self.start_process_log(filename)
            self.logger.info(f"ID del proceso asignado: {process_id}")

            # Cargar y parsear XML
            self.logger.debug("Parseando archivo XML...")
            start_time = datetime.now()
            tree = ET.parse(file_path)
            root = tree.getroot()
            parsing_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"Archivo XML parseado en {parsing_time:.2f} segundos")

            # Extraer datos del sistema
            self.logger.debug("Extrayendo datos del sistema...")
            system_data = self.extract_system_data(root)
            self.logger.info(
                f"Datos del sistema extraídos - Tenant: {system_data['tenant_guid']}")

            # Procesar en lotes
            self.logger.info("Iniciando procesamiento por lotes...")
            self.process_in_batches(root, system_data, process_id, filename)

            # Actualizar log de proceso como completado
            self.logger.debug("Finalizando registro del proceso...")
            self.complete_process_log(process_id)

            self.logger.info(
                f"Procesamiento del archivo {filename} completado exitosamente")

        except ET.ParseError as e:
            self.logger.error(
                f"Error al parsear el archivo XML {filename}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error procesando archivo {filename}: {str(e)}")
            self.logger.error(f"Stacktrace: {traceback.format_exc()}")
            if self.debug_mode:
                raise

    def process_in_batches(self, root: ET.Element, system_data: Dict[str, str],
                           process_id: int, filename: str):
        """Procesa los registros en lotes"""
        tickets = root.findall('commodityticket')
        total_tickets = len(tickets)
        valid_tickets = len([t for t in tickets if t.get('delete') != 'true'])
        deleted_tickets = total_tickets - valid_tickets

        self.logger.info(f"Total de tickets encontrados: {total_tickets}")
        self.logger.info(f"Tickets válidos para procesar: {valid_tickets}")
        self.logger.info(f"Tickets marcados para eliminar: {deleted_tickets}")

        # Actualizar total de registros en el log
        self.update_process_total_records(process_id, valid_tickets)

        batch_number = 1
        processed_records = 0
        start_time = datetime.now()

        while processed_records < valid_tickets:
            batch_start_time = datetime.now()
            remaining_records = valid_tickets - processed_records

            self.logger.info(f"Procesando lote {batch_number}")
            self.logger.info(
                f"Registros procesados: {processed_records}/{valid_tickets}")
            self.logger.info(f"Registros restantes: {remaining_records}")

            batch_id = self.start_batch_log(
                process_id, batch_number, batch_start_time)

            try:
                # Procesar lote actual
                current_batch = tickets[processed_records:
                                        processed_records + self.batch_size]
                self.logger.debug(
                    f"Tamaño del lote actual: {len(current_batch)}")

                self.process_batch(current_batch, system_data,
                                   batch_number, filename)

                # Actualizar logs y métricas
                records_in_batch = len(current_batch)
                self.complete_batch_log(batch_id, records_in_batch)
                self.record_performance_metrics(
                    process_id, batch_id, batch_start_time)

                batch_time = (datetime.now() -
                              batch_start_time).total_seconds()
                self.logger.info(
                    f"Lote {batch_number} completado en {batch_time:.2f} segundos")

                processed_records += records_in_batch
                batch_number += 1

            except Exception as e:
                self.logger.error(f"Error en el lote {batch_number}: {str(e)}")
                self.logger.error(f"Stacktrace: {traceback.format_exc()}")
                self.log_batch_error(batch_id, str(e))
                if self.debug_mode:
                    raise

        total_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(
            f"Procesamiento por lotes completado en {total_time:.2f} segundos")
        self.logger.info(f"Total de lotes procesados: {batch_number - 1}")

    def process_batch(self, batch_tickets: List[ET.Element], system_data: Dict[str, str],
                      batch_number: int, filename: str):
        """Procesa un lote de tickets"""
        self.logger.debug(f"Iniciando procesamiento del lote {batch_number}")
        start_time = datetime.now()

        # Preparar datos para las tablas staging
        staging_data = {
            'commodity_tickets': [],
            'applications': [],
            'discounts': [],
            'grade_factors': []
        }

        processed_ticket_ids = set()
        processed_application_ids = set()

        for index, ticket in enumerate(batch_tickets, 1):
            if ticket.get('delete') == 'true':
                continue

            try:

                # Verificar si el ticket ya fue procesado
                ticket_id = ticket.get('uniqueid')
                if ticket_id in processed_ticket_ids:
                    self.logger.warning(
                        f"Ticket duplicado encontrado y omitido: {ticket_id}")
                    continue

                processed_ticket_ids.add(ticket_id)
                # Extraer datos del ticket
                ticket_data = self.extract_ticket_data(
                    ticket, system_data, batch_number, filename)
                staging_data['commodity_tickets'].append(ticket_data)

                # Extraer aplicaciones
                applications = ticket.findall('applications/application')

                for app_index, app in enumerate(applications, 1):

                    app_id = f"{ticket.get('ticketlocation')}:{ticket.get('ticketnumber')}:" \
                        f"{ticket.get('shiptofromid')}-{app.get('applyno')}-{ticket.get('uniqueid')}"


                    if app_id in processed_application_ids:
                        self.logger.warning(
                            f"Aplicación duplicada encontrada y omitida: {app_id}")
                        continue

                    processed_application_ids.add(app_id)

                    app_data = self.extract_application_data(app, ticket, system_data, batch_number)
                    staging_data['applications'].append(app_data)

                    # Extraer descuentos
                    discounts = app.findall('discounts/discount')

                    for discount in discounts:
                        discount_data = self.extract_discount_data(
                            discount, app, ticket, system_data, batch_number)
                        staging_data['discounts'].append(discount_data)

                # Extraer factores de grado
                grade_factors = ticket.findall('gradefactors/gradefactor')

                for grade_factor in grade_factors:
                    grade_factor_data = self.extract_grade_factor_data(
                        grade_factor, ticket, system_data, batch_number)
                    staging_data['grade_factors'].append(grade_factor_data)

            except Exception as e:
                self.logger.error(
                    f"Error procesando ticket {index} en lote {batch_number}: {str(e)}")
                raise

        # Registrar estadísticas del lote
        self.logger.info(f"Estadísticas del lote {batch_number}:")
        self.logger.info(
            f"- Tickets procesados: {len(staging_data['commodity_tickets'])}")
        self.logger.info(
            f"- Aplicaciones procesadas: {len(staging_data['applications'])}")
        self.logger.info(
            f"- Descuentos procesados: {len(staging_data['discounts'])}")
        self.logger.info(
            f"- Factores de grado procesados: {len(staging_data['grade_factors'])}")

        # Actualizar la base de datos
        self.logger.debug("Iniciando actualización de la base de datos...")
        with self.get_connection() as conn:
            self.update_database_tables(conn, staging_data)

        processing_time = (datetime.now() - start_time).total_seconds()
        self.logger.info(
            f"Lote {batch_number} procesado en {processing_time:.2f} segundos")

    # 3. Métodos de Extracción de Datos
    def extract_system_data(self, root: ET.Element) -> Dict[str, str]:
        """Extrae los datos del sistema del XML"""
        system = root.find('system')
        return {
            'tenant_guid': system.get('licenseguid'),
            'dataset_guid': system.get('datasetguid')
        }

    def extract_ticket_data(self, ticket: ET.Element, system_data: Dict[str, str],
                            batch_number: int, filename: str) -> Dict[str, Any]:
        """
        Extrae todos los datos de un ticket del XML.

        Args:
            ticket: Elemento XML del ticket
            system_data: Datos del sistema (tenant_guid, dataset_guid)
            batch_number: Número del lote actual
            filename: Nombre del archivo fuente

        Returns:
            Diccionario con todos los campos del ticket
        """
        try:
            # Calcular start_datetime y end_datetime
            gross_datetime = self.parse_datetime(ticket.get('grossdatetime'))
            tare_datetime = self.parse_datetime(ticket.get('taredatetime'))

            start_datetime = min(
                gross_datetime, tare_datetime) if gross_datetime and tare_datetime else None
            end_datetime = max(
                gross_datetime, tare_datetime) if gross_datetime and tare_datetime else None

            return {
                'tenant_guid': system_data['tenant_guid'],
                'dataset_guid': system_data['dataset_guid'],
                'unique_id': ticket.get('uniqueid'),
                'integration_guid': ticket.get('integrationguid'),
                'in_out_code': ticket.get('inoutcode'),
                'in_out_description': ticket.get('inoutdescription'),
                'ticket_location': ticket.get('ticketlocation'),
                'ticket_location_description': ticket.get('ticketlocationdescription'),
                'ticket_number': ticket.get('ticketnumber'),
                'ship_to_from_id': ticket.get('shiptofromid'),
                'ship_to_from_description': ticket.get('shiptofromdescription'),
                'shipper_id': ticket.get('shipperid'),
                'shipper_description': ticket.get('shipperdescription'),
                'hauler_id': ticket.get('haulerid'),
                'hauler_description': ticket.get('haulerdescription'),
                'producer_id': ticket.get('producerid'),
                'producer_description': ticket.get('producerdescription'),
                'farm': ticket.get('farm'),
                'farm_description': ticket.get('farmdescription'),
                'field': ticket.get('field'),
                'field_description': ticket.get('fielddescription'),
                'shipment_date': self.parse_date(ticket.get('shipmentdate')),
                'entry_date': self.parse_date(ticket.get('entrydate')),
                'type': ticket.get('type'),
                'type_description': ticket.get('typedescription'),
                'commodity': ticket.get('commodity'),
                'commodity_description': ticket.get('commoditydescription'),
                'variety': ticket.get('variety'),
                'class': ticket.get('class'),
                'variety_description': ticket.get('varietydescription'),
                'storage_bin': ticket.get('storagebin'),
                'storage_bin_description': ticket.get('storagebindescription'),
                'transport_mode': ticket.get('transportmode'),
                'transport_description': ticket.get('transportdescription'),
                'vehicle_id': ticket.get('vehicleid'),
                'other_ref': ticket.get('otherref'),
                'gross_date_time': gross_datetime,
                'tare_date_time': tare_datetime,
                'gross_entry_method': ticket.get('grossentrymethod'),
                'tare_entry_method': ticket.get('tareentrymethod'),
                'driver_on': ticket.get('driveron'),
                'freight_voucher': ticket.get('freightvoucher'),
                'gross_weight': self.parse_decimal(ticket.get('grossweight')),
                'tare_weight': self.parse_decimal(ticket.get('tareweight')),
                'net_weight': self.parse_decimal(ticket.get('netweight')),
                'uom': ticket.get('uom'),
                'uom_description': ticket.get('uomdesc'),
                'gross_quantity': self.parse_decimal(ticket.get('grossquantity')),
                'deduct_quantity': self.parse_decimal(ticket.get('deductquantity')),
                'net_quantity': self.parse_decimal(ticket.get('netquantity')),
                'freight_weight': self.parse_decimal(ticket.get('freightweight')),
                'freight_quantity': self.parse_decimal(ticket.get('freightquantity')),
                'freight_rate': self.parse_decimal(ticket.get('freightrate')),
                'additional_freight': self.parse_decimal(ticket.get('additionalfreight')),
                'freight_total': self.parse_decimal(ticket.get('freighttotal')),
                'freight_tax_percent': self.parse_decimal(ticket.get('freighttaxpercent')),
                'cash_price': self.parse_decimal(ticket.get('cashprice')),
                'cash_basis': self.parse_decimal(ticket.get('cashbasis')),
                'car_set_date': self.parse_date(ticket.get('carsetdate')),
                'notify_date': self.parse_date(ticket.get('notifydate')),
                'days_allowed': self.parse_int(ticket.get('daysallowed')),
                'days_over': self.parse_int(ticket.get('daysover')),
                'days_used': self.parse_int(ticket.get('daysused')),
                'weight_base': ticket.get('weightbase'),
                'weight_base_description': ticket.get('weightbasedescription'),
                'grade_base': ticket.get('gradebase'),
                'grade_base_description': ticket.get('gradebasedescription'),
                'shipment_id': ticket.get('shipmentid'),
                'ticket_status': ticket.get('ticketstatus'),
                'ticket_status_description': ticket.get('ticketstatusdescription'),
                'freight_status': ticket.get('freightstatus'),
                'freight_status_description': ticket.get('freightstatusdescription'),
                'sample_number': self.parse_int(ticket.get('samplenumber')),
                'name_id_type': ticket.get('nameidtype'),
                'name_id_type_description': ticket.get('nameidtypedescription'),
                'id_number': ticket.get('idnumber'),
                'weight_uom': ticket.get('weightuom'),
                'weight_uom_description': ticket.get('weightuomdescription'),
                'freight_uom': ticket.get('freightuom'),
                'freight_uom_description': ticket.get('freightuomdescription'),
                'freight_currency': ticket.get('freightcurrency'),
                'freight_currency_description': ticket.get('freightcurrencydescription'),
                'exchange_rate': self.parse_decimal(ticket.get('exchangerate')),
                'exchange_rate_date': self.parse_date(ticket.get('exchangeratedate')),
                'their_invoice_number': ticket.get('theirinvoicenumber'),
                'exec_id': ticket.get('execid'),
                'exec_id_description': ticket.get('execiddescription'),
                'last_change_date_time': self.parse_datetime(ticket.get('lastchangedatetime')),
                'last_change_user_id': ticket.get('lastchangeuserid'),
                'last_change_user_name': ticket.get('lastchangeusername'),
                'primary_ticket_in_out_code': ticket.get('primaryticketinoutcode'),
                'primary_ticket_in_out_description': ticket.get('primaryticketinoutdescription'),
                'primary_ticket_location': ticket.get('primaryticketlocation'),
                'primary_ticket_location_description': ticket.get('primaryticketlocationdescription'),
                'primary_ticket_number': ticket.get('primaryticketnumber'),
                'match_status': ticket.get('matchstatus'),
                'match_status_description': ticket.get('matchstatusdescription'),
                'match_date': self.parse_datetime(ticket.get('matchdate')),
                'forced_match_user_id': ticket.get('forcedmatchuserid'),
                'forced_match_user_name': ticket.get('forcedmatchusername'),
                'preadvice_location': ticket.get('preadvicelocation'),
                'preadvice_location_description': ticket.get('preadvicelocationdescription'),
                'preadvice_group_number': ticket.get('preadvicegroupnumber'),
                'preadvice_shipment_line_number': ticket.get('preadviceshipmentlinenumber'),
                'preadvice_application_line_number': ticket.get('preadviceapplicationlinenumber'),
                'ticket_split_group': ticket.get('ticketsplitgroup'),
                'ticket_split_group_description': ticket.get('ticketsplitgroupdescription'),
                'start_datetime': start_datetime,
                'end_datetime': end_datetime,
                'date_created': datetime.now(),
                'date_modified': datetime.now(),
                'date_deleted': None,
                'delete': False,
                'source_filename': filename  # Nueva columna para el nombre del archivo
            }
        except Exception as e:
            self.logger.error(f"Error extracting ticket data: {str(e)}")
            raise

    def extract_application_data(self, app: ET.Element, ticket: ET.Element,
                                 system_data: Dict[str, str], batch_number: int) -> Dict[str, Any]:
        """
        Extrae los datos de aplicación del XML.

        Args:
            app: Elemento XML de la aplicación
            ticket: Elemento XML del ticket padre
            system_data: Datos del sistema (tenant_guid, dataset_guid)
            batch_number: Número del lote actual

        Returns:
            Diccionario con todos los campos de la aplicación
        """
        try:
            # Construir el ID único de la aplicación
            application_id = f"{ticket.get('ticketlocation')}:{ticket.get('ticketnumber')}:" \
                f"{ticket.get('shiptofromid')}-{app.get('applyno')}-{ticket.get('uniqueid')}"

            return {
                '_id': application_id,
                'tenant_guid': system_data['tenant_guid'],
                'dataset_guid': system_data['dataset_guid'],
                'unique_id': ticket.get('uniqueid'),
                'apply_no': app.get('applyno'),
                'apply_type': app.get('applytype'),
                'apply_description': app.get('applydescription'),
                'expected_apply_type': app.get('expectedapplytype'),
                'expected_apply_description': app.get('expectedapplydescription'),
                'apply_location': app.get('applylocation'),
                'apply_location_description': app.get('applylocationdescription'),
                'apply_reference': app.get('applyreference'),
                'apply_name_id': app.get('applynameid'),
                'apply_name_description': app.get('applynamedescription'),
                'apply_date': self.parse_date(app.get('applydate')),
                'delivery_sheet': app.get('deliverysheet'),
                'settlement_date': self.parse_date(app.get('settlementdate')),
                'settlement_purchase_sales': app.get('settlementpurchasesales'),
                'settlement_purchase_sales_description': app.get('settlementpurchasesalesdesc'),
                'settlement_location': app.get('settlementlocation'),
                'settlement_location_description': app.get('settlementlocationdesc'),
                'settlement_no': app.get('settlementno'),
                'application_gross_quantity': self.parse_decimal(app.get('grossquantity')),
                'application_net_quantity': self.parse_decimal(app.get('netquantity')),
                'position_quantity': self.parse_decimal(app.get('positionquantity')),
                'price': self.parse_decimal(app.get('price')),
                'dollar_discount': self.parse_decimal(app.get('dollardiscount')),
                'ticket_total': self.parse_decimal(app.get('tickettotal')),
                'freight_adjustment': self.parse_decimal(app.get('freightadjustment')),
                'freight_adjusted': self.parse_decimal(app.get('freightadjusted')),
                'in_transit': app.get('intransit'),
                'quantity_uom': app.get('quantityuom'),
                'quantity_uom_description': app.get('quantityuomdescription'),
                'price_uom': app.get('priceuom'),
                'price_uom_description': app.get('priceuomdescription'),
                'apply_net_quantity': self.parse_decimal(app.get('applynetquantity')),
                'apply_gross_quantity': self.parse_decimal(app.get('applygrossquantity')),
                'currency': app.get('currency'),
                'currency_description': app.get('currencydescription'),
                'discount_schedule_code': app.get('discountschedulecode'),
                'discount_schedule_code_description': app.get('discountschedulecodedescription'),
                'recalculate_discounts': app.get('recalculatediscounts'),
                'date_created': datetime.now(),
                'date_modified': datetime.now(),
                'date_deleted': None,
                'delete': False,
                'last_change_date_time_commodity_tickets': self.parse_datetime(ticket.get('lastchangedatetime')),
            }
        except Exception as e:
            self.logger.error(f"Error extracting application data: {str(e)}")
            error_details = {
                'ticket_id': ticket.get('uniqueid'),
                'apply_no': app.get('applyno'),
                'error': str(e)
            }
            self.logger.error(
                f"Application extraction error details: {error_details}")
            raise

    def extract_discount_data(self, discount: ET.Element, app: ET.Element,
                              ticket: ET.Element, system_data: Dict[str, str],
                              batch_number: int) -> Dict[str, Any]:
        """
        Extrae los datos de descuento del XML.
        """
        try:
            # Construir el ID de la aplicación (usado como foreign key)
            application_id = f"{ticket.get('ticketlocation')}:{ticket.get('ticketnumber')}:" \
                f"{ticket.get('shiptofromid')}-{app.get('applyno')}-{ticket.get('uniqueid')}"

            return {
                'tenant_guid': system_data['tenant_guid'],
                'dataset_guid': system_data['dataset_guid'],
                'commodity_ticket_application_id': application_id,
                'apply_no': app.get('applyno'),
                'number': self.parse_int(discount.get('number')),
                'code': discount.get('code'),
                'description': discount.get('description'),
                'grade_factor': self.parse_decimal(discount.get('gradefactor')),
                'table': discount.get('table'),
                'table_description': discount.get('tabledescription'),
                'rate': self.parse_decimal(discount.get('rate')),
                'rate_type': discount.get('ratetype'),
                'rate_description': discount.get('ratedescription'),
                'dollar_discount': self.parse_decimal(discount.get('dollardiscount')),
                'apply_unit_discount': self.parse_decimal(discount.get('applyunitdiscount')),
                'unit_discount': self.parse_decimal(discount.get('unitdiscount')),
                'date_created': datetime.now(),
                'date_modified': datetime.now(),
                'date_deleted': None,
                'delete': False,
                'last_change_date_time_commodity_tickets': self.parse_datetime(ticket.get('lastchangedatetime'))
            }
        except Exception as e:
            self.logger.error(f"Error extracting discount data: {str(e)}")
            error_details = {
                'ticket_id': ticket.get('uniqueid'),
                'apply_no': app.get('applyno'),
                'discount_number': discount.get('number'),
                'error': str(e)
            }
            self.logger.error(
                f"Discount extraction error details: {error_details}")
            raise

    def extract_grade_factor_data(self, grade_factor: ET.Element, ticket: ET.Element,
                                  system_data: Dict[str, str], batch_number: int) -> Dict[str, Any]:
        """
        Extrae los datos de factor de grado del XML.

        Args:
            grade_factor: Elemento XML del factor de grado
            ticket: Elemento XML del ticket padre
            system_data: Datos del sistema
            batch_number: Número del lote actual

        Returns:
            Diccionario con todos los campos del factor de grado
        """
        try:
            return {
                'tenant_guid': system_data['tenant_guid'],
                'dataset_guid': system_data['dataset_guid'],
                'commodity_ticket_id': None,  # Se establece después de insertar el ticket
                'number': self.parse_int(grade_factor.get('number')),
                'code': grade_factor.get('code'),
                'description': grade_factor.get('description'),
                'grade_factor': self.parse_decimal(grade_factor.get('gradefactor')),
                'table': grade_factor.get('table'),
                'table_description': grade_factor.get('tabledescription'),
                'date_created': datetime.now(),
                'date_modified': datetime.now(),
                'date_deleted': None,
                'delete': False,
                'unique_id': ticket.get('uniqueid'),
                'last_change_date_time_commodity_tickets': self.parse_datetime(ticket.get('lastchangedatetime')),
            }
        except Exception as e:
            self.logger.error(f"Error extracting grade factor data: {str(e)}")
            error_details = {
                'ticket_id': ticket.get('uniqueid'),
                'grade_factor_number': grade_factor.get('number'),
                'error': str(e)
            }
            self.logger.error(
                f"Grade factor extraction error details: {error_details}")
            raise

    # 4. Métodos de Parsing
    def parse_datetime(self, value: str) -> datetime:
        """Convierte string a datetime"""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None

    def parse_date(self, value: str) -> datetime.date:
        """Convierte string a date"""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.split('T')[0]).date()
        except (ValueError, AttributeError):
            return None

    def parse_decimal(self, value: str) -> float:
        """Convierte string a decimal"""
        if not value:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def parse_int(self, value: str) -> int:
        """Convierte string a integer"""
        if not value:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # 5. Métodos de Validación
    def validate_application_data(self, data: Dict[str, Any]) -> bool:
        """
        Valida los datos de la aplicación antes de la inserción.

        Args:
            data: Diccionario con los datos de la aplicación

        Returns:
            bool: True si los datos son válidos, False en caso contrario
        """
        required_fields = [
            '_id',
            'tenant_guid',
            'dataset_guid',
            'unique_id',
            'apply_no'
        ]

        try:
            # Verificar campos requeridos
            for field in required_fields:
                if not data.get(field):
                    self.logger.warning(
                        f"Missing required field in application: {field}")
                    return False

            # Validar formatos numéricos
            numeric_fields = [
                'application_gross_quantity',
                'application_net_quantity',
                'position_quantity',
                'price',
                'dollar_discount',
                'ticket_total',
                'freight_adjustment',
                'freight_adjusted',
                'apply_net_quantity',
                'apply_gross_quantity'
            ]

            for field in numeric_fields:
                value = data.get(field)
                if value is not None and not isinstance(value, (int, float)):
                    self.logger.warning(
                        f"Invalid numeric value in field: {field}")
                    return False

            # Validar fechas
            date_fields = [
                'apply_date',
                'settlement_date',
                'date_created',
                'date_modified',
                'date_deleted',
                'last_change_date_time_commodity_tickets'
            ]

            for field in date_fields:
                value = data.get(field)
                if value is not None and not isinstance(value, (datetime, datetime.date)):
                    self.logger.warning(
                        f"Invalid date value in field: {field}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating application data: {str(e)}")
            return False

    def validate_discount_data(self, data: Dict[str, Any]) -> bool:
        """
        Valida los datos del descuento antes de la inserción.

        Args:
            data: Diccionario con los datos del descuento

        Returns:
            bool: True si los datos son válidos, False en caso contrario
        """
        required_fields = [
            'tenant_guid',
            'dataset_guid',
            'commodity_ticket_application_id',
            'apply_no',
            'number'
        ]

        try:
            # Verificar campos requeridos
            for field in required_fields:
                if not data.get(field):
                    self.logger.warning(
                        f"Missing required field in discount: {field}")
                    return False

            # Validar campos numéricos
            numeric_fields = [
                'grade_factor',
                'rate',
                'dollar_discount',
                'apply_unit_discount',
                'unit_discount'
            ]

            for field in numeric_fields:
                value = data.get(field)
                if value is not None and not isinstance(value, (int, float)):
                    self.logger.warning(
                        f"Invalid numeric value in discount field: {field}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating discount data: {str(e)}")
            return False

    def validate_grade_factor_data(self, data: Dict[str, Any]) -> bool:
        """
        Valida los datos del factor de grado antes de la inserción.

        Args:
            data: Diccionario con los datos del factor de grado

        Returns:
            bool: True si los datos son válidos, False en caso contrario
        """
        required_fields = [
            'tenant_guid',
            'dataset_guid',
            'number',
            'unique_id'
        ]

        try:
            # Verificar campos requeridos
            for field in required_fields:
                if not data.get(field):
                    self.logger.warning(
                        f"Missing required field in grade factor: {field}")
                    return False

            # Validar el campo grade_factor
            grade_factor_value = data.get('grade_factor')
            if grade_factor_value is not None and not isinstance(grade_factor_value, (int, float)):
                self.logger.warning("Invalid grade factor value")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error validating grade factor data: {str(e)}")
            return False

    # 6. Métodos de Actualización de Base de Datos
    def update_database_tables(self, conn, staging_data: Dict[str, List[Dict[str, Any]]]):
        """Actualiza las tablas de la base de datos usando inserción masiva"""
        try:
            self.logger.info("Iniciando actualización masiva de tablas")

            # Verificar y eliminar duplicados en commodity_tickets
            if staging_data['commodity_tickets']:
                original_count = len(staging_data['commodity_tickets'])
                # Convertir a DataFrame para mejor manejo de duplicados
                df_tickets = pd.DataFrame(staging_data['commodity_tickets'])
                df_tickets.drop_duplicates(subset=['unique_id'], keep='last', inplace=True)
                duplicates_removed = original_count - len(df_tickets)
                if duplicates_removed > 0:
                    self.logger.warning(f"Se encontraron {duplicates_removed} tickets duplicados (unique_id)")
                staging_data['commodity_tickets'] = df_tickets.to_dict('records')

            # Verificar duplicados en applications
            if staging_data['applications']:
                original_count = len(staging_data['applications'])
                df_apps = pd.DataFrame(staging_data['applications'])
                df_apps.drop_duplicates(subset=['_id'], keep='last', inplace=True)
                duplicates_removed = original_count - len(df_apps)
                if duplicates_removed > 0:
                    self.logger.warning(f"Se encontraron {duplicates_removed} aplicaciones duplicadas (_id)")
                staging_data['applications'] = df_apps.to_dict('records')

            # Verificar duplicados en discounts (clave compuesta)
            if staging_data['discounts']:
                original_count = len(staging_data['discounts'])
                df_discounts = pd.DataFrame(staging_data['discounts'])
                df_discounts.drop_duplicates(
                    subset=['commodity_ticket_application_id', 'number'], 
                    keep='last', 
                    inplace=True
                )
                duplicates_removed = original_count - len(df_discounts)
                if duplicates_removed > 0:
                    self.logger.warning(f"Se encontraron {duplicates_removed} descuentos duplicados")
                staging_data['discounts'] = df_discounts.to_dict('records')

            # Verificar duplicados en grade_factors (clave compuesta)
            if staging_data['grade_factors']:
                original_count = len(staging_data['grade_factors'])
                df_factors = pd.DataFrame(staging_data['grade_factors'])
                df_factors.drop_duplicates(
                    subset=['unique_id', 'number'], 
                    keep='last', 
                    inplace=True
                )
                duplicates_removed = original_count - len(df_factors)
                if duplicates_removed > 0:
                    self.logger.warning(f"Se encontraron {duplicates_removed} factores de grado duplicados")
                staging_data['grade_factors'] = df_factors.to_dict('records')
            
            # Procesar cada tipo de dato en orden
            # self.bulk_insert_data(conn, 'commodity_tickets', staging_data['commodity_tickets'])
            self.bulk_insert_data(conn, 'commodity_tickets_applications', staging_data['applications'])
            self.bulk_insert_data(conn, 'commodity_tickets_discounts', staging_data['discounts'])
            self.bulk_insert_data(conn, 'commodity_tickets_grade_factors', staging_data['grade_factors'])
            
            self.logger.info("Actualización masiva completada exitosamente")
            
        except Exception as e:
            self.logger.error(f"Error en actualización masiva: {str(e)}")
            raise

    def create_temp_table_with_schema(self, cursor, table_name: str, temp_table: str):
        """Crea una tabla temporal copiando el esquema exacto de la tabla original"""
        try:
            # Verificar si la tabla temporal existe y eliminarla
            cursor.execute(f"""
                IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL 
                    DROP TABLE {temp_table}
            """)
            cursor.commit()

            # Obtener la definición exacta de la tabla
            cursor.execute(f"""
                SELECT c.COLUMN_NAME, 
                    c.DATA_TYPE,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.NUMERIC_PRECISION,
                    c.NUMERIC_SCALE,
                    c.IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = '{table_name}'
                AND (c.COLUMN_NAME != '_id' OR 
                    ('{table_name}' NOT IN ('commodity_tickets_discounts', 
                                        'commodity_tickets_grade_factors')))
                ORDER BY c.ORDINAL_POSITION
            """)

            columns = cursor.fetchall()
            if not columns:
                raise ValueError(
                    f"No se encontraron columnas para la tabla {table_name}")

            if table_name == 'commodity_tickets':
                has_source_filename = any(
                    col[0] == 'source_filename' for col in columns)
                self.logger.info(
                    f"¿Columna source_filename encontrada? {has_source_filename}")

            column_definitions = []

            for col in columns:
                col_name, data_type, char_max_length, numeric_precision, numeric_scale, is_nullable = col

                # Construir la definición de la columna
                if data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                    length_spec = f"({char_max_length})" if char_max_length != - \
                        1 else "(max)"
                    column_definitions.append(
                        f"[{col_name}] {data_type}{length_spec}")
                elif data_type in ('decimal', 'numeric'):
                    column_definitions.append(
                        f"[{col_name}] {data_type}({numeric_precision}, {numeric_scale})")
                else:
                    column_definitions.append(f"[{col_name}] {data_type}")

                # Agregar NULL/NOT NULL
                column_definitions[-1] += " NULL" if is_nullable == 'YES' else " NOT NULL"

            create_sql = f"""
                IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL 
                    DROP TABLE {temp_table};
                
                CREATE TABLE {temp_table} (
                    {', '.join(column_definitions)}
                );
            """

            self.logger.debug(f"Creando tabla temporal {temp_table}")
            cursor.execute(create_sql)

            # Retornar lista de nombres de columnas
            return [col[0] for col in columns]

        except Exception as e:
            self.logger.error(f"Error creando tabla temporal: {str(e)}")
            self.logger.error(
                f"SQL: {create_sql if 'create_sql' in locals() else 'SQL no generado'}")
            raise

    def get_valid_columns(self, cursor, table_name: str) -> List[str]:
        """Obtiene las columnas válidas de una tabla"""
        cursor.execute(f"""
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_NAME = '{table_name}'
            AND (c.COLUMN_NAME != '_id' OR 
                ('{table_name}' NOT IN ('commodity_tickets_discounts', 
                                    'commodity_tickets_grade_factors')))
            ORDER BY c.ORDINAL_POSITION
        """)
        return [row[0] for row in cursor.fetchall()]

    def bulk_insert_data(self, conn, table_name: str, data: List[Dict[str, Any]], max_workers: int = 12, batch_size: int = 1000):
        """
        Realiza inserción masiva de datos con procesamiento paralelo.
        
        Args:
            conn: Conexión a la base de datos
            table_name: Nombre de la tabla
            data: Lista de diccionarios con los datos
            max_workers: Número máximo de workers para procesamiento paralelo
        """
        if not data:
            self.logger.info(f"No hay datos para insertar en {table_name}")
            return

        cursor = conn.cursor()
        start_time = datetime.now()
        self.logger.info(f"Iniciando inserción masiva en {table_name} - {len(data)} registros a procesar")
        
        try:
            # Convertir a DataFrame
            df = pd.DataFrame(data)
            
            # Diagnóstico inicial para source_filename
            if table_name == 'commodity_tickets':
                self.logger.info("Verificando datos iniciales de source_filename:")
                if 'source_filename' in df.columns:
                    self.logger.info(f"Valores únicos de source_filename: {df['source_filename'].unique()}")
                    self.logger.info(f"Muestra de registros con source_filename:\n{df[['unique_id', 'source_filename']].head()}")
                else:
                    self.logger.warning("source_filename no encontrado en las columnas iniciales")
            
            # Configuración de tabla
            table_config = {
                'commodity_tickets': {
                    'key_field': 'unique_id',
                    'version_field': 'last_change_date_time'
                },
                'commodity_tickets_applications': {
                    'key_field': '_id',
                    'version_field': 'last_change_date_time_commodity_tickets'
                },
                'commodity_tickets_discounts': {
                    'key_fields': ['commodity_ticket_application_id', 'number'],
                    'version_field': 'last_change_date_time_commodity_tickets'
                },
                'commodity_tickets_grade_factors': {
                    'key_fields': ['unique_id', 'number'],
                    'version_field': 'last_change_date_time_commodity_tickets'
                }
            }
            
            config = table_config[table_name]
            
            # Crear tabla temporal y obtener columnas válidas
            temp_table = f"##temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            valid_columns = self.create_temp_table_with_schema(cursor, table_name, temp_table)
            
            # Verificar si source_filename está en las columnas válidas
            if table_name == 'commodity_tickets':
                self.logger.info(f"Columnas válidas para la tabla temporal: {valid_columns}")
                if 'source_filename' in valid_columns:
                    self.logger.info("source_filename está en las columnas válidas")
                else:
                    self.logger.warning("source_filename no está en las columnas válidas")
            
            conn.commit()

            # Filtrar DataFrame para incluir solo columnas válidas
            df = df[[col for col in df.columns if col in valid_columns]]
            columns = list(df.columns)
            columns_str = ', '.join(f"[{col}]" for col in columns)
            
            self.logger.debug(f"Columnas a procesar: {columns}")
            
            def process_batch(batch_data):
                try:
                    with self.get_connection() as batch_conn:
                        batch_cursor = batch_conn.cursor()
                        rows_processed = 0
                        
                        # Obtener los tamaños máximos de las columnas
                        batch_cursor.execute(f"""
                            SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = '{table_name}'
                            AND DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar')
                        """)
                        column_lengths = {row[0]: row[1] for row in batch_cursor.fetchall()}
                        
                        # Procesar cada fila individualmente
                        for _, row in batch_data.iterrows():
                            # Truncar strings que excedan el tamaño máximo
                            processed_values = []
                            for col, value in zip(columns, row):
                                if pd.isna(value):
                                    processed_values.append(None)
                                elif isinstance(value, str) and col in column_lengths:
                                    max_length = column_lengths[col]
                                    if max_length != -1:  # -1 significa MAX
                                        processed_values.append(value[:max_length])
                                    else:
                                        processed_values.append(value)
                                else:
                                    processed_values.append(value)
                            
                            # Verificar valores de source_filename
                            if table_name == 'commodity_tickets':
                                try:
                                    filename_index = columns.index('source_filename')
                                    self.logger.debug(f"Valor de source_filename en fila: {processed_values[filename_index]}")
                                except ValueError:
                                    self.logger.warning("source_filename no encontrado en columnas procesadas")
                            
                            # Insertar fila
                            placeholders = '(' + ','.join(['?' for _ in processed_values]) + ')'
                            insert_sql = f"""
                                INSERT INTO {temp_table} ({columns_str})
                                VALUES {placeholders}
                            """
                            
                            try:
                                batch_cursor.execute(insert_sql, processed_values)
                                rows_processed += 1
                                
                                # Commit cada 100 filas
                                if rows_processed % 500 == 0:
                                    batch_conn.commit()
                                    
                            except Exception as row_error:
                                self.logger.error(f"Error insertando fila: {str(row_error)}")
                                self.logger.error(f"Valores: {processed_values}")
                                raise
                        
                        # Commit final
                        batch_conn.commit()
                        return rows_processed
                                
                except Exception as e:
                    self.logger.error(f"Error procesando lote: {str(e)}")
                    raise

            # Dividir datos en lotes
            batches = [df[i:i + batch_size] for i in range(0, len(df), batch_size)]
            total_rows = 0
            
            # Procesar lotes en paralelo
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {executor.submit(process_batch, batch): i 
                                for i, batch in enumerate(batches)}
                
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch_index = future_to_batch[future]
                    try:
                        rows_processed = future.result()
                        total_rows += rows_processed
                        self.logger.info(f"Lote {batch_index + 1}/{len(batches)} completado: {rows_processed} registros")
                        self.logger.info(f"Progreso total: {total_rows}/{len(df)} registros")
                    except Exception as e:
                        self.logger.error(f"Error en lote {batch_index}: {str(e)}")
                        raise

            # Verificar datos en tabla temporal antes de la inserción final
            if table_name == 'commodity_tickets':
                cursor.execute(f"SELECT COUNT(*) FROM {temp_table} WHERE source_filename IS NOT NULL")
                count_with_filename = cursor.fetchone()[0]
                self.logger.info(f"Registros con source_filename en tabla temporal: {count_with_filename}")

            # Crear índices en la tabla temporal para mejorar rendimiento
            if 'key_fields' in config:
                key_condition = ' AND '.join([f"target.[{k}] = source.[{k}]" for k in config['key_fields']])
                for key in config['key_fields']:
                    cursor.execute(f"CREATE INDEX IX_{temp_table}_{key} ON {temp_table}([{key}])")
            else:
                key_condition = f"target.[{config['key_field']}] = source.[{config['key_field']}]"
                cursor.execute(f"CREATE INDEX IX_{temp_table}_{config['key_field']} ON {temp_table}([{config['key_field']}])")
            
            # Actualizar registros existentes
            self.logger.debug("Ejecutando UPDATE")
            update_sql = f"""
                UPDATE target
                SET {', '.join(f'target.[{col}] = source.[{col}]' for col in columns)}
                FROM {table_name} target
                INNER JOIN {temp_table} source ON {key_condition}
                WHERE source.[{config['version_field']}] > target.[{config['version_field']}]
            """
            cursor.execute(update_sql)
            updates = cursor.rowcount
            
            # Insertar nuevos registros
            self.logger.debug("Ejecutando INSERT")
            if 'key_fields' in config:
                # Para tablas con claves compuestas
                insert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {', '.join(f'source.[{col}]' for col in columns)}
                    FROM {temp_table} source
                    LEFT JOIN {table_name} target ON {key_condition}
                    WHERE target.{config['key_fields'][0]} IS NULL
                    AND target.{config['key_fields'][1]} IS NULL
                """
            else:
                # Para tablas con clave simple
                insert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {', '.join(f'source.[{col}]' for col in columns)}
                    FROM {temp_table} source
                    LEFT JOIN {table_name} target ON {key_condition}
                    WHERE target.{config['key_field']} IS NULL
                """

            cursor.execute(insert_sql)
            inserts = cursor.rowcount
            
            # Verificar resultados finales
            if table_name == 'commodity_tickets':
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE source_filename IS NOT NULL")
                final_count = cursor.fetchone()[0]
                self.logger.info(f"Registros finales con source_filename: {final_count}")
            
            total_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"""
                Resumen de procesamiento para {table_name}:
                - Registros actualizados: {updates}
                - Registros insertados: {inserts}
                - Tiempo total: {total_time:.2f} segundos
                - Velocidad promedio: {len(df)/total_time:.2f} registros/segundo
            """)
            
        except Exception as e:
            self.logger.error(f"Error en bulk_insert_data para {table_name}: {str(e)}")
            self.logger.error(f"Detalles del error: {traceback.format_exc()}")
            raise
        finally:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                conn.commit()
            except Exception as cleanup_error:
                self.logger.warning(f"Error durante la limpieza: {str(cleanup_error)}")
    # 7. Métodos de Logging y Seguimiento
    def start_process_log(self, filename: str) -> int:
        """Inicia el registro del proceso y retorna el ID del proceso"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # En SQL Server, podemos usar OUTPUT para obtener el ID insertado
            sql = """
                INSERT INTO [dbo].[ProcessLog_Historical]
                (ProcessName, StartTime, Status, Parameters, FileName, BatchSize)
                OUTPUT INSERTED.LogId
                VALUES (?, ?, ?, ?, ?, ?)
            """

            params = (
                'CommodityTicketsProcess_CPI_HistoricalData_Python',
                datetime.now(),
                'Started',
                f'BatchSize={self.batch_size};DebugMode={self.debug_mode}',
                filename,
                self.batch_size
            )

            # Ejecutar el INSERT con OUTPUT y obtener el ID
            row = cursor.execute(sql, params).fetchone()
            process_id = row[0]

            conn.commit()
            self.logger.debug(f"Proceso registrado con ID: {process_id}")
            return process_id

    def log_application_error(self, error: Exception, ticket_id: str, apply_no: str):
        """
        Registra errores específicos de la extracción de aplicaciones.

        Args:
            error: Excepción capturada
            ticket_id: ID del ticket
            apply_no: Número de aplicación
        """
        error_data = {
            'timestamp': datetime.now(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'ticket_id': ticket_id,
            'apply_no': apply_no,
            'stack_trace': traceback.format_exc()
        }

        self.logger.error(f"Application extraction error: {error_data}")

    def log_extraction_error(self, error: Exception, data_type: str,
                             ticket_id: str, additional_info: Dict[str, Any] = None):
        """
        Registra errores de extracción de manera uniforme.

        Args:
            error: Excepción capturada
            data_type: Tipo de datos (discount/grade_factor)
            ticket_id: ID del ticket
            additional_info: Información adicional para el log
        """
        error_data = {
            'timestamp': datetime.now(),
            'data_type': data_type,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'ticket_id': ticket_id,
            'additional_info': additional_info or {},
            'stack_trace': traceback.format_exc()
        }

        self.logger.error(f"Data extraction error: {error_data}")

    def update_process_total_records(self, process_id: int, total_records: int):
        """Actualiza el total de registros en el log del proceso"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            sql = """
                UPDATE [dbo].[ProcessLog_Historical]
                SET TotalRecordsToProcess = ?
                WHERE LogId = ?
            """
            cursor.execute(sql, (total_records, process_id))
            conn.commit()

    def start_batch_log(self, process_id: int, batch_number: int, batch_start_time: datetime) -> int:
        """Inicia el log de un nuevo lote"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            sql = """
                INSERT INTO [dbo].[BatchLog_Historical]
                (ProcessId, BatchNumber, StartTime, Status)
                OUTPUT INSERTED.BatchId
                VALUES (?, ?, ?, ?)
            """

            # Ejecutar el INSERT con OUTPUT y obtener el ID
            row = cursor.execute(
                sql, (process_id, batch_number, batch_start_time, 'Started')).fetchone()
            batch_id = row[0]

            conn.commit()
            self.logger.debug(f"Lote registrado con ID: {batch_id}")
            return batch_id

    def complete_batch_log(self, batch_id: int, records_processed: int):
        """Actualiza el log del lote como completado"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            sql = """
                UPDATE [dbo].[BatchLog_Historical]
                SET EndTime = ?, Status = ?, RecordsProcessed = ?
                WHERE BatchId = ?
            """
            cursor.execute(sql, (datetime.now(), 'Completed',
                           records_processed, batch_id))
            conn.commit()

    def record_performance_metrics(self, process_id: int, batch_id: int, batch_start_time: datetime):
        """Registra métricas de rendimiento del lote"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            sql = """
                INSERT INTO [dbo].[PerformanceMetrics_Historical]
                (ProcessId, BatchId, MetricName, MetricValue, MetricUnit, CollectionTime)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            processing_time = (
                datetime.now() - batch_start_time).total_seconds() * 1000
            cursor.execute(sql, (
                process_id,
                batch_id,
                'BatchProcessingTime',
                processing_time,
                'Milliseconds',
                datetime.now()
            ))
            conn.commit()

    def complete_process_log(self, process_id: int):
        """Actualiza el log del proceso como completado"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            sql = """
                UPDATE [dbo].[ProcessLog_Historical]
                SET EndTime = ?, Status = ?, Message = ?
                WHERE LogId = ?
            """
            cursor.execute(sql, (
                datetime.now(),
                'Completed',
                'Process completed successfully',
                process_id
            ))
            conn.commit()

    def log_batch_error(self, batch_id: int, error_message: str):
        """x
        Registra un error ocurrido durante el procesamiento de un lote.

        Args:
            batch_id: ID del lote
            error_message: Mensaje de error
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                sql = """
                    UPDATE [dbo].[BatchLog_Historical]
                    SET EndTime = ?, 
                        Status = ?, 
                        ErrorMessage = ?
                    WHERE BatchId = ?
                """
                cursor.execute(sql, (
                    datetime.now(),
                    'Failed',
                    error_message,
                    batch_id
                ))
                conn.commit()

                # También registrar en el log de errores
                sql_error = """
                    INSERT INTO [dbo].[ErrorLog_Historical]
                    (ProcessId, BatchId, ErrorTime, ErrorMessage, ErrorSeverity, ErrorState, ErrorLine)
                    SELECT 
                        ProcessId,
                        ?,
                        ?,
                        ?,
                        1,
                        1,
                        0
                    FROM [dbo].[BatchLog_Historical]
                    WHERE BatchId = ?
                """
                cursor.execute(sql_error, (
                    batch_id,
                    datetime.now(),
                    error_message,
                    batch_id
                ))
                conn.commit()

        except Exception as e:
            self.logger.error(f"Error logging batch error: {str(e)}")
            # No relanzamos la excepción para evitar un ciclo de errores
