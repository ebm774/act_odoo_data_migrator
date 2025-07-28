from odoo import models, fields, api, _
from odoo.exceptions import UserError
import json
import logging
import hashlib
from datetime import datetime
from dateutil import parser

_logger = logging.getLogger(__name__)


class SqlImportJob(models.Model):
    _name = 'sql.import.job'
    _description = 'SQL Import Job'
    _order = 'create_date desc'
    _rec_name = 'name'

    name = fields.Char(string='Job Name', required=True, default=lambda self: self._get_default_name())
    mapping_id = fields.Many2one('sql.import.mapping', string='Mapping', required=True)

    state = fields.Selection([
        ('draft', 'Draft'),
        ('running', 'Running'),
        ('done', 'Completed'),
        ('error', 'Error'),
        ('cancelled', 'Cancelled')
    ], string='Status', default='draft')

    start_date = fields.Datetime(string='Start Date')
    end_date = fields.Datetime(string='End Date')
    duration = fields.Float(string='Duration (seconds)', compute='_compute_duration', store=True)

    # Progress tracking
    total_records = fields.Integer(string='Total Records')
    imported_records = fields.Integer(string='Imported Records')
    failed_records = fields.Integer(string='Failed Records')
    progress = fields.Float(string='Progress %', compute='_compute_progress')


    # data integrity checkup
    verification_enabled = fields.Boolean(string='Enable Data Verification', default=True)
    verification_status = fields.Selection([
        ('pending', 'Verification Pending'),
        ('running', 'Verification Running'),
        ('passed', 'Verification Passed'),
        ('failed', 'Verification Failed')
    ], string='Verification Status', default='pending')
    checksum_mismatches = fields.Integer(string='Checksum Mismatches', default=0)
    verification_details = fields.Text(string='Verification Details')

    # Logging
    log_entries = fields.Text(string='Import Log')
    error_message = fields.Text(string='Error Message')

    @api.model
    def _get_default_name(self):
        return f"Import Job - {fields.Datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    @api.depends('total_records', 'imported_records', 'failed_records')
    def _compute_progress(self):
        for job in self:
            if job.total_records:
                job.progress = ((job.imported_records + job.failed_records) / job.total_records) * 100
            else:
                job.progress = 0

    @api.depends('start_date', 'end_date')
    def _compute_duration(self):
        for job in self:
            if job.start_date and job.end_date:
                delta = job.end_date - job.start_date
                job.duration = delta.total_seconds()
            else:
                job.duration = 0

    def _log(self, message, level='info'):
        """Add entry to job log"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] [{level.upper()}] {message}\n"

        self.log_entries = (self.log_entries or '') + log_entry
        _logger.log(getattr(logging, level.upper()), f"Job {self.name}: {message}")

    def action_start(self):
        """Start the import job"""
        self.ensure_one()

        if self.state != 'draft':
            raise UserError(_('Job must be in draft state to start'))

        self.write({
            'state': 'running',
            'start_date': fields.Datetime.now(),
            'log_entries': '',
            'imported_records': 0,
            'failed_records': 0,
            'error_message': False
        })

        try:
            self._log('Starting import job')
            self._run_import()

            self.write({
                'state': 'done',
                'end_date': fields.Datetime.now()
            })
            self._log(
                f'Import completed successfully. Imported: {self.imported_records}, Failed: {self.failed_records}')

        except Exception as e:
            self.write({
                'state': 'error',
                'end_date': fields.Datetime.now(),
                'error_message': str(e)
            })
            self._log(f'Import failed: {str(e)}', 'error')
            raise

    def _run_import(self):
        """Execute the actual import"""
        self.ensure_one()
        mapping = self.mapping_id

        # Validate mapping
        mapping.validate_mapping()

        field_mappings = json.loads(mapping.field_mappings or '[]')

        if not field_mappings:
            raise UserError(_('No field mappings defined'))

        # Count total records
        self._log('Counting source records...')

        with mapping.connection_ids.get_connection() as conn:
            cursor = conn.cursor()
            count_query = f"""
                SELECT COUNT(*) 
                FROM [{mapping.source_table_id.schema_name}].[{mapping.source_table_id.table_name}]  
            """

            cursor.execute(count_query)
            self.total_records = cursor.fetchone()[0]
            self._log(f'Found {self.total_records} records to import')

        # Fetch and import data
        with mapping.connection_ids.get_connection() as conn:
            cursor = conn.cursor()

            # Build select query
            source_fields = [m['source_field'] for m in field_mappings]
            select_query = f"""
                SELECT {', '.join(source_fields)}
                FROM [{mapping.source_table_id.schema_name}].[{mapping.source_table_id.table_name}]  
                {f'WHERE {mapping.source_filter}' if mapping.source_filter else ''}
            """
            cursor.execute(select_query)

            # Process in batches
            processed_count = 0
            while True:
                rows = cursor.fetchmany(mapping.batch_size)
                if not rows:
                    break

                batch_data = []
                for row in rows:
                    try:
                        record_data = self._prepare_record_data(row, field_mappings)
                        batch_data.append(record_data)
                        processed_count += 1

                    except Exception as e:
                        self.failed_records += 1
                        self._log(f'Failed to prepare record {processed_count}: {str(e)}', 'warning')

                        if not mapping.skip_errors:
                            raise

                # Import batch
                if batch_data:
                    try:
                        if mapping.target_mode == 'create':
                            self.env[mapping.target_model].create(batch_data)
                            self.imported_records += len(batch_data)
                        elif mapping.target_mode == 'update':
                            # Implementation depends on your update logic
                            self._update_records(batch_data, mapping)
                        elif mapping.target_mode == 'create_update':
                            # Implementation depends on your create_update logic
                            self._create_or_update_records(batch_data, mapping)

                    except Exception as e:
                        self.failed_records += len(batch_data)
                        self._log(f'Failed to import batch: {str(e)}', 'warning')

                        if not mapping.skip_errors:
                            raise

                # Commit batch
                self.env.cr.commit()
                self._log(f'Processed {self.imported_records + self.failed_records}/{self.total_records} records')

        if self.verification_enabled:
            self._log('Starting data verification ...')
            self._verify_imported_data()


    def _verify_imported_data(self):
        self.write({'verification_status' : 'running'})

        mapping = self.mapping_id
        field_mappings = json.loads(mapping.field_mappings or '[]')

        mismatches = []
        total_verified = 0

        try:
            source_checksums = self._get_source_checksums(field_mappings)
            target_checksums = self._get_target_checksums(field_mappings)

            self._log(f'Source records count: {len(source_checksums)}')
            self._log(f'Target records count: {len(target_checksums)}')

            source_keys = list(source_checksums.keys())[:3]
            target_keys = list(target_checksums.keys())[:3]
            self._log(f'First 3 source keys: {source_keys}')
            self._log(f'First 3 target keys: {target_keys}')

            for key in source_keys:
                if key in source_checksums and key in target_checksums:
                    self._log(f'Key {key}: Source={source_checksums[key]}, Target={target_checksums[key]}')

            for source_key, source_checksum in source_checksums.items():

                total_verified += 1

                if source_key not in target_checksums:
                    mismatches.append(f"Missing record:  {source_key}")

                elif source_checksum != target_checksums[source_key]:
                    mismatches.append(f"Check mismatch for record : {source_key}")

            for target_key in target_checksums:
                if target_key not in source_checksums:
                    mismatches.append(f"Extra record in target:  {target_key}")

            self.write({
                'checksum_mismatches': len(mismatches),
                'verification_status': 'failed' if mismatches else 'passed',
                'verification_details': '\n'.join(mismatches) if mismatches else f'All {total_verified} records verified successfully'
            })

            if mismatches:
                self._log(f'Verification failed: {len(mismatches)} mismatches found', 'warning')
            else:
                self._log(f'Verification passed: All {total_verified} records match', 'info')

        except Exception as e :
            self.write({
                'verification_status': 'failed',
                'verification_details': f'Verification error : {str(e)}'
            })

            self._log(f'Verification error : {str(e)}', 'error')

    def _get_source_checksums(self, field_mappings):

        checksums = {}
        mapping = self.mapping_id

        with mapping.connection_ids.get_connection() as conn:
            cursor = conn.cursor()
            schema_name = mapping.source_table_id.schema_name
            table_name = mapping.source_table_id.table_name

            self._log(f'Getting column info for {schema_name}.{table_name}')

            # Get column information including data types
            try:
                cursor.execute("""
                               SELECT COLUMN_NAME,
                                      DATA_TYPE,
                                      CHARACTER_MAXIMUM_LENGTH,
                                      NUMERIC_PRECISION,
                                      NUMERIC_SCALE
                               FROM INFORMATION_SCHEMA.COLUMNS
                               WHERE TABLE_SCHEMA = %s
                                 AND TABLE_NAME = %s
                               ORDER BY ORDINAL_POSITION
                               """, (schema_name, table_name))

                column_info = {row[0]: {
                    'data_type': row[1].lower(),
                    'max_length': row[2],
                    'precision': row[3],
                    'scale': row[4]
                } for row in cursor.fetchall()}

                self._log(f'Found {len(column_info)} columns: {list(column_info.keys())}')

            except Exception as e:
                self._log(f'Column query failed: {str(e)}', 'error')


            if not column_info:
                raise UserError(f'No columns found for table {schema_name}.{table_name}')

            # Build checksum calculation for each field with proper type handling
            checksum_parts = []
            binary_field_count = 0
            text_field_count = 0

            for field_mapping in field_mappings:
                field = field_mapping['source_field']

                if field not in column_info:
                    self._log(f'Warning: Field {field} not found in table columns', 'warning')

                    checksum_parts.append("'MISSING_FIELD'")
                    continue

                info = column_info[field]
                data_type = info['data_type']

                checksum_part, binary_count, text_count = self._source_datatype_management(
                    data_type, field, binary_field_count, text_field_count)


                checksum_parts.append(checksum_part)
                binary_field_count = binary_count
                text_field_count = text_count

            # Log special field handling
            if binary_field_count > 0:
                self._log(f'Found {binary_field_count} binary fields - using length + signature for verification')
            if text_field_count > 0:
                self._log(f'Found {text_field_count} large text fields - using length + checksum for verification')

            # Create final checksum query
            if not checksum_parts:
                raise UserError('No valid fields found for checksum calculation')

            checksum_concat = " + '|' + ".join(checksum_parts)
            order_field = field_mappings[0]['source_field']

            query = f"""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY [{order_field}]) as row_num,
                CHECKSUM({checksum_concat}) as row_checksum
            FROM [{schema_name}].[{table_name}]
            {f'WHERE {mapping.source_filter}' if mapping.source_filter else ''}
            ORDER BY [{order_field}]
            """

            self._log(f'Executing checksum query for {len(field_mappings)} fields')

            try:
                cursor.execute(query)
                row_count = 0

                for row in cursor.fetchall():
                    row_count += 1
                    checksums[row[0]] = row[1]  # row_num -> checksum

                self._log(f'Generated checksums for {row_count} records')

            except Exception as e:
                self._log(f'Checksum query failed: {str(e)}', 'error')
                # Log the problematic query for debugging
                self._log(f'Failed query: {query}', 'error')
                raise UserError(f'Failed to generate source checksums: {str(e)}')

        return checksums

    def _source_datatype_management(self, data_type, field, binary_field_count=0, text_field_count=0):
        # Create local copies to modify
        local_binary_count = binary_field_count
        local_text_count = text_field_count

        if data_type in ['image', 'varbinary', 'binary']:
            local_binary_count += 1
            checksum_part = f"""
                 ISNULL(
                     CAST(DATALENGTH([{field}]) AS NVARCHAR(20)) + ':' +
                     CASE 
                         WHEN DATALENGTH([{field}]) > 0 THEN
                             ISNULL(CONVERT(NVARCHAR(50), SUBSTRING([{field}], 1, CASE WHEN DATALENGTH([{field}]) >= 16 THEN 16 ELSE DATALENGTH([{field}]) END), 2), '') + ':' +
                             CASE 
                                 WHEN DATALENGTH([{field}]) > 16 THEN ISNULL(CONVERT(NVARCHAR(50), SUBSTRING([{field}], DATALENGTH([{field}]) - 15, 16), 2), '')
                                 ELSE ''
                             END
                         ELSE 'EMPTY'
                     END,
                     'NULL'
                 )"""

        elif data_type in ['text', 'ntext']:
            # For large text fields: use length + checksum of content
            local_binary_count += 1
            local_text_count += 1
            checksum_part = f"""
                 ISNULL(
                     CAST(LEN([{field}]) AS NVARCHAR(20)) + ':' +
                     CAST(CHECKSUM([{field}]) AS NVARCHAR(20)),
                     'NULL'
                 )"""

        elif data_type in ['datetime', 'datetime2', 'smalldatetime']:
            checksum_part = f"ISNULL(CONVERT(NVARCHAR(50), [{field}], 121), 'NULL')"

        elif data_type == 'date':
            checksum_part = f"ISNULL(CONVERT(NVARCHAR(50), [{field}], 23), 'NULL')"

        elif data_type == 'time':
            checksum_part = f"ISNULL(CONVERT(NVARCHAR(50), [{field}], 108), 'NULL')"

        elif data_type in ['float', 'real']:
            checksum_part = f"ISNULL(CAST([{field}] AS NVARCHAR(50)), 'NULL')"

        elif data_type in ['decimal', 'numeric', 'money', 'smallmoney']:
            checksum_part = f"ISNULL(CAST([{field}] AS NVARCHAR(50)), 'NULL')"

        elif data_type in ['bit']:
            checksum_part = f"ISNULL(CAST([{field}] AS NVARCHAR(1)), 'NULL')"

        elif data_type in ['uniqueidentifier']:
            checksum_part = f"ISNULL(CAST([{field}] AS NVARCHAR(50)), 'NULL')"

        else:
            checksum_part = f"ISNULL(CAST([{field}] AS NVARCHAR(MAX)), 'NULL')"

        return checksum_part, local_binary_count, local_text_count

    def _get_target_checksums(self, field_mappings):
        checksums = {}
        mapping = self.mapping_id

        target_model = self.env[mapping.target_model]
        records = target_model.search([], order='id')

        for i, record in enumerate(records, 1):

            values = []

            for field_mapping in field_mappings:

                field = field_mapping['target_field']
                transform = field_mapping.get('transform', 'direct')
                value = getattr(record, field, None)

                if value is None or value is False:
                    normalized_value = 'NULL'
                elif transform == 'bool':
                    normalized_value = '1' if value else '0'
                elif transform in ['int', 'float']:
                    normalized_value = str(value)
                elif transform in ['date', 'datetime']:
                    normalized_value = str(value) if value else 'NULL'
                else:
                    normalized_value = str(value).strip()

                values.append(normalized_value)

            checksum_string = '|'.join(values)
            checksum = sum(ord(char) for char in checksum_string) % (2**31)
            checksums[i] = checksum

        return checksums

    def action_verify_data(self):
        """Manual verification trigger"""
        self.ensure_one()

        if self.state != 'done':
            raise UserError(_('Can only verify completed import jobs'))

        self._log('Manual verification triggered')
        self._verify_imported_data()

        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sql.import.job',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_show_verification_report(self):
        self.ensure_one()

        if not self.verification_details:
            raise UserError(_('No verification details'))

        return {
            'type': 'ir.actions.act_window',
            'name': f'Verification Report - {self.name}',
            'res_model': 'sql.import.job',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
            'context': {'show_verification_details': True}
        }


    def _prepare_record_data(self, row, field_mappings):
        """Transform SQL row to Odoo record data"""
        data = {}

        for i, mapping in enumerate(field_mappings):
            source_value = row[i]
            target_field = mapping['target_field']
            transform = mapping.get('transform', 'direct')

            # Apply transformations
            try:
                if transform == 'direct':
                    data[target_field] = source_value
                elif transform == 'bool':
                    data[target_field] = bool(source_value) if source_value is not None else False
                elif transform == 'int':
                    data[target_field] = int(source_value) if source_value is not None else 0
                elif transform == 'float':
                    data[target_field] = float(source_value) if source_value is not None else 0.0
                elif transform == 'str':
                    data[target_field] = str(source_value) if source_value is not None else ''
                elif transform == 'date':
                    if source_value:
                        if isinstance(source_value, str):
                            data[target_field] = parser.parse(source_value).date()
                        else:
                            data[target_field] = source_value.date() if hasattr(source_value, 'date') else source_value
                    else:
                        data[target_field] = False
                elif transform == 'datetime':
                    if source_value:
                        if isinstance(source_value, str):
                            data[target_field] = parser.parse(source_value)
                        else:
                            data[target_field] = source_value
                    else:
                        data[target_field] = False
                else:
                    # Unknown transform, use direct
                    data[target_field] = source_value

            except Exception as e:
                raise UserError(f'Transform error for field {target_field}: {str(e)}')

        return data

    def _update_records(self, batch_data, mapping):
        """Update existing records (to be implemented based on your needs)"""
        # This is a placeholder - implement based on your specific update logic
        # You'll need to define how to identify existing records
        pass

    def _create_or_update_records(self, batch_data, mapping):
        """Create or update records (to be implemented based on your needs)"""
        # This is a placeholder - implement based on your specific create_update logic
        # You'll need to define how to identify existing records
        pass

    def action_cancel(self):
        """Cancel the import job"""
        self.ensure_one()
        if self.state == 'running':
            self.write({
                'state': 'cancelled',
                'end_date': fields.Datetime.now()
            })
            self._log('Job cancelled by user', 'warning')

    def action_retry(self):
        """Create a new job with same parameters"""
        self.ensure_one()
        new_job = self.copy({
            'name': f"Retry - {self.name}",
            'state': 'draft',
            'start_date': False,
            'end_date': False,
            'imported_records': 0,
            'failed_records': 0,
            'total_records': 0,
            'log_entries': False,
            'error_message': False
        })

        return {
            'type': 'ir.actions.act_window',
            'res_model': 'sql.import.job',
            'res_id': new_job.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def action_view_imported_records(self):
        """View imported records (if applicable)"""
        self.ensure_one()
        if self.state == 'done' and self.imported_records > 0:
            return {
                'type': 'ir.actions.act_window',
                'res_model': self.mapping_id.target_model,
                'view_mode': 'tree,form',
                'name': f'Imported Records - {self.name}',
                'domain': [],  # You might want to add a domain to filter imported records
            }