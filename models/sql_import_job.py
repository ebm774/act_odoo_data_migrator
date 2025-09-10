from odoo import models, fields, api, _
from odoo.exceptions import UserError
import json
import logging
import hashlib
from datetime import datetime
from dateutil import parser

_logger = logging.getLogger(__name__)


class SqlImportJob(models.Model):
    _name = 'dat.sql.import.job'
    _description = 'SQL Import Job'
    _order = 'create_date desc'
    _rec_name = 'name'

    name = fields.Char(string='Job Name', required=True, default=lambda self: self._get_default_name())
    mapping_id = fields.Many2one('dat.sql.import.mapping', string='Mapping', required=True)

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
        """Verify imported data against source using only field mappings"""
        self.write({'verification_status': 'running'})

        mapping = self.mapping_id
        field_mappings = json.loads(mapping.field_mappings or '[]')

        if not field_mappings:
            self._log('No field mappings defined - skipping verification', 'warning')
            self.write({
                'verification_status': 'failed',
                'verification_details': 'No field mappings configured for verification'
            })
            return

        mismatches = []
        total_verified = 0

        try:
            # Only verify fields that are in field_mappings
            source_data = self._get_source_mapped_data(field_mappings)
            target_data = self._get_target_mapped_data(field_mappings)

            self._log(f'Source records count: {len(source_data)}')
            self._log(f'Target records count: {len(target_data)}')

            # Compare each source record with target
            for source_key, source_values in source_data.items():
                total_verified += 1

                if source_key not in target_data:
                    mismatches.append(f"Missing record in target: {source_key}")
                    continue

                target_values = target_data[source_key]

                # Compare each mapped field
                for i, field_mapping in enumerate(field_mappings):
                    source_field = field_mapping['source_field']
                    target_field = field_mapping['target_field']

                    if i < len(source_values) and i < len(target_values):
                        if source_values[i] != target_values[i]:
                            mismatches.append(
                                f"Data mismatch for record {source_key}, field '{source_field}' -> '{target_field}': "
                                f"Source='{source_values[i]}', Target='{target_values[i]}'"
                            )

            # Check for extra records in target
            for target_key in target_data:
                if target_key not in source_data:
                    mismatches.append(f"Extra record in target: {target_key}")

            self.write({
                'checksum_mismatches': len(mismatches),
                'verification_status': 'failed' if mismatches else 'passed',
                'verification_details': '\n'.join(
                    mismatches) if mismatches else f'All {total_verified} records verified successfully'
            })

            if mismatches:
                self._log(f'Verification failed: {len(mismatches)} mismatches found', 'warning')
            else:
                self._log(f'Verification passed: All {total_verified} records match', 'info')

        except Exception as e:
            self.write({
                'verification_status': 'failed',
                'verification_details': f'Verification error: {str(e)}'
            })
            self._log(f'Verification error: {str(e)}', 'error')

    def _get_source_mapped_data(self, field_mappings):
        """Get source data indexed by the first field (usually ID)"""
        data = {}
        mapping = self.mapping_id

        # Build unique field list to avoid duplicates
        unique_fields = []
        seen_fields = set()

        for fm in field_mappings:
            field_name = fm['source_field']
            if field_name not in seen_fields:
                unique_fields.append(f"[{field_name}]")
                seen_fields.add(field_name)

        if not unique_fields:
            raise UserError('No fields to verify - field mappings is empty')

        # The first field should be our ID field
        id_field = field_mappings[0]['source_field']

        with mapping.connection_ids.get_connection() as conn:
            cursor = conn.cursor()

            schema_name = mapping.source_table_id.schema_name
            table_name = mapping.source_table_id.table_name

            query = f"""
            SELECT {', '.join(unique_fields)}
            FROM [{schema_name}].[{table_name}]
            {f'WHERE {mapping.source_filter}' if mapping.source_filter else ''}
            ORDER BY [{id_field}]
            """

            self._log(f'Source verification query: {query}')

            try:
                cursor.execute(query)

                for row in cursor.fetchall():
                    record_id = row[0]  # First field (ID)
                    record_values = []

                    # Map row values back to field mappings
                    field_to_index = {fm['source_field']: i for i, field in enumerate(unique_fields)
                                      for fm in field_mappings if f"[{fm['source_field']}]" == field}

                    for field_mapping in field_mappings:
                        source_field = field_mapping['source_field']
                        field_index = field_to_index.get(source_field, 0)
                        raw_value = row[field_index]

                        transform = field_mapping.get('transform', 'direct')
                        normalized_value = self._normalize_value_for_comparison(raw_value, transform)
                        record_values.append(normalized_value)

                    data[record_id] = record_values

                self._log(f'Retrieved {len(data)} source records for verification')

            except Exception as e:
                self._log(f'Source data query failed: {str(e)}', 'error')
                raise UserError(f'Failed to get source data: {str(e)}')

        return data

    def _normalize_value_for_comparison(self, value, transform):
        """Normalize values consistently for comparison between source and target"""
        if value is None or value is False:
            return 'NULL'

        if transform == 'bool':
            return '1' if value else '0'
        elif transform == 'int':
            try:
                return str(int(value))
            except (ValueError, TypeError):
                return 'NULL'
        elif transform == 'float':
            try:
                float_val = float(value)
                if float_val.is_integer():
                    return str(int(float_val))
                else:
                    return f"{float_val:.6f}".rstrip('0').rstrip('.')
            except (ValueError, TypeError):
                return 'NULL'
        elif transform in ['str', 'email']:  # Treat both as strings for comparison
            if value == '':
                return ''
            return str(value).strip() if value else 'NULL'
        elif transform in ['date', 'datetime']:
            if hasattr(value, 'strftime'):
                if transform == 'date':
                    return value.strftime('%Y-%m-%d')
                else:
                    return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            else:
                return str(value) if value else 'NULL'
        else:  # direct
            if value == '':
                return ''
            return str(value).strip() if value else 'NULL'

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

    def _get_target_mapped_data(self, field_mappings):
        """Get target data for only the mapped fields"""
        data = {}
        mapping = self.mapping_id

        target_model = self.env[mapping.target_model]

        # Find records that have legacy_id (imported records)
        domain = []
        if 'legacy_id' in target_model._fields:
            domain = [('legacy_id', '!=', False)]

        records = target_model.search(domain, order='legacy_id')

        self._log(f'Found {len(records)} target records to verify')

        for record in records:
            # Use legacy_id as identifier, fallback to id
            record_id = getattr(record, 'legacy_id', record.id)
            record_values = []

            # Process each mapped field
            for field_mapping in field_mappings:
                target_field = field_mapping['target_field']
                transform = field_mapping.get('transform', 'direct')

                # Skip system fields that don't need verification
                if target_field in ['create_uid', 'write_uid', 'create_date', 'write_date', 'display_name']:
                    record_values.append('NULL')
                    continue

                raw_value = getattr(record, target_field, None)
                normalized_value = self._normalize_target_value(raw_value, transform)
                record_values.append(normalized_value)

            data[record_id] = record_values

        return data

    def action_verify_data(self):
        """Manual verification trigger"""
        self.ensure_one()

        if self.state != 'done':
            raise UserError(_('Can only verify completed import jobs'))

        self._log('Manual verification triggered')
        self._verify_imported_data()

        return {
            'type': 'ir.actions.act_window',
            'res_model': 'dat.sql.import.job',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'current',
        }

    def _normalize_source_value(self, value, transform):
        """Normalize source value based on transform type"""
        if value is None:
            return 'NULL'

        if transform == 'bool':
            return '1' if value else '0'
        elif transform == 'int':
            try:
                return str(int(value))
            except (ValueError, TypeError):
                return 'NULL'
        elif transform == 'float':
            try:
                float_val = float(value)
                if float_val.is_integer():
                    return str(int(float_val))
                else:
                    return f"{float_val:.6f}".rstrip('0').rstrip('.')
            except (ValueError, TypeError):
                return 'NULL'
        elif transform == 'str':
            return str(value).strip() if value != '' else ''
        elif transform in ['date', 'datetime']:
            if hasattr(value, 'strftime'):
                if transform == 'date':
                    return value.strftime('%Y-%m-%d')
                else:
                    return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            else:
                return str(value)
        else:  # direct
            return str(value).strip() if value != '' else ''

    def _normalize_target_value(self, value, transform):
        """Normalize target value based on transform type"""
        if value is None or value is False:
            return 'NULL'

        if transform == 'bool':
            return '1' if value else '0'
        elif transform == 'int':
            try:
                return str(int(value))
            except (ValueError, TypeError):
                return 'NULL'
        elif transform == 'float':
            try:
                float_val = float(value)
                if float_val.is_integer():
                    return str(int(float_val))
                else:
                    return f"{float_val:.6f}".rstrip('0').rstrip('.')
            except (ValueError, TypeError):
                return 'NULL'
        elif transform == 'str':
            return str(value).strip() if value != '' else ''
        elif transform in ['date', 'datetime']:
            if value and value != False:
                if hasattr(value, 'strftime'):
                    if transform == 'date':
                        return value.strftime('%Y-%m-%d')
                    else:
                        return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                else:
                    return str(value)
            else:
                return 'NULL'
        else:  # direct
            return str(value).strip() if value != '' else ''

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
                    if source_value is None:
                        data[target_field] = False
                    else:
                        data[target_field] = bool(source_value)
                elif transform == 'int':
                    if source_value is None:
                        data[target_field] = False  # Odoo uses False for NULL integers
                    else:
                        data[target_field] = int(source_value)
                elif transform == 'float':
                    if source_value is None:
                        data[target_field] = False  # Odoo uses False for NULL floats
                    else:
                        # Preserve original precision by converting back to source format
                        data[target_field] = float(source_value)
                elif transform == 'str':
                    if source_value is None:
                        data[target_field] = False  # Odoo uses False for NULL strings
                    elif source_value == '':
                        data[target_field] = ''  # Preserve empty strings as empty strings
                    else:
                        data[target_field] = str(source_value).strip()
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