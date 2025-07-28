from odoo import models, fields, api, _
from odoo.exceptions import UserError
import json
import logging
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
            self._log(select_query)
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