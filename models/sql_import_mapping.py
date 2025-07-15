from odoo import models, fields, api, _
from odoo.exceptions import UserError
import json


class SqlImportMapping(models.Model):
    _name = 'sql.import.mapping'
    _description = 'SQL Import Table Mapping'
    _rec_name = 'name'

    name = fields.Char(string='Mapping Name', required=True)
    connection_id = fields.Many2one('sql.import.connection', string='Connection', required=True)

    # Source configuration
    source_schema = fields.Char(string='Source Schema', default='dbo')
    source_table = fields.Char(string='Source Table', required=True)
    source_filter = fields.Text(string='WHERE Clause', help='SQL WHERE clause to filter source data')

    # Target configuration
    target_model = fields.Char(string='Target Model', required=True, help='Odoo model name (e.g., res.partner)')
    target_mode = fields.Selection([
        ('create', 'Create New Records'),
        ('update', 'Update Existing Records'),
        ('create_update', 'Create or Update')
    ], string='Import Mode', default='create', required=True)

    # Field mappings
    field_mappings = fields.Text(string='Field Mappings', help='JSON field mapping configuration')

    # Options
    batch_size = fields.Integer(string='Batch Size', default=100)
    skip_errors = fields.Boolean(string='Skip Errors', help='Continue import even if some records fail')
    active = fields.Boolean(default=True)

    @api.model_create_multi
    def create(self, vals_list):
        """Override create to handle batch operations and initialize field mappings"""
        # Process each record in the batch
        for vals in vals_list:
            # Initialize field mappings with empty JSON if not provided
            if 'field_mappings' not in vals or not vals['field_mappings']:
                vals['field_mappings'] = json.dumps([])

        return super().create(vals_list)

    def fetch_source_columns(self):
        """Fetch columns from source table"""
        self.ensure_one()

        if not self.connection_id or not self.source_table:
            raise UserError(_('Connection and source table must be configured first'))

        columns = []

        try:
            with self.connection_id.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, (self.source_schema, self.source_table))

                for row in cursor.fetchall():
                    columns.append({
                        'name': row.COLUMN_NAME,
                        'type': row.DATA_TYPE,
                        'nullable': row.IS_NULLABLE == 'YES',
                        'length': row.CHARACTER_MAXIMUM_LENGTH
                    })

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Source Columns Fetched'),
                    'message': _('Found %d columns in table %s.%s') % (len(columns), self.source_schema,
                                                                       self.source_table),
                    'type': 'success',
                }
            }
        except Exception as e:
            raise UserError(_('Failed to fetch source columns: %s') % str(e))

    def fetch_target_fields(self):
        """Fetch fields from target Odoo model"""
        self.ensure_one()

        if not self.target_model:
            raise UserError(_('Target model must be configured first'))

        fields_list = []

        try:
            model = self.env[self.target_model]
            for fname, field in model._fields.items():
                if not field.compute and field.store and fname not in ['__last_update', 'display_name']:
                    fields_list.append({
                        'name': fname,
                        'type': field.type,
                        'string': field.string,
                        'required': field.required,
                        'readonly': field.readonly
                    })

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Target Fields Fetched'),
                    'message': _('Found %d fields in model %s') % (len(fields_list), self.target_model),
                    'type': 'success',
                }
            }
        except KeyError:
            raise UserError(_('Model %s not found') % self.target_model)

    def generate_default_mapping(self):
        """Auto-generate field mappings based on field names"""
        self.ensure_one()

        if not self.connection_id or not self.source_table or not self.target_model:
            raise UserError(_('Connection, source table, and target model must be configured first'))

        # Get source columns
        source_columns = []
        try:
            with self.connection_id.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION
                """, (self.source_schema, self.source_table))

                for row in cursor.fetchall():
                    source_columns.append({
                        'name': row.COLUMN_NAME,
                        'type': row.DATA_TYPE
                    })
        except Exception as e:
            raise UserError(_('Failed to fetch source columns: %s') % str(e))

        # Get target fields
        target_fields = []
        try:
            model = self.env[self.target_model]
            for fname, field in model._fields.items():
                if not field.compute and field.store and fname not in ['__last_update', 'display_name']:
                    target_fields.append({
                        'name': fname,
                        'type': field.type
                    })
        except KeyError:
            raise UserError(_('Model %s not found') % self.target_model)

        # Generate mappings
        mappings = []
        target_field_names = {f['name'].lower(): f for f in target_fields}

        for col in source_columns:
            source_name = col['name'].lower()
            transform = 'direct'

            # Determine appropriate transform based on data type
            if col['type'] in ['bit']:
                transform = 'bool'
            elif col['type'] in ['int', 'bigint', 'smallint', 'tinyint']:
                transform = 'int'
            elif col['type'] in ['float', 'real', 'decimal', 'numeric', 'money']:
                transform = 'float'
            elif col['type'] in ['varchar', 'nvarchar', 'char', 'nchar', 'text']:
                transform = 'str'
            elif col['type'] in ['datetime', 'datetime2', 'smalldatetime']:
                transform = 'datetime'
            elif col['type'] in ['date']:
                transform = 'date'

            # Try exact match
            if source_name in target_field_names:
                target_field = target_field_names[source_name]
                # Adjust transform based on target field type
                if target_field['type'] == 'boolean':
                    transform = 'bool'
                elif target_field['type'] in ['integer', 'float']:
                    transform = target_field['type'][:3]  # 'int' or 'flo'
                elif target_field['type'] in ['char', 'text']:
                    transform = 'str'
                elif target_field['type'] == 'date':
                    transform = 'date'
                elif target_field['type'] == 'datetime':
                    transform = 'datetime'

                mappings.append({
                    'source_field': col['name'],
                    'target_field': target_field['name'],
                    'transform': transform
                })
            # Try common variations
            elif source_name.replace('_', '') in target_field_names:
                target_field = target_field_names[source_name.replace('_', '')]
                mappings.append({
                    'source_field': col['name'],
                    'target_field': target_field['name'],
                    'transform': transform
                })

        self.field_mappings = json.dumps(mappings, indent=2)

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('Success'),
                'message': _('Generated %d field mappings') % len(mappings),
                'type': 'success',
            }
        }

    def validate_mapping(self):
        """Validate field mappings configuration"""
        self.ensure_one()

        if not self.field_mappings:
            raise UserError(_('Field mappings are required'))

        try:
            mappings = json.loads(self.field_mappings)
        except json.JSONDecodeError:
            raise UserError(_('Invalid JSON in field mappings'))

        if not mappings:
            raise UserError(_('No field mappings configured'))

        # Validate target model exists
        try:
            model = self.env[self.target_model]
        except KeyError:
            raise UserError(_('Target model %s does not exist') % self.target_model)

        # Validate each mapping
        model_fields = model._fields
        for i, mapping in enumerate(mappings):
            if not isinstance(mapping, dict):
                raise UserError(_('Mapping %d is not a valid object') % (i + 1))

            if 'source_field' not in mapping:
                raise UserError(_('Missing source_field in mapping %d') % (i + 1))

            if 'target_field' not in mapping:
                raise UserError(_('Missing target_field in mapping %d') % (i + 1))

            target_field = mapping['target_field']
            if target_field not in model_fields:
                raise UserError(_('Target field %s does not exist in model %s') % (target_field, self.target_model))

            # Validate transform
            transform = mapping.get('transform', 'direct')
            valid_transforms = ['direct', 'bool', 'int', 'float', 'str', 'date', 'datetime']
            if transform not in valid_transforms:
                raise UserError(_('Invalid transform "%s" in mapping %d. Valid transforms: %s') % (transform, i + 1,
                                                                                                   ', '.join(
                                                                                                       valid_transforms)))

        return True

    def action_test_mapping(self):
        """Test the mapping configuration with a small sample"""
        self.ensure_one()

        # Validate first
        self.validate_mapping()

        # Test with sample data
        try:
            with self.connection_id.get_connection() as conn:
                cursor = conn.cursor()
                mappings = json.loads(self.field_mappings)
                source_fields = [m['source_field'] for m in mappings]

                test_query = f"""
                    SELECT TOP 5 {', '.join(source_fields)}
                    FROM {self.source_schema}.{self.source_table}
                    {f'WHERE {self.source_filter}' if self.source_filter else ''}
                """

                cursor.execute(test_query)
                rows = cursor.fetchall()

                return {
                    'type': 'ir.actions.client',
                    'tag': 'display_notification',
                    'params': {
                        'title': _('Test Successful'),
                        'message': _('Mapping test passed. Found %d sample records.') % len(rows),
                        'type': 'success',
                    }
                }

        except Exception as e:
            raise UserError(_('Mapping test failed: %s') % str(e))