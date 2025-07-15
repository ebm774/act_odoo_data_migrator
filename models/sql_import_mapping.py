from odoo import models, fields, api, _
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

    @api.model
    def create(self, vals):
        # Initialize field mappings with empty JSON if not provided
        if 'field_mappings' not in vals or not vals['field_mappings']:
            vals['field_mappings'] = json.dumps([])
        return super().create(vals)

    def fetch_source_columns(self):
        """Fetch columns from source table"""
        self.ensure_one()
        columns = []

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

        return columns

    def fetch_target_fields(self):
        """Fetch fields from target Odoo model"""
        self.ensure_one()
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
        except KeyError:
            raise UserError(_('Model %s not found') % self.target_model)

        return fields_list

    def generate_default_mapping(self):
        """Auto-generate field mappings based on field names"""
        self.ensure_one()
        source_columns = self.fetch_source_columns()
        target_fields = self.fetch_target_fields()

        mappings = []
        target_field_names = {f['name'].lower(): f['name'] for f in target_fields}

        for col in source_columns:
            source_name = col['name'].lower()
            # Try exact match
            if source_name in target_field_names:
                mappings.append({
                    'source_field': col['name'],
                    'target_field': target_field_names[source_name],
                    'transform': 'direct'
                })
            # Try common variations
            elif source_name.replace('_', '') in target_field_names:
                mappings.append({
                    'source_field': col['name'],
                    'target_field': target_field_names[source_name.replace('_', '')],
                    'transform': 'direct'
                })

        self.field_mappings = json.dumps(mappings)

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('Success'),
                'message': _('Generated %d field mappings') % len(mappings),
                'type': 'success',
            }
        }