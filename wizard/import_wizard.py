from odoo import models, fields, api, _
from odoo.exceptions import UserError
import json


class SqlImportWizard(models.TransientModel):
    _name = 'dat.sql.import.wizard'
    _description = 'SQL Import Wizard'

    mapping_id = fields.Many2one('dat.sql.import.mapping', string='Mapping', required=True)
    job_name = fields.Char(string='Job Name', compute='_compute_job_name', store=True)

    # Preview
    preview_data = fields.Text(string='Preview Data', readonly=True)
    preview_count = fields.Integer(string='Preview Count', default=10)

    @api.depends('mapping_id')
    def _compute_job_name(self):
        for wizard in self:
            if wizard.mapping_id:
                wizard.job_name = f"Quick Import - {wizard.mapping_id.name} - {fields.Datetime.now().strftime('%Y-%m-%d %H:%M')}"
            else:
                wizard.job_name = False

    def action_preview(self):
        """Preview data before import"""
        self.ensure_one()

        if not self.mapping_id:
            raise UserError(_('Please select a mapping'))

        mapping = self.mapping_id

        # Validate mapping first
        mapping.validate_mapping()

        field_mappings = json.loads(mapping.field_mappings or '[]')

        if not field_mappings:
            raise UserError(_('No field mappings defined'))

        preview_lines = []
        with mapping.connection_id.get_connection() as conn:
            cursor = conn.cursor()

            # Build preview query
            source_fields = [m['source_field'] for m in field_mappings]
            preview_query = f"""
                SELECT TOP {self.preview_count} {', '.join(source_fields)}
                FROM {mapping.source_schema}.{mapping.source_table}
                {f'WHERE {mapping.source_filter}' if mapping.source_filter else ''}
            """

            cursor.execute(preview_query)
            columns = [column[0] for column in cursor.description]

            preview_lines.append("Source Fields -> Target Fields:")
            for fm in field_mappings:
                preview_lines.append(
                    f"  {fm['source_field']} -> {fm['target_field']} ({fm.get('transform', 'direct')})")

            preview_lines.append("\nSample Data:")
            preview_lines.append("-" * 80)

            for row in cursor.fetchall():
                row_data = []
                for i, value in enumerate(row):
                    row_data.append(f"{columns[i]}: {value}")
                preview_lines.append(" | ".join(row_data))

        self.preview_data = "\n".join(preview_lines)

        return {
            'type': 'ir.actions.act_window',
            'res_model': 'dat.sql.import.wizard',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'new',
        }

    def action_import(self):
        """Create and start import job"""
        self.ensure_one()

        # Validate mapping before creating job
        self.mapping_id.validate_mapping()

        # Create import job
        job = self.env['dat.sql.import.job'].create({
            'name': self.job_name,
            'mapping_id': self.mapping_id.id,
        })

        # Start the job
        job.action_start()

        # Open the job form
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'dat.sql.import.job',
            'res_id': job.id,
            'view_mode': 'form',
            'target': 'current',
        }