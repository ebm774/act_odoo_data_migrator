from odoo import models, fields, api, _
from odoo.exceptions import UserError
from .password_mixin import PasswordMixin
import logging

_logger = logging.getLogger(__name__)


class SqlLegacyTable(models.Model):
    _name = 'dat.sql.legacy.table'
    _description = 'SQL legacy tables'
    _rec_name = 'full_name'

    connection_id = fields.Many2one('dat.sql.import.connection', string='Connection', required=True)
    schema_name = fields.Char(string='Schema', required=True)
    table_name = fields.Char(string='Table')
    full_name = fields.Char(string='Full Name', compute='_compute_full_name', store=True)

    @api.depends('schema_name', 'table_name')
    def _compute_full_name(self):
        for record in self:
            if record.schema_name and record.table_name:
                record.full_name = f"{record.schema_name}.{record.table_name}"
            else:
                record.full_name = ""

    @api.model
    def refresh_tables_for_connection(self, connection_id):
        """Refresh table list for a specific connection"""
        connection = self.env['dat.sql.import.connection'].browse(connection_id)
        if not connection.exists() or connection.state != 'connected':
            raise UserError(_('Connection must be tested and connected first'))

        # Remove existing tables for this connection
        existing_tables = self.search([('connection_id', '=', connection_id)])
        existing_tables.unlink()

        # Fetch and create new tables
        try:
            tables = connection._fetch_tables_list()
            table_vals = []
            for table in tables:
                table_vals.append({
                    'connection_id': connection_id,
                    'schema_name': table['schema'],
                    'table_name': table['table'],
                })

            if table_vals:
                self.create(table_vals)
                return len(table_vals)
            return 0

        except Exception as e:
            _logger.error(f"Failed to refresh tables for connection {connection_id}: {e}")
            raise UserError(_('Failed to refresh tables: %s') % str(e))

    def action_refresh_tables(self):
        """Button action to refresh tables for current connection"""
        self.ensure_one()

        if not self.connection_id:
            raise UserError(_('Please select a connection first'))

        if self.connection_id.state != 'connected':
            raise UserError(_('Connection must be tested and connected first'))

        # Refresh tables for this connection
        count = self.refresh_tables_for_connection(self.connection_id.id)

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('Tables Refreshed'),
                'message': _('Found and imported %d tables from the database.') % count,
                'type': 'success',
            }
        }