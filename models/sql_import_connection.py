from odoo import models, fields, api, _
from odoo.exceptions import UserError

from .password_mixin import PasswordMixin

import logging
import pymssql

_logger = logging.getLogger(__name__)


class SqlImportConnection(models.Model, PasswordMixin):
    _name = 'sql.import.connection'
    _description = 'SQL Server Connection Configuration'
    _rec_name = 'name'

    name = fields.Char(string='Connection Name', required=True)
    server = fields.Char(string='Server', required=True, help='SQL Server hostname or IP')
    port = fields.Integer(string='Port', default=1433)
    database = fields.Char(string='Database', required=True)
    username = fields.Char(string='Username', required=True)
    password_encrypted = fields.Binary(string='Encrypted Password', readonly=True)
    password = fields.Char(string='Password', required=True, store=False)


    # Connection options
    timeout = fields.Integer(string='Connection Timeout', default=30)
    charset = fields.Selection([
        ('utf8', 'UTF-8'),
        ('latin1', 'Latin-1'),
        ('cp1252', 'Windows-1252'),
    ], string='Character Set', default='utf8')


    active = fields.Boolean(default=True)
    state = fields.Selection([
        ('draft', 'Not Tested'),
        ('connected', 'Connected'),
    ], string='Status', default='draft')

    last_connection_date = fields.Datetime(string='Last Connection')
    error_message = fields.Text(string='Error Message', readonly=True)

    @api.model
    def _check_dependencies(self):
        """Check if required Python packages are available"""
        missing_packages = []

        if missing_packages:
            raise UserError(_(
                'Missing required Python packages: %s\n\n'
                'Please install with:\n'
                'pip install pymssql\n'
                'or\n'
                'pip install pyodbc'
            ) % ', '.join(missing_packages))

    def _get_pymssql_connection(self):
        """Create connection using pymssql"""

        try:
            return pymssql.connect(
                server=self.server,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=self.timeout,
                charset=self.charset,
                as_dict=False  # Return tuples instead of dictionaries for consistency
            )
        except Exception as e:
            _logger.error(f"pymssql connection failed: {e}")
            raise UserError(_('Failed to connect using pymssql: %s') % str(e))


    def test_connection(self):
        """Test SQL Server connection"""
        self.ensure_one()

        try:
            # Check dependencies first
            self._check_dependencies()
            # create connection
            conn = self._get_pymssql_connection()

            # Test the connection
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            self.write({
                'state': 'connected',
                'last_connection_date': fields.Datetime.now(),
                'error_message': False
            })

            # return {
            #     'type': 'ir.actions.client',
            #     'tag': 'display_notification',
            #     'params': {
            #         'title': _('Success'),
            #         'message': _('Connection successful\n Connection string saved'),
            #         'type': 'success',
            #         'sticky': False,
            #     }
            # }

            return {
                'type': 'ir.actions.act_window',
                'res_model': 'sql.import.connection',
                'res_id': self.id,
                'view_mode': 'form',
                'target': 'current',
                'context': {
                    'show_success_message': True,
                    'success_message': f'Connection successful! SQL Server version: {version[:50]}'
                }
            }


        except Exception as e:
            self.write({
                'state': 'error',
                'error_message': str(e)
            })
            raise UserError(_('Connection failed:  %s') % str(e))

    def get_connection(self):
        """Return a SQL Server connection object"""
        self.ensure_one()

        if self.state != 'connected':
            self.test_connection()

        return self._get_pymssql_connection()

    def fetch_tables(self):
        """Fetch all tables from SQL Server database"""
        self.ensure_one()
        tables = []

        # Use get_connection() which now properly returns a connection
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                           SELECT TABLE_SCHEMA, TABLE_NAME
                           FROM INFORMATION_SCHEMA.TABLES
                           WHERE TABLE_TYPE = 'BASE TABLE'
                           ORDER BY TABLE_SCHEMA, TABLE_NAME
                           """)

            for row in cursor.fetchall():
                # Handle both pymssql and pyodbc result formats
                if hasattr(row, 'TABLE_SCHEMA'):
                    # pyodbc returns named results
                    schema = row.TABLE_SCHEMA
                    table = row.TABLE_NAME
                else:
                    # pymssql returns tuples
                    schema = row[0]
                    table = row[1]

                tables.append({
                    'schema': schema,
                    'table': table,
                    'full_name': f"{schema}.{table}"
                })

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Tables Retrieved'),
                    'message': _('Found %d tables in database %s') % (len(tables), self.database),
                    'type': 'success',
                }
            }
        finally:
            # Always close the connection
            conn.close()

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if 'password' in vals and vals['password']:
                # Encrypt password using mixin
                encrypted = self.encrypt_password(vals['password'])
                vals['password_encrypted'] = encrypted
                del vals['password']  # Don't store plain text
        return super().create(vals_list)

    def write(self, vals):
        """Handle password encryption on write"""
        if 'password' in vals and vals['password']:
            # Encrypt new password using mixin
            encrypted = self.encrypt_password(vals['password'])
            vals['password_encrypted'] = encrypted
            vals['state'] = 'draft'  # Reset state when password changes
            del vals['password']  # Don't store plain text
        return super().write(vals)