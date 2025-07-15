from odoo import models, fields, api, _
from odoo.exceptions import UserError
import pyodbc
import logging

_logger = logging.getLogger(__name__)


class SqlImportConnection(models.Model):
    _name = 'sql.import.connection'
    _description = 'SQL Server Connection Configuration'
    _rec_name = 'name'

    name = fields.Char(string='Connection Name', required=True)
    server = fields.Char(string='Server', required=True, help='SQL Server hostname or IP')
    port = fields.Integer(string='Port', default=1433)
    database = fields.Char(string='Database', required=True)
    username = fields.Char(string='Username', required=True)
    password = fields.Char(string='Password', required=True)
    driver = fields.Selection([
        ('ODBC Driver 17 for SQL Server', 'ODBC Driver 17'),
        ('ODBC Driver 18 for SQL Server', 'ODBC Driver 18'),
        ('SQL Server', 'SQL Server'),
        ('FreeTDS', 'FreeTDS'),
    ], string='Driver', default='ODBC Driver 17 for SQL Server')

    active = fields.Boolean(default=True)
    state = fields.Selection([
        ('draft', 'Not Tested'),
        ('connected', 'Connected'),
        ('error', 'Connection Error')
    ], string='Status', default='draft')

    last_connection_date = fields.Datetime(string='Last Connection')
    error_message = fields.Text(string='Error Message', readonly=True)

    @api.model
    def _get_connection_string(self):
        """Build ODBC connection string"""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )

    def test_connection(self):
        """Test SQL Server connection"""
        self.ensure_one()
        try:
            conn_str = self._get_connection_string()
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]

            self.write({
                'state': 'connected',
                'last_connection_date': fields.Datetime.now(),
                'error_message': False
            })

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Success'),
                    'message': _('Connection successful! SQL Server version: %s') % version[:50],
                    'type': 'success',
                    'sticky': False,
                }
            }

        except Exception as e:
            self.write({
                'state': 'error',
                'error_message': str(e)
            })
            raise UserError(_('Connection failed: %s') % str(e))

    def get_connection(self):
        """Return a SQL Server connection object"""
        self.ensure_one()
        if self.state != 'connected':
            self.test_connection()

        try:
            conn_str = self._get_connection_string()
            return pyodbc.connect(conn_str)
        except Exception as e:
            _logger.error(f"Failed to connect to SQL Server: {e}")
            self.write({'state': 'error', 'error_message': str(e)})
            raise UserError(_('Failed to connect to SQL Server: %s') % str(e))

    def fetch_tables(self):
        """Fetch all tables from SQL Server database"""
        self.ensure_one()
        tables = []

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                           SELECT TABLE_SCHEMA, TABLE_NAME
                           FROM INFORMATION_SCHEMA.TABLES
                           WHERE TABLE_TYPE = 'BASE TABLE'
                           ORDER BY TABLE_SCHEMA, TABLE_NAME
                           """)

            for row in cursor.fetchall():
                tables.append({
                    'schema': row.TABLE_SCHEMA,
                    'table': row.TABLE_NAME,
                    'full_name': f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
                })

        return tables