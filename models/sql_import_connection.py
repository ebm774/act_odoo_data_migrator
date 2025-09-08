from odoo import models, fields, api, _
from odoo.exceptions import UserError

from .password_mixin import PasswordMixin

import logging
import pymssql

_logger = logging.getLogger(__name__)


class SqlImportConnection(models.Model, PasswordMixin):
    _name = 'dat.sql.import.connection'
    _description = 'SQL Server Connection Configuration'
    _rec_name = 'name'

    name = fields.Char(string='Connection Name', required=True)
    server = fields.Char(string='Server', required=True, help='SQL Server hostname or IP')
    port = fields.Integer(string='Port', default=1433)
    database = fields.Char(string='Database', required=True)
    username = fields.Char(string='Username', required=True)
    password_encrypted = fields.Text(string='Encrypted Password', readonly=True)
    password = fields.Char(
        string='Password',
        compute='_compute_password',
        inverse='_inverse_password',
        help='Enter password to update stored password'
    )

    available_tables = fields.Text(string='Available Tables', readonly=True)

    # Connection options
    timeout = fields.Integer(string='Connection Timeout', default=30)

    active = fields.Boolean(default=True)
    state = fields.Selection([
        ('draft', 'Not Tested'),
        ('connected', 'Connected'),
    ], string='Status', default='draft')

    last_connection_date = fields.Datetime(string='Last Connection')
    error_message = fields.Text(string='Error Message', readonly=True)


    def _get_password(self):
        """Get decrypted password"""
        if self.password_encrypted:
            # Convert string back to bytes for decryption
            encrypted_bytes = self.password_encrypted.encode('utf-8') if isinstance(self.password_encrypted,
                                                                                    str) else self.password_encrypted
            return self.decrypt_password(encrypted_bytes)
        return None

    def _get_pymssql_connection(self):
        """Create connection using pymssql"""
        try:
            # Get decrypted password
            password = self._get_password()

            if not password:
                raise UserError(_('Password is required for connection'))

            return pymssql.connect(
                server=self.server,
                port=self.port,
                user=self.username,
                password=password,  # Use decrypted password
                database=self.database,
                timeout=self.timeout,
                charset='utf8',
                as_dict=False  # Return tuples instead of dictionaries for consistency
            )
        except Exception as e:
            _logger.error(f"pymssql connection failed: {e}")
            raise UserError(_('Failed to connect using pymssql: %s') % str(e))

    def test_connection(self):
        """Test SQL Server connection"""
        self.ensure_one()

        try:
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

            return {
                'type': 'ir.actions.act_window',
                'res_model': 'dat.sql.import.connection',
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
                'error_message': str(e)
            })
            raise UserError(_('Connection failed: %s') % str(e))

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

            self.write({
                'available_tables': self._format_tables_for_display(tables)
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
            conn.close()

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

            # STORE THE TABLES IN A FIELD FOR DISPLAY
            self.write({
                'available_tables': self._format_tables_for_display(tables)
            })

            return {
                'type': 'ir.actions.act_window',
                'res_model': 'dat.sql.import.connection',
                'res_id': self.id,
                'view_mode': 'form',
                'target': 'current',
                'context': {
                    'show_tables_message': True,
                    'tables_message': f'Found {len(tables)} tables in database {self.database}'
                }
            }
        finally:
            # Always close the connection
            conn.close()

    def _format_tables_for_display(self, tables):
        """Format tables list for display"""
        if not tables:
            return "No tables found"

        formatted = f"Found {len(tables)} tables:\n\n"

        # Group by schema
        schemas = {}
        for table in tables:
            schema = table['schema']
            if schema not in schemas:
                schemas[schema] = []
            schemas[schema].append(table['table'])

        # Format by schema
        for schema, table_list in schemas.items():
            formatted += f"Schema: {schema}\n"
            for table in sorted(table_list):
                formatted += f"  • {table}\n"
            formatted += "\n"

        return formatted

    @api.model_create_multi
    def create(self, vals_list):
        """Handle password encryption on create"""
        for vals in vals_list:
            if 'password' in vals and vals['password']:
                # Encrypt password using mixin
                encrypted = self.encrypt_password(vals['password'])
                if encrypted:
                    # Convert bytes to string for Text field storage
                    vals['password_encrypted'] = encrypted.decode('utf-8') if isinstance(encrypted,
                                                                                         bytes) else encrypted
                del vals['password']  # Don't store plain text
        return super().create(vals_list)

    def write(self, vals):
        """Handle password encryption on write"""
        if 'password' in vals and vals['password']:
            # Encrypt new password using mixin
            encrypted = self.encrypt_password(vals['password'])
            if encrypted:
                # Convert bytes to string for Text field storage
                vals['password_encrypted'] = encrypted.decode('utf-8') if isinstance(encrypted, bytes) else encrypted
                vals['state'] = 'draft'  # Reset state when password changes
            del vals['password']  # Don't store plain text
        return super().write(vals)

    @api.depends('password_encrypted')
    def _compute_password(self):
        for record in self:
            # Show placeholder if password is stored, empty if not
            if record.password_encrypted:
                record.password = '••••••••'  # Password placeholder
            else:
                record.password = ''

    def _inverse_password(self):
        """Inverse method for password field"""
        for record in self:
            if record.password and record.password != '••••••••':
                # Store the actual password value for processing in write()
                # The write method will handle encryption
                pass  # Let write() handle the encryption

    def _fetch_tables_list(self):
        """Fetch tables and return as list (for selection fields)"""
        self.ensure_one()
        tables = []

        if self.state != 'connected':
            return tables

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
                    schema = row.TABLE_SCHEMA
                    table = row.TABLE_NAME
                else:
                    schema = row[0]
                    table = row[1]

                tables.append({
                    'schema': schema,
                    'table': table,
                    'full_name': f"{schema}.{table}"
                })

            return tables
        except Exception as e:
            _logger.error(f"Failed to fetch tables list: {e}")
            return []
        finally:
            conn.close()