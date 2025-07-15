# -*- coding: utf-8 -*-
{
    'name': "Data Migrator",

    'summary': "Import data from SQL Server 2014 to Odoo",

    'description': """
        SQL Server to PostgreSQL Migration Tool
        =======================================
        
        This module provides tools to import data from SQL Server 2014 databases
        into Odoo 18, handling the migration that pgloader couldn't complete.
        
        Features:
        - Connect to SQL Server databases
        - Map SQL Server tables to Odoo models
        - Import data with progress tracking
        - Handle data transformations
        - Error logging and reporting
        - Ensure the data are not impacted during migration
    """,

    'author': "Pierre Dramaix",
    'website': "https://www.autocontrole.be",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/15.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list
    'category': 'Technical',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base', 'web'],

    # always loaded
    'data': [
        # Security
        'security/security.xml',
        'security/ir.model.access.csv',

        # Views
        'views/sql_import_connection_views.xml',
        'views/sql_import_mapping_views.xml',
        'views/sql_import_job_views.xml',

        # Wizards
        'wizard/import_wizard_views.xml',

        # Menu views
        'views/menu_views.xml',
    ],
    'demo': [],
    'installable': True,
    'application': True,
    'auto_install': False,
    'license': 'LGPL-3',
}

