# -*- coding: utf-8 -*-
# from odoo import http


# class DataMigrator(http.Controller):
#     @http.route('/data_migrator/data_migrator', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/data_migrator/data_migrator/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('data_migrator.listing', {
#             'root': '/data_migrator/data_migrator',
#             'objects': http.request.env['data_migrator.data_migrator'].search([]),
#         })

#     @http.route('/data_migrator/data_migrator/objects/<model("data_migrator.data_migrator"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('data_migrator.object', {
#             'object': obj
#         })

