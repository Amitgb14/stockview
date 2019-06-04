#!/usr/bin/python2.7
# -*- mode:python -*-
import os
import cherrypy
import jinja2
import sys
sys.path.insert(0, ".")
from utils.process import Process
import json

root_path = os.path.dirname(__file__)
# jinja2 template renderer
env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join(root_path, 'templates')))
def render_template(template,**context):
    global env
    template = env.get_template(template+'.jinja')
    return template.render(context)


class StockView(object):
    conn = Process("config.yml")
    conn.pull_data()
    @cherrypy.expose
    def index(self, name=None):
        if name is not None:
            name = str(name).strip().lower()
        stocks = self.conn.get_data(name=name)
        return render_template('index', stocks=stocks)

if __name__ == '__main__':
    conf = {
        'global': {
            'server.socket_host': '0.0.0.0',
            'server.socket_port': int(os.environ.get('PORT', 5000)),
        },
        '/': {
            'tools.sessions.on': True,
            'tools.staticdir.root': os.path.abspath(os.getcwd())
        },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public'
        }
    }
    cherrypy.quickstart(StockView(), '/', conf)
