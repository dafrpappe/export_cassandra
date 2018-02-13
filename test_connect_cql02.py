#!/usr/bin/python2.7

import logging
import os
import time
import json
import yaml
import base64
import csv
import pandas as pd

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.io.libevreactor import LibevConnection
from cassandra.policies import DCAwareRoundRobinPolicy

class ConnCQL(object):

    """
       Connect to Cassandra database and execute only read queries.
       Password should be encrypted in the yaml file using  base64
       Constructor: Load YAML configuratio file and set to variables.
       
    """

    def __init__(self, config):
   
        self.config = config
        with open(self.config, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)

        self.contact_points = (cfg['cluster'].get('contact_points'))
        self.port = (cfg['cluster'].get('port'))
        self.keyspace = (cfg['cluster'].get('keyspace'))
        self.local_dc = (cfg['cluster'].get('local_dc'))
        self.username = (cfg['credentials'].get('username'))
        self.password = base64.b64decode((cfg['credentials'].get('password')))


    def __pandas_factory(self, colnames, rows):

        """
           Convert to a panda factory. It makes easier to convert
           to a csv file.

        """
        return pd.DataFrame(rows, columns=colnames)


    def read_data(self, query):

        """ 
           Execute a query in Cassandra database, and it returns a panda
           dataframe.
     
        """
       
        auth_provider = PlainTextAuthProvider( username=self.username, password=self.password )
        cluster = Cluster( contact_points=self.contact_points,
                           load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.local_dc),
                           port=self.port,
                           auth_provider=auth_provider )

        session = cluster.connect(self.keyspace)
        session.row_factory = self.__pandas_factory
        session.default_fetch_size = None

        restult_set = session.execute(query, timeout=None)
	rs = restult_set._current_rows

        return rs
     
     
def main():

    yamlfile = 'stg_conversations.yml'
    filename = 'intraoffice_rooms.csv'

    query = """SELECT *
               FROM conversations.intraoffice_rooms """

   
    cql = ConnCQL(yamlfile)
    rows = cql.read_data(query)

    
    try:
        rows.to_csv(filename, sep=',', encoding='utf-8', index=False)
        print "[INFO]: The " + filename + " file has been exported  succesfully."

    except:
        print "[ERROR]: An error occured trying to write the file."

if __name__ == '__main__':
    main()
