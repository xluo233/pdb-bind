import os
import sys
import sqlite3
import config
import time
from config import lock
from utils import lockit
import csv
from collections import namedtuple, OrderedDict

table = namedtuple('Table',['name','columns','primary_key'])

tables = { }

class database:
    """
    A simple wrapper for sqlite3
    Generate sql query and execute it
    Now only support create table and data insert
    """

    def __init__(self):
        self.db_path = config.db_path
        self.export_dir = config.table_dir
        self.tables = tables

        # create scoring table base on content in config.scoring_terms
        self.add_scoring_term_tabel()

        if not os.path.exists(self.db_path):
            self.backup_and_reset_db()
        else:
            self.connect_db()

    def connect_db(self):

        self.conn = sqlite3.connect(self.db_path)
        self.connect = True

        print "connect to %s" % self.db_path

    def backup_db(self):
        """
        create a copy of current sqlite database
        :return: 
        """
        backup_db_path = self.db_path.replace('.', '_'+time.strftime("%Y-%m-%d-%H:%M:%S", time.gmtime()) +'.')

        if os.path.exists(self.db_path):
            cmd = 'cp %s %s' % (self.db_path , backup_db_path)
            os.system(cmd)
            print "backup database %s" % backup_db_path

    def backup_and_reset_db(self):
        """
        backup current database and create a new one
        :return: 
        """
        if os.path.exists(self.db_path):
            backup_db_path = self.db_path.replace('.', '_' + time.strftime("%Y-%m-%d-%H:%M:%S", time.gmtime()) + '.')
            os.rename(self.db_path, backup_db_path)


        self.connect_db()
        self.create_table()



    @lockit
    def insert_or_replace(self, tabel, values, head=None):
        
        db_value = lambda x:'"%s"' % x if type(x).__name__ == 'str' else str(x)

        db_values = [ map(db_value, value) for value in values ]
        #print db_values

        sql_values = [ '(' + ','.join(value) + ')' for value in db_values ]

        #print sql_values
        stmt = 'REPLACE INTO ' + tabel + ' '
        if not head is None:
            stmt += '('+ ','.join(head) + ')'
        stmt += ' VALUES '
        stmt += ','.join(sql_values)
        stmt += ';'

        #print stmt

        if not self.connect:
            self.connect_db()
        try:
            self.conn.execute(stmt)
        except Exception as e:
            print e

        self.conn.commit()

    @lockit
    def insert_or_ignore(self, tabel, values, head=None):

        db_value = lambda x: '"%s"' % x if type(x).__name__ == 'str' else str(x)

        db_values = [map(db_value, value) for value in values]
        #print db_values

        sql_values = ['(' + ','.join(value) + ')' for value in db_values]

        #print sql_values
        stmt = 'INSERT OR IGNORE INTO ' + tabel + ' '
        if not head is None:
            stmt += '(' + ','.join(head) + ')'
        stmt += ' VALUES '
        stmt += ','.join(sql_values)
        stmt += ';'

        #print stmt

        if not self.connect:
            self.connect_db()
        try:
            self.conn.execute(stmt)
        except Exception as e:
            print e

        self.conn.commit()

    def get_all_success(self, table_name):
        
        columns = self.tables[table_name].columns.keys()[:-2]
        columns = ','.join(columns)
        
        stmt = 'select ' + columns + ' from ' + table_name
        stmt += ' where state = 1 ;'

        print stmt

        cursor = self.conn.cursor()
        cursor.execute(stmt)
        # fetch all result is a list of tuple
        values = cursor.fetchall()
        values = map(lambda x:list(x), values)

        return values
        
    def if_success(self, table_name, values):
        """
        find if there's success record in _state table
                
        """

        db_value = lambda x: '"%s"' % x if type(x).__name__ == 'str' else str(x)

        
        db_values = map(db_value, values)
        
        columns = self.tables[table_name].primary_key

        print columns

        cond = map(lambda (col,val) : '%s=%s' % (col,val), zip(columns, db_values))
        cond.append('state=1')

        stmt = 'select count(*) from ' + table_name
        stmt += ' where ' + ' and '.join(cond) + ';'
        print stmt

        cursor = self.conn.cursor()
        cursor.execute(stmt)
        #fetch one will return tuple like (3,)
        values = cursor.fetchone()[0]

        print values

        return values



    def create_table(self):
        
        for tab in tables.values():
            stmt = 'create table '+ tab.name + ' ('
            for key in tab.columns.keys():
                stmt += key + ' ' + tab.columns[key]
                if key in tab.primary_key:
                    stmt += ' not null ,'
                else:
                    stmt += ' ,'
            stmt += 'primary key(' + ','.join(tab.primary_key) + '));'
            
            self.conn.execute(stmt)

        print "Create all %d tables" % len(tables)        

    def add_scoring_term_tabel(self):
        """
        columns of this tabel will determined by 
        the content of config.scoring_tems 
        """

        
        columns = []
        columns.append(('ligand','text'))
        columns.append(('position','integer'))
        for row in open(config.scoring_terms):
            row = row.strip().split('  ')
            col = row[1].replace(',',' ')
            col = '"%s"' % col
            columns.append((col,'real'))

        self.tables['scoring_terms'] = table(*['scoring_terms',
            OrderedDict(columns),
            ['ligand','position']])

    def export(self):
        """
        export data from database to csv file
        :return: 
        """

        cursor = self.conn.cursor()
        for tab in self.tables.values():
            table_path = os.path.join(self.export_dir, tab.name+'.csv')
            os.system('mkdir -p %s' % self.export_dir)

            cursor.execute('SELECT * FROM %s' % tab.name)
            with open(table_path,'w') as fout:
                csv_out = csv.writer(fout)
                csv_out.writerow([d[0] for d in cursor.description])
                for result in cursor:
                    csv_out.writerow(result)

                print 'export table %s' % tab.name


        
    

