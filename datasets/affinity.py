"""
extract affinity information from PDBbind index

"""
import os
import sys
import re

from glob import glob
from pprint import pprint
import pandas as pd
import config



def scalar(item):
    unit = item['unit']
    value = float(item['value'])
    if unit == 'fM':
        return value / 1e6
    elif unit == 'pM':
        return value / 1e3
    elif unit == 'nM':
        return  value
    elif unit == 'uM':
        return  value * 1e3
    elif unit == 'mM':
        return  value * 1e6
    elif unit == 'M':
        return  value * 1e9

def unify_unit(item):
    unit = item['unit']
    if unit in ['fM', 'pM', 'nM', 'uM', 'mM', 'M']:
        return 'nM'


def extract_from_file(file_path):
    """
    parse binding affinity information from PDBbind index file
    """

    file_name = os.path.basename(file_path)

    entrys = []
    reg_affinity = r'(?P<measure>IC50|Ki|Kd)(?P<op>=|>=|<=|>|<|~)(?P<value>[0-9.]*)(?P<unit>.*)'
    with open(file_path) as fin:
        for line in fin:
            try:
                former, latter = line.strip().split('//')

                receptor, resolution, year, log ,affinity = former.strip().split('  ')

                ma = re.match(reg_affinity, affinity)
                if ma == None:
                    print "Can't parse %s" % affinity
                    continue

                affinity_dict = ma.groupdict()
                measure = affinity_dict['measure']
                value = affinity_dict['value']
                unit = affinity_dict['unit']
                op = affinity_dict['op']

                ref, lig = latter.strip().split(' ')[:2]
                ligand = lig.strip()[1:-1]
                #pprint([receptor, ligand, measure, op, value, unit])

                entrys.append([receptor, ligand, resolution, log ,measure, op, value, unit])
            except:
                pass

    df = pd.DataFrame(entrys,columns = ['receptor','ligand','resolution', 'log_value','measure','op','value','unit'])
    print "[ ] parsed %s " % file_name
    print "[ ] entrys: %d" % len(df)
    print "[ ] measures: %s" % (','.join(list(set(df['measure']))))
    print "[ ] units: %s" % (','.join(list(set(df['unit']))))


    df['value'] = df.apply(scalar, axis=1)
    df['unit'] = df.apply(unify_unit, axis=1)

    print "[ ] unified units: %s" % (','.join(list(set(df['unit']))))
    
    df = df.drop_duplicates().dropna()
    print "[ ] unique entrys: %d" % len(df)

    return df

def gather_affinity():
    
    index_list = glob(os.path.join(config.input_index_dir,'*data*'))
    dataframes = [extract_from_file(index) for index in index_list]
    
    df = pd.concat(dataframes)
    print "[ ] toal entrys: %d" % len(df)
    
    df = df.drop_duplicates().dropna()

    print "[ ] unique entrys: %d" % len(df)

    output_path = os.path.join(config.table_dir,'pdbbind_affinity.csv')
    df.to_csv(output_path, index=False)

if __name__ == '__main__':
    gather_affinity()
    