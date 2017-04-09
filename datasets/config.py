"""
config file for pdbbind parse
"""
import os
import sys
import multiprocessing

"""
variable
"""
manager = multiprocessing.Manager()
lock = manager.Lock()

db_name = 'pdbbind.db'

"""
parameter
"""
process_num = 8

"""
directory
"""

input_index_dir = '/home/xander/data/PDBbind/indexs/index'
input_data_dir = '/home/xander/data/PDBbind/data/pdbbind-set'

base_root = '/home/xander/pdbbind'

index_dir = os.path.join(base_root, 'index')
data_dir = os.path.join(base_root, 'data')
table_dir = os.path.join(base_root,'table')

"""
file path
"""
db_path = os.path.join(base_root,'pdbbind.db')

smina = '/home/xander/Program/smina/smina.static'
scoring_terms = os.path.join(sys.path[0],'scoring','smina.score')


reorder_pm = {
    'arg':['score_only'],
    'kwarg':{
    'custom_scoring':scoring_terms 
    }
}