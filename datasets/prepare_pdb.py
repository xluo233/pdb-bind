"""
get data from ligands
"""
import os
import sys

import prody
import subprocess
import multiprocessing
from functools import partial
from glob import glob
from utils import smina_param
from db import database
import config



db = database()

_mkdir = lambda x:os.system('mkdir -p %s' % x)

def get_pocket(input_path):
    """
    select protein atoms from pocket file
    """

    output_path = input_path.replace(config.input_data_dir, config.data_dir)
    if not os.path.exists(output_path):
        parsed = prody.parsePDB(pocket_input)
        pocket = parsed.select('protein')
        _mkdir(os.path.dirname(output_path))
        prody.writePDB(output_path, pocket)



def get_protein(input_path):
    """
    select protein atoms from protein file
    """
    try:
        output_path = input_path.replace(config.input_data_dir, config.data_dir)
        if not os.path.exists(output_path):
            parsed = prody.parsePDB(input_path)
            pocket = parsed.select('protein')
            _mkdir(os.path.dirname(output_path))
            prody.writePDB(output_path, pocket)
    except Exception as e:
        print e

def get_ligand(smina_pm, input_path):
    """
    calculate terms values from ligands
    """
    try:
        
        output_path = input_path.replace(config.input_data_dir, config.data_dir)
        output_path = output_path.replace('.mol2','.pdb')
        _mkdir(os.path.dirname(output_path))

        ligand_name = os.path.basename(output_path).split('_')[0]

        receptor_path = output_path.replace('ligand.pdb','protein.pdb')

        kw = {
            'receptor': receptor_path,
            'ligand':   input_path,
            'out'   :   output_path
        }

        if not os.path.exists(output_path):
            cmd = smina_pm.make_command(**kw)
            print cmd
            cl = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            cl.wait()
            cont = cl.communicate()[0].strip().split('\n')

            terms = [line.strip().split(' ')[2:] for line in cont if line.startswith('##')]
            head = map(lambda x:x.replace(',',' '),terms[0])

            head = ['ligand','position'] + map(lambda x:'"%s"' % x, head)
            data = [ligand_name, 0] + map(lambda x:float(x),terms[1])
            data = [data]
            db.insert_or_replace('scoring_terms',data,head)
    except Exception as e:
        print e



def run_multiprocess(target_list, func):
    print type(target_list)
    print type(func)
    pool = multiprocessing.Pool(config.process_num)
    pool.map_async(func, target_list)
    pool.close()
    pool.join()

def select_protein_and_ligands():
    print 'get pockets'
    pockets_list = glob(os.path.join(config.input_data_dir,'*','*pocket.pdb'))
    
    run_multiprocess(pockets_list, get_pocket)
    print 'get protein'
    proteins_list = sorted(glob(os.path.join(config.input_data_dir,'*','*protein.pdb')))
    print len(proteins_list)
    run_multiprocess(proteins_list, get_protein)

    print 'get ligand'
    ligands_list = sorted(glob(os.path.join(config.input_data_dir,'*','*ligand.mol2')))
    print len(ligands_list)
    smina_pm = smina_param('reorder')
    smina_pm.load_param(*config.reorder_pm['arg'],**config.reorder_pm['kwarg'])
    run_multiprocess(ligands_list, partial(get_ligand,smina_pm))

if __name__ == '__main__':
    select_protein_and_ligands()
    db.export()
