"""
pepare data
"""
import os
import sys

from copy import copy
import numpy as np
import pandas as pd
import config
from db import database


def get_scoring_terms():
    """
    get scoring terms from csv file
    """
    scoring_terms_path = os.path.join(config.table_dir, 'scoring_terms.csv')
    if not os.path.exists(scoring_terms_path):
        print "scoring terms doesn't exists, try export..."
        pdbbind_db = database()
        pdbbind_db.export()
    if not os.path.exists(scoring_terms_path):
        print "cannot export data from database."
        exit(1)

    # read scoring term col name
    cols = []
    for row in open(config.scoring_terms):
        row = row.strip().split('  ')
        col = row[1].replace(',', ' ')
        cols.append(col)

    # load scoring term table
    scoring_df = pd.read_csv(scoring_terms_path)

    # load binding affinity table
    affinity_path = os.path.join(config.table_dir, 'pdbbind_affinity.csv')
    affinity_df = pd.read_csv(affinity_path)

    merged_df = affinity_df.merge(scoring_df, on='receptor')
    merged_df = merged_df.drop_duplicates().dropna()

    merged_df.to_csv(os.path.join(config.table_dir,'merged.csv'),index=False)

    print "load %d entrys " % len(merged_df)

    receptors = copy(merged_df['receptor'])
    scorings = copy(merged_df[cols])
    log_affinitys = copy(merged_df['log_value'])

    return receptors, scorings, log_affinitys

def normalize(scorings):
    """
    uniform scoring in for all columns in the dataframe
    """
    for col in scorings.columns:
        mean = np.mean(scorings[col])
        std = np.std(scorings[col])
        norm = lambda x:  ( x -  mean ) / std if std else 0
        scorings[col] = scorings[col].apply(norm)

    return scorings



def test():
    """
    test input
    """

    a, b, c = get_scoring_terms()
    print a
    print normalize(b)
    print c

if __name__ == '__main__':
    test()