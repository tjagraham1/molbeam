import sys
import datamol as dm
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa
from pyarrow.parquet import ParquetWriter
from rdkit.Chem import rdMolDescriptors
from rdkit import DataStructs
import pandas as pd
from rich import print
from rich.console import Console
from tqdm.contrib import tenumerate
console = Console()

def _preprocess(i, row):
    '''Takes a smiles string and generates a clean rdkit mol with datamol. The stereoisomers
    are then enumerated while holding defined stereochemistry. Morgan fingerprints are then
    generated using RDkit with and without stereochemistry. The try/except logic deals with 
    RDkit mol failures on conversion of an invalid smiles string. Smarts are added for later
    searching.'''
    try: 
        dm.disable_rdkit_log()
        mol = dm.to_mol(row[smiles_column], ordered=True)
        mol = dm.fix_mol(mol)
        mol = dm.sanitize_mol(mol, sanifix=True, charge_neutral=False)
        mol = dm.standardize_mol(mol, disconnect_metals=False, normalize=True, reionize=True, uncharge=False, stereo=True)

        fingerprint_function = rdMolDescriptors.GetMorganFingerprintAsBitVect
        pars2 = { "radius": radius,
                         "nBits": fp_size,
                         "invariants": [],
                         "fromAtoms": [],
                         "useChirality": False,
                         "useBondTypes": True,
                         "useFeatures": False, }

        fp1 = fingerprint_function(mol, **pars2)


        row["standard_smiles"] = dm.standardize_smiles(dm.to_smiles(mol))
        row["achiral_fp"] = list(fp1.GetOnBits())


        return row

    except ValueError:

        
        row["standard_smiles"] = 'dropped'
        row["achiral_fp"] = 'dropped'

        return row

def prep_parquet_db(df, n_jobs, smiles_column, fp_size, radius):
    
    '''Take a cleaned df that contains protonated/tautomerized smiles, 
    the vendor database ID and a canonical ID -- number indicates protomer/taut
    and 1) enumerate stereoisomers 2) generate chiral/achiral fingerprints 3) smarts and
    a new canonical ID that references stereoisomer. 
    
    Returns: elaborated dataframe - pandas dataframe
    
    args: df == dataframe to be passed in - pandas dataframe
    n_jobs == number of jobs utilized by joblib - integer
    smiles_col == the name of the smiles column - string
    catalog_id == name of column referencing the catalog ID - string
    canonical_id == name of col referencing the canonical ID usually Z123456789_1 where _1 is protomer/taut num -string
    '''
    
    
    #Add clean the mols, standardize and generate lists for enumerated smiles, fingerprints both chiral/achiral at 8kbits
    data_clean = dm.parallelized(_preprocess, df.iterrows(), arg_type='args', progress=True, total=len(df), n_jobs=n_jobs)
    data_clean = pd.DataFrame(data_clean)
    
    df_out = data_clean[data_clean.standard_smiles != 'dropped']
    
    return df_out


dataset_dir = sys.argv[1]
output_dir = sys.argv[2]
out_prefix = sys.argv[3]

#name of smiles column in parquet dataset
smiles_column = sys.argv[4]
dataset_format = 'parquet'
#number of cpu cores to use for joblib
n_jobs = int(sys.argv[5])
#size of fingerprint to use for rdkit
fp_size = int(sys.argv[6])
#radius of fingerprint to use for rdkit
radius = int(sys.argv[7])

def main():
    dataset = ds.dataset(dataset_dir, format=dataset_format)

    # Create a list of fragments that are not memory loaded
    fragments = [file for file in dataset.get_fragments()]

    total_frags = len(fragments)
    print(f'There are a total of {len(fragments)} fragments in the dataset: {output_dir}')

    for count,element in tenumerate(fragments, start=0, total=None):
        

        console.print(f'Starting fragment dataset: {count} in {dataset_dir}', style="blue bold")
        #cast the fragment as a pandas df
        df = element.to_table().to_pandas()
        df2 = prep_parquet_db(df, n_jobs, smiles_column, fp_size, radius)

        console.print(f'There are a total of {len(df2)} valid smiles strings with fingerprints', style="green bold")

        table = pa.Table.from_pandas(df2, preserve_index=False)

        
        #write the molchunk to disk
        pq.write_table(table, f'{output_dir}/{out_prefix}_{count}.parquet')
        console.print(f'Wrote parquet to {output_dir}/{out_prefix}_{count}.parquet', style="purple bold")

if __name__ == '__main__':
    main()
