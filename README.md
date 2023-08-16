molbeam_fp

molbeam_fp is a collection of python scripts that function to serve as tools for generating a library of morgan fingerprints and an engine for running queries on that library. The fingerprint library is built using RDKit fingerprints and datamol for normalizing/processing input smiles. The resulting fingerprints are then stored as chunked arrays in Apache Arrow tables along with the original SMILES and compound ID. These tables are then saved as parquet files for use during fingerprint queries. 

The query engine uses Apache Arrow to access and process the fingerprints to determine the jaccard distance between each member of the library and the query or queries. The results are then queried with duckdb and matches with good scores, (values closer to zero are better) as determined by a predefined quantile, are saved to a new dataframe. 

The resulting dataframe is saved as a parquet (and a csv for easier viewing) and can be later accessed as a dataframe through Pandas or Apache Arrow. If your fingerprint database is split over several directories, then the query process can be parallelized (code not provided here). In our lab, we have split the Enamine REAL (3.6B) across ~4000 directories and run parallel queries on each fingerprint dataset where the results are aggregated in post processing steps. 

Steps for utilizing the scripts:

1) First create a conda enviroment for running the scripts. Code was only tested with python 3.8.

<pre>
conda create --name molbeam_fp python=3.8
conda activate molbeam_fp
</pre>

2) Install dependencies.

<pre>
pip install pyarrow duckdb datamol rdkit pandas rich
</pre>

3) Clone the repo.

<pre>
git clone https://github.com/abazabaaa/molbeam_fp.git
</pre>

4) Convert the SMILES csv into a parquet dataset. You must create a directory for the output parquet files. Based on the settings in the script the output parquets are roughly ~30k smiles strings.

<pre>
python large_csv_to_fp_pq.py /path/to/file.csv /path/to/output_dir smiles_column_name compound_id_col_name
</pre>

/path/to/file.csv = absolute path to csv file. Can be compressed (bz2). \
/path/to/output_dir = a directory that has been previously created and is meant to store the output parquet files from the csv. \
smiles_column_name = the name of the smiles column found within the csv file. \
compound_id_col_name = the name of the column that contains the compound IDs or names.

5) Convert the SMILES parquet dataset into a parquet dataset that contains morgan fingerprints. You must create the directory where the parquet files will be saved to.

<pre>
python process_smiles_to_fp_pq.py /path/to/parquet_dataset/from_csv_file /path/to/parquet_fps/output_dir parquet_output_file_name_prefix name_of_smiles_column num_cpu_cores_to_use fingerprint_size fingerprint_radius
</pre>

/path/to/parquet_dataset/from_csv_file = a directory that has been previously created and is meant to store the output parquet files from the csv in previous step.
\
/path/to/parquet_fps/output_dir = a directory that has been previously created and is meant to store the dataset containing the parquet with fingerprints. \
parquet_output_file_name_prefix = prefix of the filename for each parquet created in this step
\
num_cpu_cores_to_use = number of cpu cores datamol is allowed to use during fingerprint generation
\
fingerprint_size = number of bits in morgan fingerprint (256, 512, 1024, etc)
\
fingerprint_radius = radius used in generation of morgan fingerprints (2, 3, etc)

6) Run a fingerprint query with multiple queries. The input queries must be saved into a csv (comma seperated) with the format shown below.

<pre>
names,smiles
PV-004207401484,CC(C)C(C#N)C(=O)N1CCCN(C(=O)CSc2cnn(C)c2)CC1C
PV-005694609843,CC(C)C(C#N)C(=O)N1CCCC(CN(C)C(=O)c2ncnc3c2CCC3)C1
</pre>
Commandline for running fingerprint query:
<pre>
python app.py /path/to/parquet_fps/output_dir /path/to/search_results_dir /path/to/query_file.csv fingerprint_size fingerprint_radius
</pre>

/path/to/parquet_fps/output_dir = The directory created in the previous step that contains your parquet fingerprint library.
\
/path/to/search_results_dir = The directory where the results of the search will be deposited. The output will be called "query_out.pq" and there is also an equivalent CSV file written for easier inspection.
\
/path/to/query_file.csv = The csv file with the query (using the format shown above).
\
fingerprint_size = number of bits in morgan fingerprint; should be consistent with fp library (256, 512, 1024, etc).
\
fingerprint_radius = radius used in generation of morgan fingerprints; should be consistent with fp library (2, 3, etc).

7) The output file will contain 4 columns with the first two being the ID and SMILES string of the hit compounds and then the final columns being labeled as "query1_score", "query2_score" where query1 is the name/ID of the compound(s) queried. Numbers closer to zero represent more similar structures.

A demo/test is provided in the repo. Within the molbeam_test directory is a cxsmiles file containing ~1M smiles strings. Each directory within the molbeam_test dir has the intermediate parquet database files. The molbeam_test/query_results contains example output. The queries for the demo are taken directly from the cxsmiles file in an attempt to demonstrate that the fingerprint search will identify them and related compounds. 
\
In order to run the tests execute the following (The process will use 5 cpu cores; this can be modified in the script if needed):
<pre>
bash run_all_test.sh
</pre>



