#!/bin/bash

# Convert large CSV to Parquet chunks
python ./molbeam_fp/large_csv_to_parquet_chunks.py \
    ./molbeam_test/Enamine_REAL_strict_fragments_cxsmiles.cxsmiles.bz2 \
    ./molbeam_test/output_parquet_from_csv \
    smiles \
    idnumber

# Process SMILES to fingerprint Parquet
python ./molbeam_fp/process_smiles_to_fp_pq.py \
    ./molbeam_test/output_parquet_from_csv \
    ./molbeam_test/output_fingerprint_database_from_parquet \
    strict_fragments \
    smiles \
    5 \
    512 \
    2

# Run the app
python ./molbeam_fp/app.py \
    ./molbeam_test/output_fingerprint_database_from_parquet \
    ./molbeam_test/query_results \
    ./molbeam_test/queries_fp.csv \
    512 \
    2

#test the results
python ./molbeam_test/test_if_queries_present_in_results.py \
    ./molbeam_test/queries_fp.csv \
    ./molbeam_test/query_results/query_out.csv
