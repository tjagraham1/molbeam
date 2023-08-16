import resource
from time import sleep

from pyarrow import Table
from pyarrow import csv
import pyarrow as pa
from pyarrow.parquet import ParquetWriter

import pandas as pd

import sys
from datetime import timedelta
from timeit import time
import pathlib


file = sys.argv[1]
output_dir = sys.argv[2]
name_of_smiles_col = sys.argv[3]
name_of_column_w_ids = sys.argv[4]
#column1 = 'smiles'
#column2 = 'idnumber'

output_prefix = pathlib.Path(file).stem



def stopwatch(method):
    def timed(*args, **kw):
        ts = time.perf_counter()
        result = method(*args, **kw)
        te = time.perf_counter()
        duration = timedelta(seconds=te - ts)
        print(f"{method.__name__}: {duration}")
        return result
    return timed


class InputStreamReader:
    def __init__(self, file_stream, include_columns):
        self.file_stream = file_stream
        self.include_columns = include_columns
        self._stream = None

    def batches(self):
        i = tries = 0
        while True:
            try:
                batch = self.__next_batch()
                i += 1
                yield i, batch
            except StopIteration:
                break

    def __next_batch(self):
        return self.stream.read_next_batch()


    @property
    def stream(self):
        if not self._stream:
            read_options = pa.csv.ReadOptions(block_size=2097152)
            parse_options = pa.csv.ParseOptions(delimiter='\t')
            convert_options = pa.csv.ConvertOptions(include_columns=self.include_columns)
            self._stream = pa.csv.open_csv(
                self.file_stream, read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options
            
        )
        return self._stream


@stopwatch        
def csv_stream_to_parquet_batch_writer(include_columns, \
                 input_file_to_stream, \
                 output_file_stream_directory, \
                 output_file_prefix, \
                 smiles_column_title):
    
    print('Initiating stream.')
    
    input_stream_reader = InputStreamReader(input_file_to_stream, include_columns)
    
    outfiles_list = []
    
    for i, batch in input_stream_reader.batches():
        print(f'Ingesting batch number {i}')
        df = batch.to_pandas()
        table = pa.Table.from_pandas(df)
        schema = table.schema
        smiles = list(df[smiles_column_title])
        print(f'Writing a total of {len(smiles)} smiles per output file to disk.')
        outfile = f'{output_file_stream_directory}/{output_file_prefix}_{i}.parquet'
        ParquetWriter(outfile, schema).write_table(table)
        print(f'Wrote parquet to {outfile}')
        outfiles_list.append(outfile)
        
    return outfile


def main():

    include_columns = [name_of_smiles_col, name_of_column_w_ids]

    outfiles = csv_stream_to_parquet_batch_writer(include_columns, \
                         file, \
                         output_dir, \
                         output_prefix, \
                         name_of_smiles_col)

    print(outfiles)

if __name__ == '__main__':
    main()

