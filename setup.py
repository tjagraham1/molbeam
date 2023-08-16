from setuptools import setup, find_packages

setup(
    name="molbeam_fp",
    version="0.1.2",
    packages=find_packages(),
    install_requires=[
        "pyarrow",
        "duckdb",
        "datamol",
        "rdkit",
        "pandas"
    ],
    entry_points={
        'console_scripts': [
            'molbeam_search=molbeam_fp.app:main',
            'large_csv_to_parquet=molbeam_fp.large_csv_to_parquet_chunks:main',
            'process_smiles_to_parquet_fp=molbeam_fp.process_smiles_to_fp_pq:main'
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)