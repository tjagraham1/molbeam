import pandas as pd
import sys
from rich import print
from rich.console import Console
console = Console()

query_csv = sys.argv[1]
fp_search_out_csv = sys.argv[2]

# Read the CSV files
csv1 = pd.read_csv(query_csv)
csv2 = pd.read_csv(fp_search_out_csv)

# Get the names from the first CSV
names_csv1 = set(csv1['names'])

# Get the names from the second CSV
names_csv2 = set(csv2['idnumber'])

# Check if names from the first CSV are present in the second CSV
names_present = names_csv1.issubset(names_csv2)

# Print the result
if names_present:
    console.print("Successful test, all queries have been identified in the fp search output.", style="green bold")
else:
    console.print("Failed test, test queries were not identified in the fp search output.", style="red bold")

# To find the missing names
missing_names = names_csv1.difference(names_csv2)
if missing_names:
    console.print("The following names are missing in the fp search output:")
    for name in missing_names:
        console.print(name)
