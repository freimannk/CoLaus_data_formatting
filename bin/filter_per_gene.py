#!/usr/bin/env python3
import argparse

import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser(description="Input file list and variants pattern.")

parser.add_argument('-i', '--incoming_pq_file', required=True,type=str,
                    help="Input newpq file.")
parser.add_argument('-g', '--gene', required=True, type=str,
                    help="Analyzed gene.")
parser.add_argument('-c', '--concated_pq_file', required=True, type=str,
                    help="Concated pq file.")
parser.add_argument('-f', '--isFirst', nargs='?', const=None, type=int,
                    help="Concated pq file.")

args = parser.parse_args()

def return_output(incoming_pq_file:str, concated_pq_file:str):
    if args.isFirst!=None:
        income_pq_table = pq.read_table(incoming_pq_file, filters=[("phenotype_id", "==", args.gene)])
        pq.write_table(income_pq_table, args.concated_pq_file)
    elif args.isFirst==None:
        income_pq_table = pq.read_table(incoming_pq_file, filters=[("phenotype_id", "==", args.gene)])
        concated_pq_table = pq.read_table(concated_pq_file, filters=[("phenotype_id", "==", args.gene)])
        concated_tables = pa.concat_tables([income_pq_table, concated_pq_table])
        pq.write_table(concated_tables, args.concated_pq_file)

if __name__ == "__main__":
    return_output(args.incoming_pq_file, args.concated_pq_file)