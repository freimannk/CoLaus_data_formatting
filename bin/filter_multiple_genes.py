#!/usr/bin/env python3
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import time

parser = argparse.ArgumentParser(description="Input file list and variants pattern.")

parser.add_argument('-i', '--incoming_pq_file', required=True,type=str,
                    help="Input newpq file.")
parser.add_argument('-g', '--phenotype_ids_str', required=True, type=str,
                    help="Analyzed gene.")
parser.add_argument('-f', '--is_first', nargs='?', const=None, type=int,
                    help="Concated pq file.")

args = parser.parse_args()

CONCATED_PQ_FILE_BASE_PATH= "CoLaus_tensorQTL_sumstat_"


def get_all_pgenotype_ids_table(phenotype_ids:list, incoming_pq_file:str):
    return pq.read_table(incoming_pq_file, filters=[('phenotype_id', 'in', phenotype_ids)])


def write_concated_pq_files(incoming_all_phenotype_ids_table, phenotype_ids):
    for phenotype_id in phenotype_ids:
        filtered_phenotype_id_table = get_phenotype_id_table(incoming_all_phenotype_ids_table, phenotype_id)
        file_name = CONCATED_PQ_FILE_BASE_PATH + phenotype_id + ".parquet"
        pq.write_table(filtered_phenotype_id_table, file_name)


def get_phenotype_id_table(incoming_all_phenotype_ids_table, phenotype_id):
    table = incoming_all_phenotype_ids_table.filter(
        pc.equal(incoming_all_phenotype_ids_table['phenotype_id'], phenotype_id)
    )
    return table


def add_to_existing_pq_files(incoming_all_phenotype_ids_table, phenotype_ids):
    for phenotype_id in phenotype_ids:
        filtered_phenotype_id_table = get_phenotype_id_table(incoming_all_phenotype_ids_table, phenotype_id)
        concated_pq_file = CONCATED_PQ_FILE_BASE_PATH + phenotype_id + ".parquet"
        concated_pq_table = pq.read_table(concated_pq_file)
        concated_tables = pa.concat_tables([filtered_phenotype_id_table, concated_pq_table])
        pq.write_table(concated_tables, concated_pq_file)

if __name__ == "__main__":
    start = time.time()
    phenotype_ids = list(map(str, args.phenotype_ids_str.strip('[]').split(',')))
    phenotype_ids = list(map(str.strip, phenotype_ids))
    incoming_all_phenotype_ids_table = get_all_pgenotype_ids_table(phenotype_ids, args.incoming_pq_file)
    if args.is_first!=None:
        write_concated_pq_files(incoming_all_phenotype_ids_table, phenotype_ids)
    elif args.is_first==None:
        add_to_existing_pq_files(incoming_all_phenotype_ids_table, phenotype_ids)
    end = time.time()
    print(f"Finish time {end-start}")