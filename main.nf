#!/bin/bash nextflow
nextflow.enable.dsl=2

include { FILTER_PER_GENE } from './modules/filter_by_gene'


workflow {
    paraquets_channel = Channel.fromPath( '/gpfs/space/home/a82371/CoLaus/old_pq_files/*.trans_qtl_pairs.parquet')
    Channel
       .fromPath(params.included_phenotype_ids)
       .splitText(by:20)
       .set{phenotype_list_ch}
    FILTER_PER_GENE(phenotype_list_ch, paraquets_channel.collect() )

    
}
