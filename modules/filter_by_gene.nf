#!/bin/bash nextflow
//nextflow.enable.dsl=2

process FILTER_PER_GENE {
    container = 'quay.io/fhcrc-microbiome/python-pandas:4a6179f'

    publishDir "${params.outdir}/formated_pq_files", mode:'copy'

    

    input:
    val str_phenotype_ids  // list of phenotypes?
    val pq_files

    // Output data
    output:



       script:
       phenotype_ids = str_phenotype_ids.split("\n")

       pq_files_str = pq_files.join(" ")
       pq_files_str = pq_files_str.replace("[", "(")
       pq_files_str = pq_files_str.replace("]", ")")


  
        """

                isFirst=1
        for pq_file in ${pq_files_str} ;
          do
            if [ \$isFirst -eq 1 ];
              then
                filter_multiple_genes.py -i \$pq_file -g '${phenotype_ids}' -f 1
                isFirst=0

            else
                filter_multiple_genes.py -i \$pq_file -g '${phenotype_ids}'
              fi
          done
 

        """
  

}
