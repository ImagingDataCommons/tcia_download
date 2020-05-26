#!/bin/bash
docker run \
       -v $HOME/tcia_download:/root/tcia_download \
       --env GOOGLE_APPLICATION_CREDENTIALS=/root/tcia_download/application_default_config.json \
       --env SERIES_STATISTICS=/root/tcia_download/idc-dsub-logs/output/tcga-read/series_statistics.log \
       --env OUTPUT_FILE=/root/tcia_download/idc-dsub-logs/output/tcga-read/stdout.log \
       --env CLONE_TCIA=/root/tcia_download/helpers \
       -it tcia_cloner python -m pdb /root/tcia_download/clone_collection.py -c TCGA-READ -p 4
