#!/bin/bash
docker run -v=$HOME/clone_tcia:/root/clone_tcia --env GOOGLE_APPLICATION_CREDENTIALS=/root/clone_tcia/application_default_config.json -it tcia_cloner python /root/clone_tcia/clone_collection.py -c TCGA-READ
