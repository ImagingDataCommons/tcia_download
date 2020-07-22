#!/bin/bash

# Script to build docker image used by dsub download VMs
sudo docker build --tag tcia_cloner .

# Should add some code to copy results to Container Registry
