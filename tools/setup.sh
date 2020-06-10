#!/bin/bash
virtualenv env
source env/bin/activate

pip install dsub
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud/application_default_config.json
