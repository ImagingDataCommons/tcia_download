#!/usr/bin/env

from subprocess import run, PIPE
import os, shutil

def main():
    path = '{}/results/series_statistics'.format(os.environ['PWD'])
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.mkdir(path)

    results = run(['gsutil', '-m', 'ls', 'gs://idc-dsub-logs/output/'], stdout=PIPE, stderr=PIPE)

    collections = [collection.split('/')[-1] for collection in results.stdout.decode().split('/\n')]
    collections.remove('')
    for collection in collections:
        if os.path.isdir('{}/{}'.format(path, collection)):
            shutil.rmtree('{}/{}'.format(path, collection))
        os.mkdir('{}/{}'.format(path, collection))
        results = run(['gsutil', '-m', 'cp',
             'gs://idc-dsub-logs/output/{}/series_statistics.*.log'.format(collection),
             '{}/{}'.format(path, collection)])

if __name__ == '__main__':
    main()