#!/usr/bin/env

from subprocess import run, PIPE
import os
from os import listdir
from os.path import join
import argparse

def main(filename):
    path = join(os.environ['PWD'],'results')
    print(os.environ['PWD'])

    collections = [f for f in listdir(join(path, 'series_statistics'))]
    collections.sort()
    with open(join(path,filename),'w') as summary:
        for collection in collections:
            files = [f for f in listdir(join(path,'series_statistics',collection))]
            files.sort()
            summary.write('***{}***************************************************************\n'.format(collection))
            for report in files[-1:]:
                with open(join(path,'series_statistics',collection,report)) as stats:
                    summary.write('{}\n'.format(report))
                    while True:
                        line = stats.readline()
                        if line == '\n' or 'study' in line or line == '':
                            break
                        summary.write(line)
                    summary.write('\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', '-f', default='summary.txt')
    argz = parser.parse_args()
    print(argz)
    main(argz.file)