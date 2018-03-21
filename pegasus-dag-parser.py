#!/usr/bin/env python
#
# Copyright 2018 University of Southern California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import collections
import json
import logging
import re
import os

__author__ = "Rafael Ferreira da Silva"

logger = logging.getLogger(__name__)


def _configure_logging(debug):
    """
    Configure the application's logging.
    :param debug: whether debugging is enabled
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def main():
    # Application's arguments
    parser = argparse.ArgumentParser(
        description='Parse Pegasus DAG file to generate workflow description in JSON format.')
    parser.add_argument('dag_file', metavar='DAG_FILE', help='Pegasus DAG file')
    parser.add_argument('-o', dest='output', action='store', help='Output filename')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug messages to stderr')
    args = parser.parse_args()

    # Configure logging
    _configure_logging(args.debug)

    # Sanity check
    if not os.path.isfile(args.dag_file):
        logger.error('The provided path does not exist or is not a file:\n\t' + args.dag_file)
        exit(1)

    logger.debug('Processing DAG file.')
    workflow = {
        'workflow_id': 'TO_BE_PROVIDED',
        'jobs': []
    }

    num_jobs = 0
    with open(args.dag_file) as f:
        for line in f:
            if line.startswith('JOB'):
                num_jobs += 1
                job_name = line.split()[1]
                job_submit_file = line.split()[2]

                # job object
                job = collections.OrderedDict()
                job['name'] = job_name
                if re.search('create_dir|cleanup_|register_', job_name):
                    job['type'] = 'auxiliary'
                elif re.search('stage_in|stage_out', job_name):
                    job['type'] = 'transfer'
                else:
                    job['type'] = 'compute'
                job['submit_file'] = job_submit_file
                job['parents'] = []
                workflow['jobs'].append(job)

            elif line.startswith('PARENT'):
                # Typically, parent/child references are at the end of the DAG file
                s = line.split()
                parent = s[1]
                child = s[3]
                for j in workflow['jobs']:
                    if j['name'] == child:
                        j['parents'].append(parent)
                        break

    logger.debug('Found %s jobs.' % num_jobs)

    if args.output:
        with open(args.output, 'w') as outfile:
            json.dump(workflow, outfile, indent=2)
            logger.info('JSON trace file written to "%s".' % args.output)
    else:
        print(json.dumps(workflow, indent=2))


if __name__ == '__main__':
    main()
