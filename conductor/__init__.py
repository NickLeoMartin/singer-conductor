#! /usr/bin/env python3
"""
Executes tap-to-target replication.
"""
import argparse

from conductor.client import SingerConductor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c',
                        '--config',
                        help='Conductor config file',
                        required=True)
    parser.add_argument('-d',
                        '--discover',
                        help='Flag for discovery mode',
                        action='store_true')
    parser.add_argument('-s',
                        '--select',
                        help='Flag for selection mode',
                        action='store_true')
    parser.add_argument('-r',
                        '--replicate',
                        help='Flag for replication mode',
                        action='store_true')

    args = parser.parse_args()
    config_filepath = args.config

    runner = SingerConductor.load(filepath=config_filepath)
    runner.validate_configs()
    runner.validate_environments()

    at_least_one_required_args = [args.discover, args.select, args.replicate]

    if all(arg is not False for arg in at_least_one_required_args):
        error_message = (
            'At least one of arg must be specified: '
            '--discover, --select or --replicate'
        )
        raise ValueError(error_message)

    if args.discover:
        runner.discover()

    if args.select:
        runner.select()

    if args.replicate:
        runner.replicate()


if __name__ == '__main__':
    main()
