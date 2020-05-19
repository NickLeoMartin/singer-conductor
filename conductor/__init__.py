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

    args = parser.parse_args()
    config_filepath = args.config

    runner = SingerConductor.load(filepath=config_filepath)
    runner.validate_configs()
    runner.validate_environments()

    runner.discover()
    runner.select()
    runner.replicate()


if __name__ == '__main__':
    main()
