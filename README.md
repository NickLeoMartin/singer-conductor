# singer-conductor
A singer. io compatible module to orchestrate tap-to-target replication. 

## Installation

``` BASH
pip install -e .
```

## Configuration

A single configuration file can be used. The only required arguements are `tap_bin` , `tap_config_filepath` , `tap_catalog_filepath` , `target_bin` and `target_config_filepath` . 

See the example config below:

``` BASH
{
    "tap_bin": "~/.virtualenvs/venv_tap/bin/tap-postgres",
    "tap_config_filepath": "tap/config.json",
    "tap_catalog_filepath": "raw_catalog.json",
    "selector_bin": "~/.virtualenvs/venv_selector/bin/selector-postgresql",
    "selector_config_filepath": "selector/config.json",
    "selector_catalog_filepath": "edited_catalog.json",
    "transformer_bin": "~/.virtualenvs/venv_transformer/bin/transform-field",
    "transformer_config_filepath": "transformer/config.json",
    "target_bin": "~/.virtualenvs/venv_target/bin/target-bigquery",
    "target_config_filepath": "target/config.json",
    "state_persistence_filepath": "gs://mygcpbucket/state.json"
}
```

## Invocation

From the command line:

``` BASH
conductor --config test/test_config.json
```

## Tests

This repository makes use of `pytest` :

``` 
pytest tests/
```
