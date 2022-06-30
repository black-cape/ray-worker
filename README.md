# Ray Cast-Iron Worker Example (ray-worker)

## Getting Started

This Cast-Iron Worker example leverages several Python libraries to accomplish the ETL process.
* [Ray](https://ray.io)
* [Kafka](https://github.com/dpkp/kafka-python)
* [Minio](https://docs.min.io/docs/python-client-api-reference.html)
* [Pydantic](https://pydantic-docs.helpmanual.io/)
* [Toml](https://github.com/uiri/toml)
* [urllib3](https://urllib3.readthedocs.io/en/latest/)

## Installing Dependencies

* Install Python 3.8, preferably using [Pyenv](https://github.com/pyenv/pyenv)
```bash
$ pyenv install
```
* This project utilizes [Poetry](https://python-poetry.org/docs/#installation) for managing python dependencies.
```bash
$ curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
```
* Install dependencies
```bash
$ poetry install
```
## Configure Cast Iron (using Ray Worker)

1. Retrieve the Cast Iron project (https://github.com/black-cape/cast-iron)
1. Replace the Worker_Dockerfile with the one included in this project

## Start the Worker

1. Add `127.0.0.1 kafka` entry to your /etc/hosts file
1. Start the Cast-Iron Ray-based ETL worker
    * Locally
    ```
    $ poetry shell
    $ uvicorn --host 0.0.0.0 --port 8080 etl.app_manager:app
    ```
      - To run with a debugger use `python worker.py`
    * Docker (with the Cast Iron Project)
    ```
    $ docker-compose -f docker-compose-single.yml up --build 
    ```

## Utlize the ETL

With the docker containers running and the worker running in either a container or locally
1. Navigate to MinIO `http://localhost:9000`
1. Add `example_config.toml` to the `etl` bucket
1. Refresh the page to verify that additional etl buckets are created
1. Navigate into `01_inbox`
1. Add `data/data_test.tsv`
1. TSV should be ETL-ed
1. TSV moves to the `archive_dir` bucket

### Matching files to processors
The processor config `handled_file_glob` configures file extension pattern matching. The matchers should be provided as e.g. `_test.tsv|_updated.csv|.mp3` (no spaces).

The processor config `handled_mimetypes` specifies Tika mimetypes for a processor to match. Its value should be a comma-separated string of mimetypes, e.g. `application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document`
* Note: in order to enable Tika mimetype matching, the environment setting `ENABLE_TIKA` must be set to a truthy value. See the `Settings` section below for details about environment settings.

Files are matched to processors as such: for a single file, checks are made based on processor configurations, one processor at a time.
* The first processor that is found to match the file is used to process the file, and the rest are ignored.
  * So if two processors could have each matched a file, the order in which the processors are checked determines which matches and which is ignored. 
* One or the other, or both, of `handled_mimetypes` and `handled_file_glob` can be specified for a processor.
  * If both are specified, mimetype checking is tried first, then file extension glob if mimetype failed or returned False for that processor.
  * Each processor will check both mimetype and file extension glob matching before moving on to the next processor.

## Technology

### Toml

[Toml](https://en.wikipedia.org/wiki/TOML) is used to create configuration files that can be used to tell the worker how
to ETL a given file.

An example configuration file can be seen in the `example_config.toml` and the `example_python_config.toml`.

### MinIO

Several buckets are used as stages in the ETL process. These buckets are defined in the toml config file. The buckets
are created i
* `inbox_dir`
* `processing_dir`
* `archive_dir`
* `error_dir`

### Settings

The `Settings` class allows for the usage of environment variables or a `.env` file to supply the appropriate arguments
based on its inheritance of Pyandatic's `BaseSettings` class. Additional details on how `BaseSettings` works can be
found in the Pydantic [documentation](https://pydantic-docs.helpmanual.io/usage/settings/).
