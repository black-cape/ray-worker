# Ray Cast-Iron Worker Example (ray-worker)

## Getting Started

This Ray Cast-Iron Worker example leverages several Python libraries to accomplish distributed File workflow
* [Ray](https://ray.io)
* [Kafka](https://github.com/dpkp/kafka-python)
* [Clickhouse](https://clickhouse.com/)
* [Minio](https://docs.min.io/docs/python-client-api-reference.html)
* [Pydantic](https://pydantic-docs.helpmanual.io/)
* [Toml](https://github.com/uiri/toml)


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

Run 
`docker-compose up -d`

## Streaming Text Payload Workflow

Ray Cast Iron Worker listens for message send to the topic named `castiron_text_payload` in the following format

``` {'worker_run_method': worker_run_method, 'data': data, 'arg1': 'test', 'arg2': 'test2'}```

data string will be executed against the Python method specified.  Any other field value pairs in the messsage payload
are submitted into the same method as keyed arguments

To see this, run the following

``` poetry run python etl/example/streaming_text_producer.py```

Observe the logs for Ray Cast Iron Worker
```docker logs -f ray-cast-iron-worker```  

You should see something to the effect of
```commandline
ray-cast-iron-worker  | (StreamingTextPayloadWorker pid=241) got arg1 test and arg2 test2
ray-cast-iron-worker  | (StreamingTextPayloadWorker pid=241) INFO:etl.messaging.streaming_txt_payload_worker:invoking etl.example.example_text_stream_processor.process with arg_list {'arg1': 'test', 'arg2': 'test2'}
ray-cast-iron-worker  | (StreamingTextPayloadWorker pid=241) INFO:etl.messaging.streaming_txt_payload_worker:invoking etl.example.example_text_stream_processor.process with arg_list {'arg1': 'test', 'arg2': 'test2'}
ray-cast-iron-worker  | (StreamingTextPayloadWorker pid=241) in example text stream processor got data {"field1": 1, "field2": 2}
ray-cast-iron-worker  | (StreamingTextPayloadWorker pid=241) got arg1 test and arg2 test2
ray-cast-iron-worker  | (StreamingTextPayloadWorker pid=241) in example text stream processor got data {"field1": 1, "field2": 2}
ray-cast-iron-worker  | (StreamingTextPayloadWorker pid=241) got arg1 test and arg2 test2
```




## Streaming Video Workflow 

coming soon

## S3 Compliant Object Store Backed File Workflow

### Example workflow

With the docker containers running and the worker running in either a container or locally
1. Navigate to MinIO `http://localhost:9000`, login using user: castiron, pass: castiron
2. Add `etl/example/example_config.toml` to the `etl` bucket
3. Refresh the page to verify that additional etl buckets are created
![img.png](img.png)
4. Navigate into `01_inbox`
5. Add `etl/example/test.csv`
![img_1.png](img_1.png)
6. Observe the log for Ray Cast Iron Worker
```docker logs -f ray-cast-iron-worker```

The configured example processor will simply output the file you just dropped
![img_2.png](img_2.png)
8. The test.csv file will also be moved to the `archive_dir` bucket

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
* `canceled_dir`

### Settings

The `Settings` class allows for the usage of environment variables or a `.env` file to supply the appropriate arguments
based on its inheritance of Pyandatic's `BaseSettings` class. Additional details on how `BaseSettings` works can be
found in the Pydantic [documentation](https://pydantic-docs.helpmanual.io/usage/settings/).
