# arXiv-demo-dataset  <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='180' align='right' />

This repository will contain a demo using Weaviate with data and metadata from the [arXiv dataset](https://www.kaggle.com/Cornell-University/arxiv).

The code is tested with Python version 3.8.5. 

## Steps to set up:
1. Spin up a default Weaviate instance with docker-compose (see https://www.semi.technology/documentation/weaviate/current/getting-started/installation.html#docker-compose).
2. Run `python start_project.py`, with the following optional arguments. If a config file (`-cf CONFIG_FILE, --config_file CONFIG_FILE`) is given, all other parameters are ignored:
  
  | short argument | long argument | default value | description |
  | ------ | ------ | ------ | ------ | 
  | -cf | --config_file |  | config file name |
  | -i | --metadata_file | data/arxiv-metadata-oai-snapshot.json | location and name of the arXiv metadata json file |
  | -s | --schema | project/schema.json | location and name of the schema |
  | -w | --weaviate | http://localhost:8080 | weaviate url |
  | -np | --n_papers | 1000000000 | maximum number of papers to import |
  | -snp | --skip_n_papers | 0 | number of papers to skip before starting the import |
  | -po | --papers_only | false | skips all other data object imports except for papers if set to True, and ignores --skip_journals, --skip_authors and --skip_taxonomy |
  | -sj | --skip_journals | false | whether you want to skip the import of all the journals |
  | -sa | --skip_authors | false | whether you want to skip the import of all the authors |
  | -st | --skip_taxonomy | false | whether you want to skip the import of all the arxiv taxonomy objects |
  | -to | --timeout | 20 | max time out in seconds for the python client batching operations |
  | -ows | --overwrite_schema | false | overwrites the schema if one is present and one is given |
  | -bs | --batch_size | 512 | maximum number of data objects to be sent in one batch |

## Usage notes
- If you want to import the whole arXiv dataset of 2.65GB, make sure you have enough memory resources available in your environment (and Docker setup, I allocated 200GB for the Docker image size). 
- In addition, set the `--timeout` parameter to at least 50, to avoid batches to fail because of longer read and write times.
- Moreover, make sure to allocate enough memory for ES, by setting `ES_JAVA_OPTS: -Xms4g -Xmx4g` in `docker-compose.yaml`

## Build Status

| Branch   | Status        |
| -------- |:-------------:|
| Master   | [![Build Status](https://travis-ci.com/semi-technologies/arXiv-demo-dataset.svg?branch=master)](https://travis-ci.com/semi-technologies/arXiv-demo-dataset)
