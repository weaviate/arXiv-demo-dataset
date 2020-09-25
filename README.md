# arXiv-demo-dataset  <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='180' align='right' />

This repository will contain a demo using Weaviate with data and metadata from the [arXiv dataset](https://www.kaggle.com/Cornell-University/arxiv).

## Steps to set up:
1. Spin up a default Weaviate instance with docker-compose (see https://www.semi.technology/documentation/weaviate/current/getting-started/installation.html#docker-compose).
2. If Weaviate is running on `localhost:8080`, run `initialization.py` to load in the schema, and populate it with all categories and the number of papers you specify, with all the corresponding authors and journals. The argument `1000` in the following example is the number of papers you'd like to import. If you don't specify this argument, then all data will be imported. 

```bash
python initialization.py 1000
```

### Note
With the current docker-compose setup, not the full dataset can be imported. Adviced is to import not more than 300.000 papers with the current ES setup. More details on how to import the full dataset coming soon!   


## Build Status

| Branch   | Status        |
| -------- |:-------------:|
| Master   | [![Build Status](https://travis-ci.com/semi-technologies/weaviate-python-client.svg?token=1qdvi3hJanQcWdqEstmy&branch=master)](https://travis-ci.com/semi-technologies/weaviate-python-client)

## Roadmap
A more stable version is coming soon!
