# arXiv-demo-dataset
This repository will contain a demo using Weaviate with data and metadata from the [arXiv dataset](https://www.kaggle.com/Cornell-University/arxiv).

## Steps to set up:
1. Spin up a default Weaviate instance with docker-compose (see https://www.semi.technology/documentation/weaviate/current/getting-started/installation.html#docker-compose).
2. If Weaviate is running on `localhost:8080`, run `initialization.py` to load in the schema, and populate it with all categories and the number of papers you specify, with all the corresponding authors and journals. The argument `1000` in the following example is the number of papers you'd like to import. 
```bash
python initialization.py 1000
```