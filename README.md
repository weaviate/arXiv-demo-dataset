# arXiv-demo-dataset
This repository will contain a demo using Weaviate with data and metadata from the [arXiv dataset](https://www.kaggle.com/Cornell-University/arxiv).

## Steps to set up:
1. Spin up a default Weaviate instance with docker-compose (see https://www.semi.technology/documentation/weaviate/current/getting-started/installation.html#docker-compose)
2. Create the schema (see `schema.json`)
3. Import the schema to localhost:8080 (here done with Python in `initialization.py` and see https://www.semi.technology/documentation/weaviate/current/how-tos/how-to-create-a-schema.html#creating-your-first-schema-with-the-python-client)