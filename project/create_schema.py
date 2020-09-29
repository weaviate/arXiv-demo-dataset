import weaviate
import os
from project.helper import log

def create_schema(schema: str=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'schema.json'), weaviate_url: str="http://localhost:8080", overwrite: bool=False):
    client = weaviate.Client(weaviate_url)
    if not client.is_ready():
        raise Exception("Container is not ready")
    if not client.schema.contains() or overwrite == True:
        log("Creating schema")
        client.schema.create(schema)
        log("Done Creating schema")
    else:
        log("Weaviate container already contained a schema")

if __name__ == "__main__":
    create_schema()