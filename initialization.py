import weaviate
import json

client = weaviate.Client("http://localhost:8080")

def import_schema(schema_file):
    with open(schema_file) as json_file:
        schema = json.load(json_file)

    client.schema.delete_all()
    client.schema.create(schema)

if __name__ == "__main__":
    # Import the full schema to Weaviate
    schema_file = './schema.json'
    import_schema(schema_file)