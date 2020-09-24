import weaviate
import json
import import_data
import import_taxanomy
import sys

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

    categories_with_uuid = import_taxanomy.add_full_taxanomy() # adds groups, archives and categories

    if len(sys.argv) > 1:
        max_papers = int(sys.argv[1])
    else:
        max_papers = 10000000
    import_data.add_data(categories_with_uuid, max_papers=max_papers)