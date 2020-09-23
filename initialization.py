import weaviate
import json
import import_data
import import_taxanomy

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
    import_data.add_data(categories_with_uuid, max_papers=200000, start=0) # adds papers, journals and authors