import weaviate
import os
try:
    from project.helper import log
except ModuleNotFoundError:
    from helper import log


def create_schema(
        schema: str = os.path.join(
            os.path.dirname(
                os.path.realpath(__file__)),
            'schema.json'),
        weaviate_url: str = "http://localhost:8080",
        overwrite: bool = False):
    """ Creates a defined schema

    :param schema: file location and name of the schema json, defaults to
        os.path.join( os.path.dirname( os.path.realpath(__file__)), 'schema.json')
    :type schema: str, optional
    :param weaviate_url: location of the running weaviate, defaults to "http://localhost:8080"
    :type weaviate_url: str, optional
    :param overwrite: whether you want to overwrite an existing schema, defaults to False
    :type overwrite: bool, optional
    :raises Exception: if the weaviate container is not ready
    """
    client = weaviate.Client(weaviate_url)
    if not client.is_ready():
        raise Exception("Container is not ready")
    if not client.schema.contains() or overwrite:
        log("Creating schema")
        client.schema.create(schema)
        log("Done Creating schema")
    else:
        log("Weaviate container already contained a schema")


if __name__ == "__main__":
    create_schema()
