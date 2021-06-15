""" This modules contains general utility functions """

import os
import datetime
import uuid
import re
import weaviate
from weaviate.tools import WCS


DEFAULT_WEAVIATE = 'http://localhost:8080'
DEFAULT_MAX_BATCH = 1000
DEFAULT_VERBOSE = False


def log(i: str) -> str:
    """ A simple logger

    :param i: the log message
    :type i: str
    """
    now = datetime.datetime.utcnow()
    print(now, "| " + str(i))


def generate_uuid(class_name: str, identifier: str) -> str:
    """ Generate a uuid based on an identifier

    :param identifier: characters used to generate the uuid
    :type identifier: str, required
    :param class_name: classname of the object to create a uuid for
    :type class_name: str, required
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, class_name + identifier))


def check_batch_result(results: dict):
    """
    checks the outcome of a batch request to Weaviate

    Parameters
    ----------
    results: dict
        A dict that contains the outcome of a batch request
    """

    if results is not None:
        for result in results:
            if 'result' in result and 'errors' in result['result']:
                if 'error' in result['result']['errors']:
                    for message in result['result']['errors']['error']:
                        print(message['message'])


def load_schema(client, config):
    """
    loads the schema into Weaviate

    Parameters
    ----------
    client: weaviate.client
        The weaviate client
    config: dict
        A dict that contains the parameters
    """

    path = "./schema/schema.json"
    if 'schema' in config['weaviate']:
        path = config['weaviate']['schema']

    if client.schema.contains():
        client.schema.delete_all()
    client.schema.create(path)


def get_weaviate_client(instance: dict) -> weaviate.client:
    """
    Gets the Weaviate client

    Parameters
    ----------
    instance: weaviate.client
        The weaviate client

    Returns
    -------
    client: weaviate.client
        the Weaviate client
    """

    if instance is None:
        return None

    auth = username = password = client = None
    if 'username' in instance and 'password' in instance:
        username = os.getenv(instance['username'])
        password = os.getenv(instance['password'])
        print(username)
        if username is not None and password is not None:
            auth = weaviate.AuthClientPassword(username, password)

    print(auth)
    if 'url' in instance:
        if auth is not None:
            client = weaviate.Client(instance['url'], auth_client_secret=auth)
        else:
            client = weaviate.Client(instance['url'])

    elif 'wcs' in instance:
        print("trying wcs")
        if auth is not None:
            my_wcs = WCS(auth)
            try:
                result = my_wcs.get_cluster_config(instance['wcs'])
                print(result)
                weaviatepath = 'https://'+result['meta']['PublicURL']
            except:
                config = {}
                config['id'] = instance['wcs']
                config['configuration'] = {}
                config['configuration']['requiresAuthentication'] = True
                config['configuration']['tier'] = "sandbox"
                weaviatepath = my_wcs.create(config=config)
            client = weaviate.Client(weaviatepath, auth_client_secret=auth)

    else:
        client = weaviate.Client(DEFAULT_WEAVIATE)

    return client


def format_journal_name(name: str) -> str:
    """ Parses and formats the journal name from the raw journal metadata string

    :param name: name of the journal
    :type name: str
    :return: parsed, formatted name
    :rtype: str
    """
    splitted = re.split('([0-9]+)', name)
    new_name = re.sub('[\"\'\n]', '', splitted[0])
    return new_name


def format_author_name(names: str) -> list:
    """ Parses and formats the author names

    :param names: the raw names of the authors
    :type names: str
    :return: a list of parsed, formatted author names
    :rtype: list
    """
    regex = re.compile(r'[\n\r\t\'\\\"\`]')
    result = regex.sub('', names)
    result = re.sub(r'\(.*\)', '', result)
    result = re.sub(r'[\(\[\{].*?[\)\]\}]', "", result)
    result = result.replace(' and ', ', ').split(', ')
    return result


def extract_year(paper_id: str) -> int:
    """ Tries to extract the publication year from the ArXiv paper id

    :param paper_id: original paper id
    :type paper_id: str
    :return: year, 0 if no year was found.
    :rtype: int
    """
    try:
        year = 2000 + int(paper_id[:2])
    except BaseException:
        year = 0
    return year
