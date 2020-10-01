import uuid
import datetime
import weaviate
import json
import re
import os
import sys
from retry import retry


def generate_uuid(class_name: str, identifier: str,
                  test: str = 'teststrong') -> str:
    """ Generate a uuid based on an identifier

    :param identifier: characters used to generate the uuid
    :type identifier: str, required
    :param class_name: classname of the object to create a uuid for
    :type class_name: str, required
    """
    test = 'overwritten'
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, class_name + identifier))


@retry(Exception, tries=3, delay=3)
def send_batch(
        client: weaviate.client.Client,
        classname: str,
        batch: weaviate.batch.requests.ThingsBatchRequest,
        total_imported: int = 0):
    """[summary]

    :param client: weaviate python client
    :type client: weaviate.client.Client
    :param classname: class name of the objects to send a batch of
    :type classname: str
    :param batch: the batch request defined with the python client
    :type batch: weaviate.batch.requests.ThingsBatchRequest
    :param total_imported: total number of previously imported items of the class, defaults to 0
    :type total_imported: int, optional
    """
    try:
        results = client.batch.create_things(batch)
        imported_without_errors = 0
        for result in results:
            if result['result']:
                log(result['result'])
            else:
                imported_without_errors += 1
        total_imported += imported_without_errors
        log('{} (out of batch of {}) new {} objects imported in last batch. Total {} {} objects imported'.format(
            imported_without_errors, len(batch), classname, total_imported, classname))
        return total_imported
    except weaviate.UnexpectedStatusCodeException as usce:
        log('Batching: Handle weaviate error: {} {}'.format(
            usce.status_code, usce.json))
        if usce.status_code >= 400 and usce.status_code < 500:
            sys.exit('Exiting import script because of error during last batch')
        return total_imported
    except weaviate.ConnectionError as ce:
        log('Batching: Handle networking error: {}'.format(ce))
        if ce.status_code >= 400 and ce.status_code < 500:
            sys.exit('Exiting import script because of error during last batch')
        return total_imported
    except Exception as e:
        log("Error in batching: {}".format(e))
        sys.exit('Exiting import script because of error during last batch')
        return total_imported


def get_metadata(datafile: str, max_size: int = 1000000000, skip_n_papers: int = 0) -> list:
    """ converts and returns the arxiv data set from json to a list

    :param datafile: the json file location and name
    :type datafile: str
    :param max_size: the maximum number of papers to import, defaults to 1000000000
    :type max_size: int, optional
    :param skip_n_papers: the number of papers to skip, defaults to 0
    :type skip_n_papers: int, optional
    :return: a list of paper objects with metainfo
    :rtype: list
    """
    data = []
    ids = set()
    log('Start loading ArXiv dataset ...')
    i = 0
    datafile = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), datafile)
    with open(datafile, 'r') as f:
        for line in f:
            if len(data) >= max_size:
                break
            i += 1
            if i <= skip_n_papers:
                continue
            line_loaded = json.loads(line)
            if line_loaded["id"] in ids:
                continue
            data.append(line_loaded)
            ids.add(line_loaded["id"])
    log('Completed loading ArXiv dataset! {} papers loaded, {} papers skipped.'.format(len(data), i - len(data)))
    return data[:max_size]


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


def log(i: str) -> str:
    """ A simple logger

    :param i: the log message
    :type i: str
    """
    now = datetime.datetime.utcnow()
    print(now, "| " + str(i))
