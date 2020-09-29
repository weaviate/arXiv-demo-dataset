import uuid
import datetime
import weaviate
import json
import re

def generate_uuid(class_name: str, identifier: str) -> str:
    """ Generate a uuid based on an identifier

    :param identifier: characters used to generate the uuid
    :type identifier: str, required
    :param class_name: classname of the object to create a uuid for
    :type class_name: str, required
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, class_name+identifier))

def send_batch(client: weaviate.client.Client, classname: str, batch: weaviate.batch.requests.ThingsBatchRequest, total_imported: int=0):
    try:
        results = client.batch.create_things(batch)
        for result in results: 
            if result['result']:
                log(result['result'])
        log('{} new {} objects imported in last batch. Total {} {} objects imported'.format(len(batch), classname, total_imported+len(batch), classname))
    except weaviate.UnexpectedStatusCodeException as usce:
        log('Batching: Handle weaviate error: {} {}'.format(usce.status_code, usce.json))
    except weaviate.ConnectionError as ce:
        log('Batching: Handle networking error: {}'.format(ce))
    except Exception as e:
        log("Error in batching: {}".format(e))

def get_metadata(datafile: str, max_size: float=float('inf')) -> list:
    data  = []
    log('Start loading ArXiv dataset ...')
    with open(datafile, 'r') as f:
        for line in f:
            if len(data) >= max_size:
                break
            data.append(json.loads(line))
    log('Completed loading ArXiv dataset! ...')
    return data[:max_size]

def format_journal_name(name: str) -> str:
    splitted = re.split('([0-9]+)', name)
    new_name = re.sub('[\"\'\n]', '', splitted[0])
    return new_name

def format_author_name(name: str) -> str:
    regex = re.compile(r'[\n\r\t\'\\\"\`]')
    result = regex.sub('', name)
    result = re.sub(r'\(.*\)', '', result)
    result = re.sub(r'[\(\[\{].*?[\)\]\}]', "", result)
    result = result.replace(' and ', ', ').split(', ')
    return result

def extract_year(paper_id):
    try: 
        year = 2000 + int(paper_id[:2])
    except:
        year = 0
    return year

def log(i: str) -> str:
    """ A simple logger

    :param i: the log message
    :type i: str
    """    
    now = datetime.datetime.utcnow()
    print(now, "| " + i)