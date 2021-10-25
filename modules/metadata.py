""" This modules contains general utility functions """

from os import path
import zipfile
import json
import requests


def _read_metadata_file(filename: str, max_size: int, skip_n_papers: int) -> list:
    """ converts and returns the arxiv data set from json to a list
    """
    data = []
    ids = set()
    count = 0

    print("Start loading ArXiv dataset -----------:", filename)
    file = open(filename)
    lines = file.readlines()
    for line in lines:
        if max_size > 0:
            if len(data) >= max_size:
                break
        count += 1
        if count <= skip_n_papers:
            continue
        line_loaded = json.loads(line)
        if line_loaded["id"] in ids:
            continue
        data.append(line_loaded)
        ids.add(line_loaded["id"])
        print("Number of papers loaded ---------------:", count, end='\r')
    print('\nDone loading ArXiv dataset ------------: load {}, skip {}'.format(len(data),count-len(data)))

    return data[:max_size]


def get_metadata(config: dict) -> list:
    """ converts and returns the arxiv data set from json to a list
    """

    if config is None or 'data' not in config or 'metadata_file' not in config['data']:
        return None

    max_size = -1
    if 'n_papers' in config['data']:
        max_size = config['data']['n_papers']
    skip = 0
    if 'skip_n_papers' in config['data']:
        skip = config['data']['skip_n_papers']

    location = config['data']['metadata_file']
    if path.exists(location):
        filename = location
    else:
        if 'http' in location:
            print("Downloading ArXiv dataset from --------:", location)
            response = requests.get(location)
            download = config['data']['metadata_dir'] + location.split('/')[-1]
            open(download, 'wb').write(response.content)
            if download.endswith('.zip'):
                with zipfile.ZipFile(download, 'r') as zip_ref:
                    if len(zip_ref.namelist()) > 0:
                        filename = config['data']['metadata_dir'] + zip_ref.namelist()[0]
                    zip_ref.extractall(config['data']['metadata_dir'])
            else:
                filename = download

    result = _read_metadata_file(filename, max_size, skip)

    return result
