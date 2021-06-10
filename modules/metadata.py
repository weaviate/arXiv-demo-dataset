""" This modules contains general utility functions """

import json


def get_metadata(config: dict) -> list:
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
    i = 0

    if config is None or 'data' not in config or 'metadata_file' not in config['data']:
        return None

    path = config['data']['metadata_file']
    if 'n_papers' in config['data']:
        max_size = config['data']['n_papers']
    skip_n_papers = 0
    if 'skip_n_papers' in config['data']:
        skip_n_papers = config['data']['skip_n_papers']

    print("Start loading ArXiv dataset -----------:", path)
    with open(path, 'r') as file:
        for line in file:
            if max_size > 0:
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
    print('Done loading ArXiv dataset ------------: load {}, skip {}'.format(len(data),i-len(data)))

    return data[:max_size]
