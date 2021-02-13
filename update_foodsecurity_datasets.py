"""
This script was created to update the foodsecurity packages which were already harvested.
It aims to:
- add them to the group `foodsecurity`
- mark them as output datasets
"""

import requests
import json
import os
import sys
import pprint

try:
    API_TOKEN = os.environ['API_TOKEN']
    CKAN_BASE_URL = os.environ['CKAN_BASE_URL']
except KeyError as missing_key:
    print('Please set the environment variable for {}'.format(missing_key))
    sys.exit(1)

DEFAULT_COLLECTION_IDS = ['NEXTGEOSS_SENTINEL2_FAPAR', 'NEXTGEOSS_SENTINEL2_FCOVER', 'NEXTGEOSS_SENTINEL2_LAI', 'NEXTGEOSS_SENTINEL2_NDVI']
COLLECTION_IDS = os.environ.get('COLLECTION_IDS')
if COLLECTION_IDS is None:
    COLLECTION_IDS = DEFAULT_COLLECTION_IDS
    print('Using default FoodSecurity collection ids: {}'.format(DEFAULT_COLLECTION_IDS))
    print('Overwrite COLLECTION_IDS otherwise')
else:
    COLLECTION_IDS = COLLECTION_IDS.split(',')

def get_collection_packages(collection_id):
    """Retrieve all the packages pertaining to the given organization
        Returns: list of packages
        Raises: request errors if any
    """

    query_params = {
        'q': 'collection_id:{}'.format(collection_id),
        'rows': 1000,
    }
    try:
        resp = requests.post('{}/api/action/package_search'.format(CKAN_BASE_URL),
            params=query_params,
            headers = {"Authorization": API_TOKEN, 'content-type': 'application/json'},
            verify=False)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print('Error retrieving packages for collection `{}`: {}'.format(collection_id, resp.text))
        raise e
    else:
        parsed_resp = resp.json()
        packages = parsed_resp['result']['results']
        return packages


def update_package(package):
    """Update package with missing info
        Returns: the attributes of the updated package as a dict
        Raises: request errors if any
    """

    package['is_output'] = True
    package['groups'] = [{'name': 'food-security'}]
    try:
        resp = requests.post('{}/api/action/package_update'.format(CKAN_BASE_URL),
            params={ 'id': package['id'] },
            data = json.dumps(package),
            headers = {"Authorization": API_TOKEN, 'content-type': 'application/json'},
            verify=False)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print('Error updating package `{}`: {}'.format(package['name'], resp.text))
        raise e
    else:
        print('Successfully updated package `{}`'.format(package['name']))
        return resp.json()


if __name__ == "__main__":
    for collection_id in COLLECTION_IDS:
        processed = 0
        packages = get_collection_packages(collection_id)
        for package in packages:
            try:
                update_package(package)
            except requests.exceptions.HTTPError as e:
                continue
            else:
                processed += 1
        print('Processed {} packages from collection {}'.format(processed, collection_id))
