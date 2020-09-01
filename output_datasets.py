import requests
import json
import os
import glob
import datetime
import configparser

from bs4 import BeautifulSoup as Soup


def read_config_parameters():
    read_config = configparser.ConfigParser()
    read_config.read("credentials.ini")

    return read_config


class Datasets(object):
    def __init__(self):
        self.config = read_config_parameters()
        self.ckan_url = self.config.get("ckan", "ckan_url")
        self.api_token = self.config.get("ckan", "ckan_api_token")

    def get_dataset_information(self):
        owner_org = raw_input('Name of the owner organisation: ')
        print "The datasets that are going to be added with the script, will belong to ", owner_org

        group = raw_input('Name of Pilot/Topic: ')
        print "The datasets that are going to be added with the script will be associated with ", group

        path_to_file = raw_input('Enter path to pilots XML file: ')
        print "Path to pilot XML file is: ", path_to_file

        pathFormat = os.path.join(path_to_file, '*.xml')
        filePaths = glob.glob(pathFormat)

        for filePath in filePaths:
            package_attrs = self.parse_xml(owner_org, group, filePath)
            existing_package = self.search_package(package_attrs)
            try:
                package_response = self.upsert_package(package_attrs, existing_package)
            except requests.exceptions.HTTPError as e:
                continue
            else:
                resource_response = self.create_resource(package_response['result'], filePath)


    def parse_xml(self, owner_org, group, path_to_file):
        """
            Parses original metadata file.
            Returns a package dict
        """
        f = open(path_to_file, 'r')
        raw_xml = f.read()
        soup_resp = Soup(raw_xml, 'xml')

        data_dict = {
            'private': False,
            'owner_org': owner_org,
        }

        data_dict['groups'] = [{"name" : group}]

        extras = [
            { 'key': 'is_output', 'value': True }
        ]

        fields_mapping = {
            'purpose': 'purpose',
            'lineage': 'lineage',
            'supplementalInformation': 'supplemental_information',
            'fileIdentifier': 'file_identifier',
        }

        for item_node in soup_resp:

            for subitem_node in item_node.findChildren():
                key = subitem_node.name
                value = subitem_node.text.rstrip()

                if key == 'title':
                    data_dict['title'] = value.strip()
                    data_dict['name'] = value.strip().replace(' ', '_').lower()

                if key == 'abstract':
                    data_dict['notes'] = value.strip()

                if key == 'resourceMaintenance':
                    for maintanance_attr in subitem_node.findChildren():
                        if maintanance_attr.name == 'individualName':
                            data_dict['maintainer'] = maintanance_attr.text.strip()

                        if maintanance_attr.name == 'electronicMailAddress':
                            data_dict['maintainer_email'] = maintanance_attr.text.strip()

                if key == 'MD_LegalConstraints':
                    legalNotice = subitem_node.find('CharacterString')
                    extras.append({ 'key': 'legal_notice', 'value': legalNotice.text.strip() })

                # We're only given the date so we make timerange_start the start of that day
                # and timerange_end the end of that day
                if key == 'beginPosition':
                    start_date = datetime.datetime.strptime(value, '%Y-%m-%d')
                    extras.append({ 'key': 'timerange_start', 'value': start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z") })

                if key == 'endPosition':
                    end_date = datetime.datetime.strptime(value, '%Y-%m-%d')
                    end_date = end_date.replace(hour=23, minute=59, second=59)
                    extras.append({ 'key': 'timerange_end', 'value': end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z") })

                for extra_field, normalized_field in fields_mapping.items():
                    if key == extra_field:
                        extras.append({ 'key': normalized_field, 'value': value.strip() })

            # Spatial
            for coordinates in item_node.find_all('EX_GeographicBoundingBox'):
                for c in coordinates.findChildren():
                    if c.name == 'westBoundLongitude':
                        west = c.text.strip()
                    if c.name == 'eastBoundLongitude':
                        east = (c.text).strip()
                    if c.name == 'southBoundLatitude':
                        south = (c.text).strip()
                    if c.name == 'northBoundLatitude':
                        north = (c.text).strip()

                coord_tmp = []
                coord_tmp.append(west)
                coord_tmp.append(east)
                coord_tmp.append(south)
                coord_tmp.append(north)

            coord = '{ "type": "Polygon", "coordinates": [[['+west+','+south+'], ['+east+','+south+'], ['+east+','+north+'], ['+west+','+north+'], ['+west+', '+south+']]]}'

            extras.append({'key': 'spatial', 'value': coord.strip()})

            collection_name = raw_input('Dataset ' + data_dict['name'] + ' will belong to COLLECTION_NAME: ')
            collection_id = raw_input('Dataset ' + data_dict['name'] + ' will belong to COLLECTION_ID: ')
            extras.append({'key': 'collection_id', 'value': collection_id})
            extras.append({'key': 'collection_name', 'value': collection_name})


            # Resources
            resources = []
            resource_fields = ['url', 'name', 'description', 'format']
            for resource in item_node.find_all('CI_OnlineResource'):
                resource_data = {}
                for r in resource.findChildren():
                    key = r.name.lower()
                    value = r.text.strip()
                    if key in resource_fields:
                        resource_data[key] = value
                resources.append(resource_data)
            data_dict['resources'] = resources
            data_dict['no_resources'] = len(resources)

        data_dict['extras'] = extras

        return data_dict

    def search_package(self, package_attrs):
        """Searches for existing packages with the same name
            Returns: the attributes of the existing package as a dict or None
        """

        try:
            resp = requests.get('{}/api/action/package_show'.format(self.ckan_url),
                    params={ 'id': package_attrs['name'] })
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            print('Error retrieving package `{}`: {}'.format(package_attrs['name'], resp.text))
            return None
        else:
            return resp.json()

    def create_resource(self, package, filePath):
        """Creates a resource from the original xml file
            Returns: the attributes of the newly created resource as a dict
            Raises: request errors if any
        """

        resource_data = {
            'package_id': package['id'],
            'format': 'XML',
            "mimetype": "text/xml",
            "name": package['title'],
        }
        try:
            resp = requests.post('{}/api/action/resource_create'.format(self.ckan_url),
                    data=resource_data,
                    headers={"Authorization": self.api_token},
                    files=[('upload', open(filePath))])
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            print('Error creating resource `{}`: {}'.format(filePath, resp.text))
            print('Resource attributes: {}'.format(resource_data))
        else:
            print('Successfully created resource `{}`'.format(filePath))
            return resp.json()

    def upsert_package(self, package_attrs, existing_package=None):
        """Creates a package (dataset) in NextGEOSS from package_attrs
            Returns: the attributes of the newly created package as a dict
            Raises: request errors if any
        """

        try:
            if existing_package is not None:
                resp = requests.post('{}/api/action/package_update'.format(self.ckan_url),
                    json.dumps(package_attrs),
                    headers = {"Authorization": self.api_token, 'content-type': 'application/json'},
                    verify=False)
            else:
                resp = requests.post('{}/api/action/package_create'.format(self.ckan_url),
                    json.dumps(package_attrs),
                    headers = {"Authorization": self.api_token, 'content-type': 'application/json'},
                    verify=False)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print('Error upserting dataset `{}`: {}'.format(package_attrs['name'], resp.text))
            print('Dataset attributes: {}'.format(package_attrs))
            raise e
        else:
            print('Successfully upserted package `{}`'.format(package_attrs['name']))
            return resp.json()

if __name__ == "__main__":
    """
        Upsert packages (datasets) from the LAI products (static EBVs) XML files.
        To run this script:
            $ pip install -r requirements.txt
            $ python output_datasets.py
    """
    dataset = Datasets()
    dataset.get_dataset_information()


