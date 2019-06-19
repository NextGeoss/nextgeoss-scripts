import requests
import json
import datetime
import pprint
from bs4 import BeautifulSoup as Soup

API_TOKEN ='a52a2808-d8d0-41dd-9ec5-447745c82d93'
CKAN_BASE_URL = 'http://localhost:5000'
OWNER_ORG_ID = 'epos'


def parse_xml(filePath):
    """Parses original metadata file.
        Returns a package dict
    """

    f = open(filePath, 'r')
    raw_xml = f.read()
    soup_resp = Soup(raw_xml, 'xml')

    data_dict = {
        'private': False,
        'owner_org': OWNER_ORG_ID,
    }
    extras = [
        { 'key': 'is_output', 'value': True }
    ]

    for item_node in soup_resp:

        for subitem_node in item_node.findChildren():
            key = subitem_node.name
            value = subitem_node.text.rstrip()

            if key == 'title':
                data_dict['title'] = value.strip()
                data_dict['name'] = value.strip().replace(' ', '_').lower()

            if key == 'abstract':
                data_dict['notes'] = value.strip()

            # We're only given the date so we make timerange_start the start of that day
            # and timerange_end the end of that day
            if key == 'beginPosition':
                start_date = datetime.datetime.strptime(value, '%Y-%m-%d')
                extras.append({ 'key': 'timerange_start', 'value': start_date.isoformat() })

            if key == 'endPosition':
                end_date = datetime.datetime.strptime(value, '%Y-%m-%d')
                end_date.replace(hour=23, minute=59)
                extras.append({ 'key': 'timerange_end', 'value': end_date.isoformat() })

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

def create_package(package_attrs):
    """Creates a package (dataset) in NextGEOSS from package_attrs
        Returns: undefined if everything is ok
        Raises: request errors if any
    """

    resp = requests.post('{}/api/action/package_create'.format(CKAN_BASE_URL),
        data = json.dumps(package_attrs),
        headers = {"Authorization": API_TOKEN, 'content-type': 'application/json'},
        verify=False)
    print resp.status_code
    print resp.text


if __name__ == "__main__":
    filePath = '/home/g/Documents/nextgeoss-scripts/bdd2f2af-757b-4612-af74-d0f00c72b2d5.xml'
    package_attrs = parse_xml(filePath)
    pprint.pprint(package_attrs)
    create_package(package_attrs)
