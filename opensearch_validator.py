import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import csv
import json
import os
import sys
import glob
import datetime
import pprint
import operator
import functools
from bs4 import BeautifulSoup as Soup

COLLECTION_IDS = [
    "SENTINEL1_L1_SLC",
    "SENTINEL1_L1_GRD",
    "SENTINEL1_L2_OCN",
    "SENTINEL2_L1C",
    "SENTINEL2_L2A",
    "SENTINEL3_SRAL_L1_CAL",
    "SENTINEL3_SRAL_L1_SRA",
    "SENTINEL3_SRAL_L2_LAN",
    "SENTINEL3_SRAL_L2_WAT",
    "SENTINEL3_OLCI_L1_EFR",
    "SENTINEL3_OLCI_L1_ERR",
    "SENTINEL3_OLCI_L2_LFR",
    "SENTINEL3_OLCI_L2_LRR",
    "SENTINEL3_SLSTR_L1_RBT",
    "SENTINEL3_SLSTR_L2_LST",
    "SENTINEL5P_OFFL_L1B",
    "SENTINEL5P_OFFL_L2",
    "SENTINEL5P_NRTI_L2",
    "SENTINEL5P_RPRO_L2",
    "METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS",
    "METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2",
    "METNO-GLO-SEAICE_CONC-SOUTH-L4-NRT-OBS",
    "ARCTIC_ANALYSIS_FORECAST_PHYS_002_001_A",
    "SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046",
    "GLOBAL_ANALYSIS_FORECAST_PHY_001_024",
    "MULTIOBS_GLO_PHY_NRT_015_003",
    "METOP_A_GOME2_O3",
    "METOP_A_GOME2_NO2",
    "METOP_A_GOME2_TropNO2",
    "METOP_A_GOME2_SO2",
    "METOP_A_GOME2_SO2mass",
    "PROBAV_L2A_333M_V001",
    "PROBAV_S1-TOC_1KM_V001",
    "PROBAV_S1-TOA_1KM_V001",
    "PROBAV_S10-TOC_1KM_V001",
    "PROBAV_S10-TOC-NDVI_1KM_V001",
    "PROBAV_L2A_1KM_V001",
    "PROBAV_P_V001",
    "PROBAV_S1-TOC_333M_V001",
    "PROBAV_S1-TOA_333M_V001",
    "PROBAV_S10-TOC_333M_V001",
    "PROBAV_S10-TOC-NDVI_333M_V001",
    "PROBAV_S1-TOC_100M_V001",
    "PROBAV_S1-TOA_100M_V001",
    "PROBAV_S1-TOC-NDVI_100M_V001",
    "PROBAV_S5-TOC_100M_V001",
    "PROBAV_S5-TOA_100M_V001",
    "PROBAV_S5-TOC-NDVI_100M_V001",
    "PROBAV_L2A_100M_V001",
    "OPEN_LAND_USE_MAP",
    "TREE_SPECIES_DISTRIBUTION_HABITAT_SUITABILITY",
    "FLOOD_HAZARD_EU_GL",
    "RSP_AVHRR_1KM_ANNUAL_USA",
    "EMODIS_PHENOLOGY_250M_ANNUAL_USA",
    "EBV",
    "DE2_PM4_L1B",
    "DE2_PSH_L1B",
    "DE2_PSH_L1C",
    "NEXTGEOSS_SENTINEL2_FAPAR",
    "NEXTGEOSS_SENTINEL2_FCOVER",
    "NEXTGEOSS_SENTINEL2_LAI",
    "NEXTGEOSS_SENTINEL2_NDVI",
    "UNWRAPPED_INTERFEROGRAM",
    "WRAPPED_INTERFEROGRAM",
    "SPATIAL_COHERENCE",
    "LOS_DISPLACEMENT_TIMESERIES",
    "INTERFEROGRAM_APS_GLOBAL_MODEL",
    "MAP_OF_LOS_VECTOR",
    "SIMOCEAN_SURFACE_WIND_FORECAST_FROM_AROME",
    "SIMOCEAN_CLOUDINESS_FORECAST_FROM_AROME",
    "SIMOCEAN_SURFACE_CURRENTS_FROM_HF_RADAR",
    "SIMOCEAN_NEARSHORE_SEA_STATE_FORECAST_FROM_SWAN",
    "SIMOCEAN_PORT_SEA_STATE_FORECAST_FROM_SMARTWAVE",
    "SIMOCEAN_DATA_FROM_MULTIPARAMETRIC_BUOYS",
    "SIMOCEAN_TIDAL_DATA",
    "SIMOCEAN_SURFACE_FORECAST_FROM_HYCOM",
    "SIMOCEAN_SEA_SURFACE_WIND_FORECAST",
    "SIMOCEAN_MEAN_SEA_LEVEL_PRESSURE_FORECAST",
    "SIMOCEAN_SEA_SURFACE_TEMPERATURE_FORECAST",
    "SIMOCEAN_AIR_SURFACE_TEMPERATURE_FORECAST",
    "SIMOCEAN_SEA_WAVE_DIRECTION_FORECAST",
    "SIMOCEAN_SEA_WAVE_PERIOD_FORECAST",
    "SIMOCEAN_PRECIPITATION_FORECAST_FROM_AROME",
    "SIMOCEAN_SIGNIFICANT_WAVE_HEIGHT_FORECAST",
    "EBAS_NILU_DATA_ARCHIVE",
    "AVERAGE_FLOOD_SIGNAL",
    "AVERAGE_FLOOD_MAGNITUDE",
]

OS_CHECKS = {
    'OgcOpenSearchEarthObservationExtension': ['validate_osdd_ogc_eox_extension_compliance',
                                               'validate_osdd_ogc_eox_base_compliance',
                                               'validate_response_ogc_eox_extension_compliance'],
    'OgcOpenSearchGeoTimeExtension': ['validate_osdd_ogc_opensearch_time_compliance',
                                      'validate_response_ogc_atom_feed_compliance',
                                      'validate_response_ogc_atom_opensearch_compliance',
                                      'validate_response_ogc_atom_opensearch_geo_compliance',
                                      'validate_response_ogc_atom_opensearch_time_compliance',
                                      'validate_osdd_ogc_opensearch_geo_compliance',
                                      'validate_osdd_ogc_opensearch_base_compliance'],
    'OpenSearchCeosBestPractices': ['validate_collection_and_granule_search_support',
                                    'validate_relation_attribute_in_url_element',
                                    'validate_data_access_links_in_result',
                                    'validate_minimum_bounding_rectangle_in_result',
                                    'validate_optional_query_param_empty_or_missing',
                                    'validate_relationship_attribute_values_in_result',
                                    'validate_specification_declaration_in_tags',
                                    'validate_atom_summary_in_result',
                                    'validate_browse_links_in_result',
                                    'validate_provenance_information_in_result',
                                    'validate_spatial_extents_in_result',
                                    'validate_link_media_type_in_result',
                                    'validate_sample_query_in_osdd',
                                    'validate_start_index_over_start_page_in_result',
                                    'validate_index_and_page_offset_in_url',
                                    'validate_parameter_extension_support',
                                    'validate_temporal_extents_expressed_as_dublin_core_dates_in_result',
                                    'validate_supported_search_parameters',
                                    'validate_handling_of_unsupported_request_parameters',
                                    'validate_metadata_links_in_result',
                                    'validate_presence_of_dc_identifier_in_result',
                                    'validate_geo_name_search',
                                    'validate_parameter_extension_free_text_notation_support',
                                    'validate_parameter_extension_geometry_type_profile_support',
                                    'validate_atom_format_results',
                                    'validate_reference_to_documentation_in_result',
                                    'validate_search_with_satellite_name',
                                    'validate_client_supplied_identifier_in_osdd',
                                    'validate_result_set_navigation',
                                    'validate_output_encoding_as_extension'],
    'OpenSearchCeosDeveloperGuide': ['validate_parameter_extension_osdd_support',
                                     'validate_parameter_definition_support',
                                     'validate_parameter_extension_geometry_type_profile_osdd_support',
                                     'validate_temporal_extents_expressed_as_dublin_core_dates_in_result_support',
                                     'validate_supported_search_parameters_in_osdd',
                                     'validate_start_index_value_in_result',
                                     'validate_relationship_attribute_values_in_result_support',
                                     'validate_minimum_bounding_rectangle_in_result_support',
                                     'validate_parameter_extension_free_text_notation_osdd_support',
                                     'validate_two_step_searching_support',
                                     'validate_atom_format_results_support',
                                     'validate_relation_attribute_in_url_element_support',
                                     'validate_client_supplied_identifier_in_osdd_support',
                                     'validate_result_set_navigation_support',
                                     'validate_output_encoding_as_extension_support',
                                     'validate_metadata_links_in_result_support'],
    'OpenSearchEsipBestPractice': ['validate_presence_of_parent_link_element_in_collection_level_result',
                                   'validate_categorization_of_link_elements_in_result',
                                   'validate_spatial_extents_have_corresponding_mbr',
                                   'validate_version',
                                   'validate_temporal_extents_expressed_as_dc_dates',
                                   'validate_two_step_searching',
                                   'validate_esip_namespace_is_present',
                                   'validate_categorization_of_url_elements_in_osdd',
                                   'validate_collection_template_has_search_terms_query_parameter'],
    'OpenSearchGeoExtension': ['validate_geo_bounding_box_parameter_is_allowed',
                               'validate_geo_uid_parameter_is_allowed',
                               'validate_geo_namespace_is_present',
                               'validate_search_response_georss_boundingbox_extent_if_present_is_valid'],
    'OpenSearchHttp': ['validate_osdd_accessible'],
    'OpenSearchParamExtension': ['validate_parameter_attributes',
                                 'validate_url_method',
                                 'validate_url_enctype',
                                 'validate_param_namespace'],
    'OpenSearchRelevanceExtension': ['validate_relevance_scores'],
    'OpenSearchSpecification': ['validate_description',
                                'validate_short_name',
                                'validate_url_of_type_atom',
                                'validate_well_formed_xml',
                                'validate_osdd_contains_OpenSearchDescription_element_with_correct_namespace',
                                'validate_result_contains_totalResults_itemsPerPage_startIndex',
                                'validate_osdd_schema_compliance',
                                'validate_search_response_has_request_search_query',
                                'validate_url_template_syntax',
                                'validate_query_example',
                                'validate_response_schema_compliance'],
    'OpenSearchTimeExtension': ['validate_time_end_parameter',
                                'validate_time_namespace_is_present',
                                'validate_time_start_parameter']
}

def requests_retry_session(retries=3, backoff_factor=0.2, status_forcelist=(500, 502, 503, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def validate_collection(model, collection_id):
    """Perform OpenSearch checks on collection
        Returns: collection_id as a dict
    """

    base_os_url = "https://opensearch-ui.earthdata.nasa.gov/validations/execute_validation.json"
    osdd = "https://catalogue-staging.nextgeoss.eu/opensearch/description.xml?osdd={}".format(collection_id)
    print('Checking {}'.format(osdd))
    checks = OS_CHECKS[model]
    results = {}
    for check in checks:
        try:
            r = requests_retry_session(retries=3).get(
                base_os_url,
                params={
                    'model': model,
                    'method': check,
                    'osdd': osdd,
                },
            )
        except requests.exceptions.HTTPError as e:
            print('Couldn\'t perform check `{0}` for collection `{1}`: {2}'.format(check, collection_id, e.message))
            continue
        else:
            results[check] = r.json()
    return results


if __name__ == "__main__":
    """
        Validate Collections against OpenSearch automatically
        To run this script:
            $ pip install -r requirements.txt
            $ python openserch_geotime_validator.py
    """
    for model in OS_CHECKS:
        with open('./opensearch_results/failing_{0}.csv'.format(model), 'w') as csvfile:
            headers = ['collection_id', 'check', 'score', 'error', 'hint']
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for collection_id in COLLECTION_IDS:
                collection_results = validate_collection(model, collection_id)
                for check, result in collection_results.items():
                    if result['score'] < 5:
                        row = {
                            'collection_id': collection_id,
                            'check': check,
                            'score': result['score'],
                            'error': result['failure_detail'],
                            'hint': result.get('further_remarks'),
                        }
                        writer.writerow(row)
