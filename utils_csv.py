import requests
from logger_csv import main_logger as logger
import configparser
from datetime import datetime, timedelta
import csv
import os

config = configparser.RawConfigParser()
config.read('config.ini')


def get_api_data(api_url, token):
    """
    Fetch data from an API endpoint using a provided token.

    :param api_url: URL of the API endpoint.
    :param token: Authorization token for accessing the API.

    :return: Parsed JSON data from the API if the response status code is 200.
            None if there's an error or the status code is not 200.
    """
    try:
        # Make the API request with the provided token
        response = requests.get(
            api_url, headers={"Authorization": f"Token {token}"}
        )

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and return the JSON data
            data = response.json()
            return data
        else:
            # Handle non-200 status codes
            logger.error("API returned error or invalid data")
            logger.error(f"Error: Unable to fetch data from the API. Status code: {response.status_code}")
            return None

    except requests.RequestException as ex:
        # Handle general request exceptions
        logger.error("API returned error or invalid data")
        logger.error(f"Error making API request: {ex}")
        return None


def map_data_to_header(data):
    # Define the mapping between dictionary keys and CSV header names
    header_mapping = {
        'election_id': 'ExternalID',
        'election_name_encode': 'NameEncode',
        'election_name': 'ElectName',
        'election_date_updated': 'PubDate',
        'election_issues': 'ElectIssues',
        'is_snap_election': 'Snap',
        'original_election_year': 'OrigElectYear',
        'election_range_start_date': 'StartTime',
        'election_range_end_date': 'EndTime',
        'is_delayed_covid19': 'CovidDelay',
        'covid_effects': 'CovidEffects',
        'election_declared_start_date': 'ElectStartDate',
        'election_declared_end_date': 'ElectEndDate',
        'election_blackout_start_date': 'ElectBlackoutStartDate',
        'election_blackout_end_date': 'ElectBlackoutEndDate',
        'election_type': 'Category',
        'election_scope': 'SubCategory',
        'electoral_system': 'ElecSys',
        'election_commission_name': 'ElectCommName',
        'administring_election_commission_website': 'Url',
        'election_source': 'Source',
        'district_ocd_id': 'DistrictID',
        'district_name': 'Title',
        'district_country': 'CountryCode',
        'district_type': 'DistrictType',
        'government_functions': 'GovFun',
        'government_functions_updated_date': 'GovFunUpdate',
        'voter_registration_day_deadline': 'RegDeadline',
        'voting_age_minimum_inclusive': 'VotingAge',
        'eligible_voters': 'EligibleVoters',
        'first_time_voters': 'FirstTimeVoters',
        'voting_methods_type': 'VotingType',
        'voting_methods_primary': 'VotingPrimary',
        'voting_methods_start_date': 'VotingStartDate',
        'voting_methods_end_date': 'VotingEndDate',
        'voting_methods_execuse_required': 'Excuse',
        'voting_methods_instructions': 'Description',
        'days_offset': 'DaysOffset'
    }

    # Map the keys to the desired header names
    mapped_data = {header_mapping[key]: value for key, value in data.items()}

    return mapped_data


def insert_eguide_election_data(data,file_name):
    election_id = data['election_id']

    voting_methods_type = None
    voting_methods_primary = None
    voting_methods_start_date = None
    voting_methods_end_date = None
    voting_methods_execuse_required = None
    voting_methods_instructions = None

    # Check if data['voting_methods'] is not None
    if data.get('voting_methods') is not None:
        # Update values if voting_methods is present in data
        voting_methods_type = ' | '.join(str(method.get('type', '')) for method in data['voting_methods'])
        voting_methods_primary = ' | '.join(str(method.get('primary', '')) for method in data['voting_methods'])
        voting_methods_start_date = ' | '.join(str(method.get('start', '')) for method in data['voting_methods'])
        voting_methods_end_date = ' | '.join(str(method.get('end', '')) for method in data['voting_methods'])
        voting_methods_execuse_required = ' | '.join(
            str(method.get('excuse-required', '')) if method.get('excuse-required') is not None else '' for method in
            data['voting_methods']
        )
        voting_methods_instructions = ' | '.join(
            str(method.get('instructions', '')) for method in data['voting_methods']
        )

    # Extracting specific fields from the JSON data
    election_data = {
        'election_id': election_id,
        'election_name_encode': 'en_US',
        'election_name': data['election_name']['en_US'],
        'election_date_updated': data.get('date_updated', ''),
        'election_issues': data.get('election_issues', ''),
        'is_snap_election': data.get('is_snap_election', ''),
        'original_election_year': data.get('original_election_year', ''),
        'election_range_start_date': data.get('election_range_start_date', ''),
        'election_range_end_date': data.get('election_range_end_date', ''),
        'is_delayed_covid19': str(data.get('is_delayed_covid19', '')),
        'covid_effects': data.get('covid_effects', ''),
        'election_declared_start_date': data.get('election_declared_start_date', ''),
        'election_declared_end_date': data.get('election_declared_end_date', ''),
        'election_blackout_start_date': data.get('election_blackout_start_date', ''),
        'election_blackout_end_date': data.get('election_blackout_end_date', ''),
        'election_type': data.get('election_type', ''),
        'election_scope': data.get('election_scope', ''),
        'electoral_system': data.get('electoral_system', ''),
        'election_commission_name': data.get('election_commission_name', ''),
        'administring_election_commission_website': data.get('administering_election_commission_website', ''),
        'election_source': data.get('source', ''),
        'district_ocd_id': data['district'].get("district_ocd_id"),
        'district_name': data['district'].get("district_name"),
        'district_country': data['district'].get("district_country"),
        'district_type': data['district'].get("district_type"),
        'government_functions': data['government_functions'].get('details', ''),
        'government_functions_updated_date': data['government_functions'].get('updated', ''),
        'voter_registration_day_deadline': data.get('voter_registration_day', ''),
        'voting_age_minimum_inclusive': data.get('voting_age_minimum_inclusive', ''),
        'eligible_voters': data.get('eligible_voters', None),
        'first_time_voters': data.get('first_time_voters', None),
        'voting_methods_type': voting_methods_type,
        'voting_methods_primary': voting_methods_primary,
        'voting_methods_start_date': voting_methods_start_date,
        'voting_methods_end_date': voting_methods_end_date,
        'voting_methods_execuse_required': voting_methods_execuse_required,
        'voting_methods_instructions': voting_methods_instructions,
    }
    if election_data['election_declared_start_date'] is None:
        if election_data['election_range_start_date']:
            election_data['election_declared_start_date'] = election_data['election_range_start_date']
        else:
            election_data['election_range_start_date'] = election_data['voting_methods_start_date']
            election_data['election_declared_start_date'] = election_data['voting_methods_start_date']
    elif election_data['election_range_start_date'] is None:
        election_data['election_range_start_date'] = election_data['election_declared_start_date']
    elif election_data['voting_methods_start_date'] is None:
        election_data['voting_methods_start_date'] = election_data['election_declared_start_date']

    if not election_data['government_functions']:
        election_data['government_functions'] = None

    if not election_data['voting_methods_execuse_required']:
        election_data['voting_methods_execuse_required'] = None

    election_start_date_str = election_data.get("election_declared_start_date")
    election_start_date = datetime.strptime(election_start_date_str, '%Y-%m-%d')
    now = datetime.now()
    days_offset = election_start_date - now + timedelta(days=1)

    if days_offset.days < 0:
        election_data['days_offset'] = 1
    else:
        election_data['days_offset'] = days_offset.days

    mapped_data = map_data_to_header(election_data)
    file_exists = os.path.exists(file_name)

    with open(file_name, 'a', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=mapped_data.keys(), delimiter=',')

        # If the file doesn't exist, write the header
        if not file_exists or csv_file.tell() == 0:
            csv_writer.writeheader()
        csv_writer.writerow(mapped_data)

    return True
