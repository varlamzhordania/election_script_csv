import os
import requests
from logger_csv import main_logger as logger
import sys
import configparser
from datetime import datetime, timedelta
from utils_csv import get_api_data, insert_eguide_election_data

config = configparser.RawConfigParser()
config.read('config_csv.ini')

if __name__ == "__main__":
    total_inserted = 0
    api_endpoint = config["General"]["API_ENDPOINT"]
    api_token = config["General"]["API_TOKEN"]

    csv_dir = config["General"]["CSV_PATH"]

    now = datetime.now().strftime('%Y-%m-%d_%H-%M')
    csv_filename = os.path.join(csv_dir, f"{now}" + ".csv")

    try:
        logger.info("Election Guide API Script Running")
        logger.info(f"Script Run Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        api_data = get_api_data(api_endpoint, api_token)

        if api_data:
            logger.info("API returned payload is VALID")

            total_election_ids = len(api_data)
            today = datetime.today()
            previous_week = today - timedelta(days=7)
            election_ids_starting_from_previous_week = [
                record['election_id']
                for record in api_data
                if record.get("election_range_start_date") and datetime.strptime(
                    record.get("election_range_start_date"),
                    '%Y-%m-%d'
                ) >= previous_week
            ]

            election_ids_before_previous_week = [
                record['election_id']
                for record in api_data
                if record.get("election_range_start_date") and datetime.strptime(
                    record.get("election_range_start_date"),
                    '%Y-%m-%d'
                ) < previous_week
            ]

            logger.info(f"Found Total of {total_election_ids} unique Election IDs")
            logger.info("Processing Election Guide JSON payload data")
            logger.info(
                f"Found {len(election_ids_starting_from_previous_week)} Election IDs starting from 1 week before today {today}"
            )
            logger.info(
                f"Found {len(election_ids_before_previous_week)} Election IDs with Election Dates before previous week {previous_week}"
            )
            logger.info(f"Inserting Total of {len(election_ids_starting_from_previous_week)} unique Election IDs")
            raw_data = []
            for record in api_data:
                if record.get("election_range_start_date"):
                    record_start_date = datetime.strptime(record.get("election_range_start_date"), '%Y-%m-%d')

                    if record_start_date >= previous_week:
                        insert_eguide_election_data(record, csv_filename)
                        total_inserted += 1
                        logger.info(
                            f"Inserting Election ID {record['election_id']} – {record['election_name']['en_US']} – {record_start_date.strftime('%Y-%m-%d')}"
                        )
                        raw_data.append(record)

            logger.info("Election Guide Data Successfully Inserted")

        else:
            logger.info("Script Terminated")
            sys.exit(1)
    except Exception as ex:
        logger.error(f"An error occurred {ex}")
    finally:
        logger.info(f"INFO Election Guide Data Successfully Inserted {total_inserted} Election Records")
        logger.info(f"Terminating Script – {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
