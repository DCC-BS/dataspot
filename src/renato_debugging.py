import logging
import os
from time import sleep

from dotenv import load_dotenv
from dataspot_client import DataspotClient
from dataspot_dataset import BasicDataset, OGDDataset
import json

import ods_utils_py as ods_utils

from metadata_translator import ods_to_dataspot


def main():
    load_dotenv('../../.dataspot.env')
    
    base_url = os.getenv("DATASPOT_API_BASE_URL")
    if not base_url:
        raise ValueError("DATASPOT_API_BASE_URL environment variable is not set")
    
    client = DataspotClient(base_url)

    # Test DNK download and save
    if False:
        dnk_data = client.download_dnk()
        print("Successfully downloaded DNK:")
        print(json.dumps(dnk_data, indent=4)[:500] + "...")

        output_path = client.save_dnk()
        print(f"\nSaved DNK to: {output_path}")


    name_departement = "Test-Departement"
    name_dienststelle = "Test-Dienststelle"
    title_sammlung = "Test-Sammlung"

    # Test teardown
    if True:
        logging.info("\nTearing down DNK assets...")
        client.teardown_dnk()
        logging.info("Successfully deleted all DNK assets")

    # Test creating new department
    if False:
        client.create_new_department(name=name_departement)

    # Test creating new dienststelle
    if False:
        client.create_new_dienststelle(name=name_dienststelle, belongs_to_department=name_departement)

    # Test creating new sammlung
    if False:
        client.create_new_sammlung(title=title_sammlung, belongs_to_dienststelle=name_dienststelle)

    # Test creation and uploading of minimal OGD dataset
    if False:
        test_dataset_ogd = OGDDataset(
            _PATH=f"{name_departement}/{name_dienststelle}/{title_sammlung}",
            name="OGD-Dataset-TEST-API",
            kurzbeschreibung="Testweise Kurzbeschreibung")

        client.create_new_dataset(dataset=test_dataset_ogd)

    # Test creation of actual ods dataset
    if False:
        ods_metadata = ods_utils.get_dataset_metadata(dataset_id='100382')
        dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata)

        client.create_new_dataset(dataset=dataspot_dataset)
    
    # Test whether the path is correctly determined
    if False:
        logging.info("Retrieving all public dataset ids...")
        ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
        logging.info(f"Found {len(ods_ids)} ids.")
        for index, ods_id in enumerate(ods_ids):
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata)
            departement, dienststelle, sammlung, subsammlung = dataspot_dataset.get_departement_dienststelle_sammlung_subsammlung()
            logging.info(f"({index+1}/{len(ods_ids)}) {ods_id}: {dataspot_dataset.name}")
            logging.info(f" {departement=}")
            logging.info(f" {dienststelle=}")
            logging.info(f" {sammlung=}")
            logging.info(f" {subsammlung=}")
            logging.info("")

            sleep(1)

            if index >= 10:
                break

    # Test creation of all datasets from ods
    if True:
        logging.info("Retrieving all public dataset ids...")
        ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
        logging.info(f"Found {len(ods_ids)} ids.")
        for index, ods_id in enumerate(ods_ids):
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata)
            
            logging.info(f"({index + 1}/{len(ods_ids)}) {ods_id}: {dataspot_dataset.name}")

            client.create_new_dataset(dataset=dataspot_dataset)

            # Sleep for 1 second to be kind to dataspot servers
            sleep(1)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
    