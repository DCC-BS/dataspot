import logging
import os
from dotenv import load_dotenv
from dataspot_client import DataspotClient
from dataspot_dataset import BasicDataset, OGDDataset
import json

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
    if False:
        logging.info("\nTearing down DNK assets...")
        client.teardown_dnk()
        logging.info("Successfully deleted all DNK assets")

    # Test creating new department
    if True:
        client.create_new_department(name=name_departement)

    # Test creating new dienststelle
    if True:
        client.create_new_dienststelle(name=name_dienststelle, belongs_to_department=name_departement)

    # Test creating new sammlung
    if True:
        client.create_new_sammlung(title=title_sammlung, belongs_to_dienststelle=name_dienststelle)

    title_dataset = "Test-Dataset"

    # Test uploading an empty dataset
    if False:
        client.create_new_dataset(title=title_dataset, belongs_to_sammlung=title_sammlung)

    # Test creation and uploading of minimal dataset
    if True:
        test_dataset_basic = BasicDataset(
            name="Basic-Dataset-TEST-API",
            kurzbeschreibung="Testweise Kurzbeschreibung")

        client.create_new_dataset(dataset=test_dataset_basic, belongs_to_sammlung=title_sammlung)

    # Test creation and uploading of minimal OGD dataset
    if True:
        test_dataset_ogd = OGDDataset(
            name="OGD-Dataset-TEST-API",
            kurzbeschreibung="Testweise Kurzbeschreibung")

        client.create_new_dataset(dataset=test_dataset_ogd, belongs_to_sammlung=title_sammlung)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
    