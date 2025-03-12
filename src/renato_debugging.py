import logging
from time import sleep

from ods_client import ODSClient
from dataspot_client import DataspotClient
from dataspot_dataset import OGDDataset
import json

import ods_utils_py as ods_utils

from metadata_translator import ods_to_dataspot


def main():
    client = DataspotClient()

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
        client.teardown_dnk(delete_empty_collections=True)
        logging.info("Successfully deleted all DNK assets")

    # Test creating new department
    if False:
        client.dnk_create_new_department(name=name_departement)

    # Test creating new dienststelle
    if False:
        client.dnk_create_new_dienststelle(name=name_dienststelle, belongs_to_department=name_departement)

    # Test creating new sammlung
    if False:
        client.dnk_create_new_sammlung(title=title_sammlung, belongs_to_dienststelle=name_dienststelle)

    # Test creation and uploading of minimal OGD dataset
    if False:
        test_dataset_ogd = OGDDataset(
            _PATH=f"{name_departement}/{name_dienststelle}/{title_sammlung}",
            name="OGD-Dataset-TEST-API",
            kurzbeschreibung="Testweise Kurzbeschreibung")

        client.dnk_create_or_update_dataset(dataset=test_dataset_ogd)

    # Test creation of actual ods dataset
    if True:
        ods_metadata = ods_utils.get_dataset_metadata(dataset_id='100382')
        dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata, ods_dataset_id='100382', dataspot_client=client)

        client.delete_dataset(title="Solarkataster: Solarpotenzial")
        client.dnk_create_or_update_dataset(dataset=dataspot_dataset)
    
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
    if False:
        logging.info("Retrieving all public dataset ids...")
        ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
        logging.info(f"Found {len(ods_ids)} ids.")
        for index, ods_id in enumerate(ods_ids):
            logging.info(f"({index + 1}/{len(ods_ids)}) {ods_id}")
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata=ods_metadata,
                                                           ods_dataset_id=ods_id,
                                                           dataspot_client=client)
            logging.info(f"Title: {dataspot_dataset.name}")

            client.dnk_create_or_update_dataset(dataset=dataspot_dataset)

            # Sleep for 1 second to be kind to dataspot servers
            sleep(1)

def main_2_rdm():
    client = DataspotClient()

    # Teardown
    if True:
        logging.info("\nTearing down DNK assets...")
        client.teardown_dnk()
        logging.info("Successfully deleted all DNK assets")

    # Create 10 datasets in dataspot from ods
    if True:
        logging.info("Retrieving all public dataset ids...")
        #ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
        ods_ids = ['100003', '100004', '100005', '100006', '100007', '100008', '100009', '100010', '100011', '100013', '100014', '100015', '100016', '100017', '100018', '100019', '100020', '100021', '100022', '100023', '100024', '100025', '100026', '100027', '100028', '100029', '100030', '100031', '100032', '100033', '100034', '100035', '100036', '100037', '100038', '100039', '100040', '100041', '100042', '100043', '100044', '100046', '100047', '100048', '100049', '100050', '100051', '100052', '100053', '100054', '100056', '100057', '100058', '100059', '100060', '100061', '100062', '100063', '100064', '100065', '100066', '100067', '100068', '100069', '100070', '100071', '100072', '100073', '100074', '100075', '100076', '100077', '100078', '100079', '100080', '100081', '100082', '100083', '100084', '100085', '100086', '100087', '100088', '100089', '100090', '100092', '100093', '100094', '100095', '100096', '100097', '100098', '100099', '100100', '100101', '100102', '100103', '100104', '100105', '100106', '100107', '100108', '100109', '100110', '100111', '100112', '100113', '100114', '100116', '100117', '100118', '100120', '100121', '100122', '100123', '100124', '100125', '100126', '100127', '100128', '100129', '100131', '100132', '100133', '100134', '100135', '100136', '100137', '100138', '100139', '100143', '100144', '100145', '100146', '100148', '100149', '100151', '100152', '100153', '100154', '100155', '100156', '100158', '100160', '100161', '100162', '100163', '100164', '100165', '100168', '100169', '100170', '100171', '100172', '100173', '100174', '100175', '100176', '100177', '100178', '100179', '100180', '100181', '100182', '100183', '100186', '100187', '100188', '100189', '100191', '100192', '100193', '100194', '100195', '100196', '100197', '100198', '100199', '100200', '100201', '100202', '100206', '100207', '100213', '100214', '100215', '100216', '100221', '100223', '100225', '100226', '100227', '100229', '100230', '100231', '100232', '100233', '100234', '100235', '100236', '100238', '100239', '100240', '100241', '100242', '100243', '100244', '100245', '100246', '100247', '100249', '100250', '100251', '100252', '100253', '100254', '100255', '100256', '100257', '100259', '100268', '100269', '100270', '100271', '100272', '100273', '100274', '100275', '100276', '100277', '100278', '100279', '100280', '100281', '100282', '100283', '100284', '100285', '100286', '100287', '100288', '100290', '100291', '100292', '100293', '100294', '100295', '100296', '100297', '100298', '100299', '100300', '100301', '100302', '100304', '100305', '100306', '100307', '100308', '100309', '100310', '100311', '100312', '100313', '100314', '100316', '100317', '100318', '100319', '100320', '100321', '100323', '100325', '100326', '100327', '100328', '100329', '100330', '100331', '100332', '100333', '100334', '100335', '100336', '100337', '100338', '100339', '100340', '100341', '100345', '100346', '100348', '100352', '100353', '100354', '100355', '100356', '100357', '100358', '100360', '100361', '100362', '100363', '100365', '100369', '100370', '100377', '100378', '100379', '100380', '100381', '100382', '100383', '100384', '100385', '100386', '100387', '100388', '100389', '100391', '100392', '100393', '100394', '100397', '100399', '100400', '100401', '100402', '100403', '100405', '100406', '100407', '100408', '100413', '100414', '100417', '100420']

        logging.info(f"Found {len(ods_ids)} ids.")
        for index, ods_id in enumerate(ods_ids):
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata, ods_id, client)

            logging.info(f"({index + 1}/{len(ods_ids)}) {ods_id}: {dataspot_dataset.name}")

            client.dnk_create_or_update_dataset(dataset=dataspot_dataset)

            # Sleep for 1 second to be kind to dataspot servers
            sleep(1)

            if index >= 9:
                break

def main_3_tdm():
    client = DataspotClient()
    ods_client = ODSClient()

    # Teardown TDM
    if False:
        logging.info("\nTearing down TDM dataobjects...")
        client.teardown_tdm()
        logging.info("Successfully deleted all TDM dataobjects in the 'Automatisch generierte ODS-Datenmodelle' collection")

    # Add an asset called "Test-Datenobjekt" with example attributes
    if False:
        logging.info("Creating new asset...")
        client.tdm_create_or_update_dataobject(name="Test-Asset API")
        logging.info("Successfully created new asset")
        pass

    # Extract the column names of an ods dataset
    if False:
        dataset_id = '100397'
        title = ods_utils.get_dataset_title(dataset_id=dataset_id)
        columns = ods_client.get_dataset_columns(dataset_id=dataset_id)
        client.tdm_create_or_update_dataobject(name=title, columns=columns)

    # Teardown and then create 10 datasets in the DNK and the corresponding TDM
    if True:
        ods_ids = ['100003', '100004', '100005', '100006', '100007', '100008', '100009', '100010', '100011', '100013']
        client.teardown_dnk()
        client.teardown_tdm()

        for index, ods_id in enumerate(ods_ids):
            # Create entry in DNK
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata, ods_id, client)

            logging.info(f"({index + 1}/{len(ods_ids)}) {ods_id}: {dataspot_dataset.name}")

            client.dnk_create_or_update_dataset(dataset=dataspot_dataset)

            # Create entry in TDM
            title = ods_utils.get_dataset_title(dataset_id=ods_id)
            columns = ods_client.get_dataset_columns(dataset_id=ods_id)
            client.tdm_create_or_update_dataobject(name=title, columns=columns)

            # Sleep for 1 second to be kind to dataspot servers
            sleep(1)

def main_4_dnk_tdm_linked():
    client = DataspotClient()
    ods_client = ODSClient()
    start_at = 100046

    # Teardown and then create 10 datasets in the DNK and the corresponding TDM
    if True:
        ods_ids = ods_utils.get_all_dataset_ids(include_restricted=False)
        #client.teardown_dnk()
        #client.teardown_tdm()

        for index, ods_id in enumerate(ods_ids):
            if int(ods_id) < start_at:
                continue

            # Create entry in DNK
            ods_metadata = ods_utils.get_dataset_metadata(dataset_id=ods_id)
            dataspot_dataset: OGDDataset = ods_to_dataspot(ods_metadata, ods_id, client)

            logging.info(f"({index + 1}/{len(ods_ids)}) {ods_id}: {dataspot_dataset.name}")

            client.dnk_create_or_update_dataset(dataset=dataspot_dataset)

            # Create entry in TDM
            title = ods_utils.get_dataset_title(dataset_id=ods_id)
            columns = ods_client.get_dataset_columns(dataset_id=ods_id)
            client.tdm_create_or_update_dataobject(name=title, columns=columns)

            # Link TDM to DNK
            client.link_dnk_bestandteile_to_tdm(title=dataspot_dataset.name)
    pass

def main_5_build_organization_structure_in_dnk():
    """
    Build the organization structure in Dataspot's DNK scheme based on data from the ODS API.
    
    This method:
    1. Retrieves organization data from the ODS API
    2. Builds the organization hierarchy in Dataspot using a path-depth approach
       where organizations are processed level by level based on their path depth
    3. This ensures parent organizations are always created before their children
    """
    logging.info("Starting organization structure build...")
    
    # Initialize clients
    ods_client = ODSClient()
    dataspot_client = DataspotClient()
    
    # Configuration for data retrieval and processing
    batch_size = 100         # Number of records to retrieve in each API call
    max_batches = None       # Maximum number of batches to retrieve (set to None for all)
    cooldown_delay = 1.0     # Delay in seconds between API calls to prevent overloading the server
    all_organizations = {"results": []}
    
    # Fetch organization data in batches
    logging.info("Fetching organization data from ODS API...")
    batch_count = 0
    total_retrieved = 0
    
    try:
        while True:
            # Get the next batch of organization data
            offset = batch_count * batch_size
            batch_data = ods_client.get_organization_data(limit=batch_size, offset=offset)
            
            # Check if we received any results
            batch_results = batch_data.get('results', [])
            num_results = len(batch_results)
            
            if num_results == 0:
                # No more results, break out of the loop
                break
                
            # Add the batch results to our collected data
            all_organizations['results'].extend(batch_results)
            total_retrieved += num_results
            
            logging.info(f"Retrieved batch {batch_count + 1} with {num_results} organizations (total: {total_retrieved})")
            
            # Check if we've reached our batch limit
            batch_count += 1
            if max_batches is not None and batch_count >= max_batches:
                logging.info(f"Reached the maximum number of batches ({max_batches})")
                break
        
        # Set the total count in the combined data
        all_organizations['total_count'] = batch_data.get('total_count', total_retrieved)
        logging.info(f"Total organizations retrieved: {total_retrieved} (out of {all_organizations['total_count']})")
        
        # Optionally clear existing structure before building
        if True:  # Set to True to clear existing structure
            logging.info("Clearing existing organization structure...")
            dataspot_client.teardown_dnk(delete_empty_collections=True)
        
        # Build the organization hierarchy in Dataspot
        logging.info("Building organization hierarchy in Dataspot...")
        try:
            dataspot_client.build_organization_hierarchy_from_ods(
                all_organizations, 
                root_id="255",
                cooldown_delay=cooldown_delay
            )
            logging.info("Organization structure build completed successfully")
        except Exception as e:
            logging.error(f"Error building organization hierarchy: {str(e)}")
            logging.info("Organization structure build partially completed with errors")
    
    except KeyboardInterrupt:
        logging.info("Operation was interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        logging.info("Organization structure build process finished")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f'Executing {__file__}...')
    main_5_build_organization_structure_in_dnk()
    logging.info('Job successful!')
    