import logging
import os
from dotenv import load_dotenv
from dataspot_client import DataspotClient
import json

def main():
    load_dotenv('../..')
    
    base_url = os.getenv("DATASPOT_API_BASE_URL")
    if not base_url:
        raise ValueError("DATASPOT_API_BASE_URL environment variable is not set")
    
    client = DataspotClient(base_url)

    try:
        # Test DNK download and save
        if False:
            dnk_data = client.download_dnk()
            print("Successfully downloaded DNK:")
            print(json.dumps(dnk_data, indent=4)[:500] + "...")
            
            output_path = client.save_dnk()
            print(f"\nSaved DNK to: {output_path}")

        # Test teardown
        if True:
            logging.info("\nTearing down DNK assets...")
            client.teardown_dnk()
            logging.info("Successfully deleted all DNK assets")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
    