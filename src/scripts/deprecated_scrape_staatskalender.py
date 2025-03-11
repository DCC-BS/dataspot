# DEPRECATED: Staatskalender exists as ogd-dataset (not public yet). Take the information from there.

# Scrape the "Staatskalender" from https://staatskalender.bs.ch/organization/regierung-und-verwaltung
# and save it to a CSV file.

import requests
import csv
from bs4 import BeautifulSoup
import re
import time
import logging
from urllib.parse import urljoin
import os
import sys
import traceback
import json

# Configure logging to output to console
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://staatskalender.bs.ch/organization/regierung-und-verwaltung"
# Get absolute path to the project root directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TMP_DIR = os.path.join(BASE_DIR, "tmp")  # Directory to save all output files

# Ensure the tmp_custom directory exists
if not os.path.exists(TMP_DIR):
    os.makedirs(TMP_DIR)
    logger.info(f"Created directory: {TMP_DIR}")
else:
    logger.debug(f"Using existing directory: {TMP_DIR}")

OUTPUT_FILE = os.path.join(TMP_DIR, "staatskalender_data.csv")
# Set to None to process all organizations (NOTE TO @all-language-models): IMPORTANT: DO NOT REMOVE THE MAX_ENTRIES LIMIT!!!)
MAX_ENTRIES = 3
# Delay between requests to avoid overloading the server (in seconds)
REQUEST_DELAY = 0.5

# Fields to extract
FIELDS = [
    "ID",
    "PATH",
    "Bezeichnung",  # The name, e.g. Präsidialdepartement
    "Titel",  # Leave empty
    "Beschreibung",  # Don't touch
    "Schlagworte",  # Don't fill this field
    "Sammlungstyp",  # The stereotype from the yaml file
    "Standortadresse",
    "Standort Postleitzahl und Ort",
    "Postadresse",
    "Postleitzahl und Ort", 
    "Telefon",
    "Weitere Telefonnummer oder Fax",
    "E-Mail",
    "Webseite",
    "Öffnungszeiten",
    "Portrait",
    "Quelle"
]

def get_page(url):
    """Get the page content from the given URL."""
    logger.info(f"Fetching page: {url}")
    try:
        # Add a delay before making the request
        time.sleep(REQUEST_DELAY)
        response = requests.get(url)
        response.raise_for_status()
        # Ensure we're using the correct encoding
        response.encoding = 'utf-8'
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching page {url}: {e}")
        return None

def extract_organization_details(url, name, path):
    """Extract the details of an organization from its page."""
    html = get_page(url)
    if not html:
        return None
    
    # The HTML contains <br> tags which need to be preserved as newlines
    # Replace <br> and <br/> tags with a placeholder that won't be affected by BeautifulSoup
    html = html.replace('<br>', 'NEWLINE_PLACEHOLDER').replace('<br/>', 'NEWLINE_PLACEHOLDER')
    
    soup = BeautifulSoup(html, 'html.parser')
    logger.info(f"Extracting details for: {name} (URL: {url})")
    
    # Extract the numeric agency ID from links in the page
    agency_id = ""
    links = soup.find_all('a', href=True)
    for link in links:
        href = link['href']
        # Check for links with agency/{numeric_id} pattern
        agency_match = re.search(r'/agency/(\d+)', href)
        if agency_match:
            agency_id = agency_match.group(1)
            logger.info(f"Found agency ID: {agency_id} for '{name}'")
            break
    
    # If we didn't find an agency ID, use the URL path as a fallback
    if not agency_id:
        agency_id = url.split('/')[-1]
        logger.warning(f"Could not find agency ID in HTML, using URL path as fallback: {agency_id}")
    
    # Count path components to determine if it's a department
    path_components = [p for p in path.split("/") if p]
    stereotype = "DEPARTEMENT" if len(path_components) == 1 else "DA"
    
    # Initialize details dictionary
    details = {
        "ID": agency_id,
        "PATH": path,
        "Bezeichnung": name,
        "Titel": "",  # Leave empty as per instructions
        "Beschreibung": "",  # Leave empty as per instructions
        "Schlagworte": "",  # Leave empty as per instructions
        "Sammlungstyp": stereotype,
        "Standortadresse": "",
        "Standort Postleitzahl und Ort": "",
        "Postadresse": "",
        "Postleitzahl und Ort": "",
        "Telefon": "",
        "Weitere Telefonnummer oder Fax": "",
        "E-Mail": "",
        "Webseite": "",
        "Öffnungszeiten": "",
        "Portrait": "",
        "Quelle": url
    }
    
    # Helper function to process text with newlines and convert links to markdown
    def process_text_with_links(element):
        if not element:
            return ""
        
        # Create a copy to avoid modifying the original
        element_copy = BeautifulSoup(str(element), 'html.parser')
        
        # Find all links and convert them to markdown
        for a_tag in element_copy.find_all('a', href=True):
            href = a_tag['href']
            text = a_tag.get_text().strip()
            
            # Skip empty links
            if not text:
                continue
                
            # Clean href for tel and mailto links
            if href.startswith('tel:'):
                href = href.replace('tel:', '').strip()
            elif href.startswith('mailto:'):
                href = href.replace('mailto:', '').strip()
                
            # Create markdown link
            markdown_link = f"[{text}]({href})"
            
            # Replace the a tag with the markdown link
            a_tag.replace_with(markdown_link)
        
        # Get the processed text
        processed_text = element_copy.get_text().strip()
        return processed_text.replace('NEWLINE_PLACEHOLDER', '\n')
    
    # Try different methods to extract contact details
    
    # Method 1: Look for agency-card with dl element
    agency_card = soup.find('div', class_='agency-card agency-single-item')
    if agency_card:
        logger.debug("Found agency-card div")
        
        # Extract Portrait if available (before dl elements)
        portrait_h3 = agency_card.find('h3', string='Portrait')
        if portrait_h3:
            portrait_div = portrait_h3.find_next('div')
            if portrait_div:
                # Process the portrait text with links converted to markdown
                details["Portrait"] = process_text_with_links(portrait_div)
                logger.debug(f"Found Portrait: {details['Portrait'][:100]}...")
        
        # Find all dl elements in the agency card
        dl_elements = agency_card.find_all('dl')
        
        for dl_element in dl_elements:
            # Extract all dt and dd pairs
            dt_elements = dl_element.find_all('dt')
            dd_elements = dl_element.find_all('dd')
            
            # Process each dt/dd pair
            for i in range(min(len(dt_elements), len(dd_elements))):
                dt_text = dt_elements[i].text.strip()
                dd_content = dd_elements[i]
                
                extract_field_value(details, dt_text, dd_content)
    
    # Method 2: Try looking for separate dl elements
    if not any(details[field] for field in ["Standortadresse", "Telefon", "E-Mail", "Webseite"]):
        dl_elements = soup.find_all('dl')
        for dl in dl_elements:
            dt_elements = dl.find_all('dt')
            dd_elements = dl.find_all('dd')
            
            for i in range(min(len(dt_elements), len(dd_elements))):
                dt_text = dt_elements[i].text.strip()
                dd_content = dd_elements[i]
                
                extract_field_value(details, dt_text, dd_content)
    
    # Log the extracted details
    field_info = []
    for field, value in details.items():
        if value and field not in ["ID", "PATH", "Bezeichnung", "Titel", "Beschreibung", "Schlagworte", "Sammlungstyp"]:
            field_info.append(f"{field}: {value}")
    
    if field_info:
        logger.info(f"Extracted details for {name}: {', '.join(field_info)}")
    else:
        logger.warning(f"No detail fields extracted for {name}")
    
    return details

def extract_field_value(details, field_name, dd_content):
    """Extract the value from a dd element based on the field name."""
    logger.debug(f"Extracting field: {field_name}")
    
    # Helper function to process text with newlines
    def process_newlines(text):
        return text.replace('NEWLINE_PLACEHOLDER', '\n').strip()
    
    # Helper function to convert links to markdown format
    def convert_links_to_markdown(element):
        if not element:
            return ""
        
        # Create a copy to avoid modifying the original
        element_copy = BeautifulSoup(str(element), 'html.parser')
        
        # Find all links and convert them to markdown
        for a_tag in element_copy.find_all('a', href=True):
            href = a_tag['href']
            text = a_tag.get_text().strip()
            
            # Skip empty links
            if not text:
                continue
                
            # Clean href for tel and mailto links
            if href.startswith('tel:'):
                href = href.replace('tel:', '').strip()
            elif href.startswith('mailto:'):
                href = href.replace('mailto:', '').strip()
                
            # Create markdown link
            markdown_link = f"[{text}]({href})"
            
            # Replace the a tag with the markdown link
            a_tag.replace_with(markdown_link)
        
        # Get the processed text
        processed_text = element_copy.get_text().strip()
        return process_newlines(processed_text)
    
    if field_name == "Standortadresse":
        # Extract paragraphs or plain text
        p_tag = dd_content.find('p')
        if p_tag:
            details["Standortadresse"] = convert_links_to_markdown(p_tag)
        else:
            details["Standortadresse"] = convert_links_to_markdown(dd_content)
        logger.debug(f"Found Standortadresse: {details['Standortadresse']}")
        
    elif field_name == "Standort Postleitzahl und Ort":
        details["Standort Postleitzahl und Ort"] = dd_content.text.strip()
        logger.debug(f"Found Standort Postleitzahl und Ort: {details['Standort Postleitzahl und Ort']}")
        
    elif field_name == "Postadresse":
        p_tag = dd_content.find('p')
        if p_tag:
            details["Postadresse"] = convert_links_to_markdown(p_tag)
        else:
            details["Postadresse"] = convert_links_to_markdown(dd_content)
        logger.debug(f"Found Postadresse: {details['Postadresse']}")
        
    elif field_name == "Postleitzahl und Ort":
        details["Postleitzahl und Ort"] = dd_content.text.strip()
        logger.debug(f"Found Postleitzahl und Ort: {details['Postleitzahl und Ort']}")
        
    elif field_name == "Telefon":
        a_tag = dd_content.find('a')
        if a_tag and a_tag.has_attr('href') and 'tel:' in a_tag['href']:
            # Format as markdown link
            phone_number = a_tag.text.strip()
            phone_link = a_tag['href'].replace('tel:', '').strip()
            details["Telefon"] = f"[{phone_number}]({phone_link})"
        else:
            details["Telefon"] = dd_content.text.strip()
        logger.debug(f"Found Telefon: {details['Telefon']}")
        
    elif field_name == "Weitere Telefonnummer oder Fax":
        details["Weitere Telefonnummer oder Fax"] = convert_links_to_markdown(dd_content)
        logger.debug(f"Found Weitere Telefonnummer oder Fax: {details['Weitere Telefonnummer oder Fax']}")
        
    elif field_name == "E-Mail":
        a_tag = dd_content.find('a')
        if a_tag and a_tag.has_attr('href') and 'mailto:' in a_tag['href']:
            # Format as markdown link
            email = a_tag.text.strip()
            email_link = a_tag['href'].replace('mailto:', '').strip()
            details["E-Mail"] = f"[{email}]({email_link})"
        else:
            details["E-Mail"] = dd_content.text.strip()
        logger.debug(f"Found E-Mail: {details['E-Mail']}")
        
    elif field_name == "Webseite":
        a_tag = dd_content.find('a')
        if a_tag and a_tag.has_attr('href'):
            # Format as markdown link
            website_text = a_tag.text.strip() or a_tag['href'].strip()
            website_link = a_tag['href'].strip()
            details["Webseite"] = f"[{website_text}]({website_link})"
        else:
            details["Webseite"] = dd_content.text.strip()
        logger.debug(f"Found Webseite: {details['Webseite']}")
        
    elif field_name == "Öffnungszeiten":
        p_tag = dd_content.find('p')
        if p_tag:
            details["Öffnungszeiten"] = convert_links_to_markdown(p_tag)
        else:
            details["Öffnungszeiten"] = convert_links_to_markdown(dd_content)
        logger.debug(f"Found Öffnungszeiten: {details['Öffnungszeiten']}")

def find_unterorganisationen(soup):
    """Find all 'Unterorganisationen' links from the HTML."""
    unterorg_links = []
    
    # Find the "Unterorganisationen" heading
    h3_elements = soup.find_all('h3')
    for h3 in h3_elements:
        if 'Unterorganisationen' in h3.text:
            # Find the following ul with class "children"
            ul_element = h3.find_next('ul', class_='children')
            if ul_element:
                # Extract all links from the list
                links = ul_element.find_all('a')
                unterorg_links = [(link.text.strip(), link['href']) for link in links]
                logger.info(f"Found {len(unterorg_links)} suborganizations: {', '.join([name for name, _ in unterorg_links])}")
            break
    
    return unterorg_links

def find_people(soup):
    """Find all people listed in the organization page."""
    people_list = []
    
    # Find the "Personen" heading
    h3_elements = soup.find_all('h3')
    for h3 in h3_elements:
        if 'Personen' in h3.text:
            # Find the following ul with class "memberships"
            ul_element = h3.find_next('ul', class_='memberships')
            if ul_element:
                # Extract all li elements
                list_items = ul_element.find_all('li')
                for li in list_items:
                    person_info = {}
                    
                    # Extract person name
                    strong_tag = li.find('strong')
                    if strong_tag:
                        name_link = strong_tag.find('a')
                        if name_link:
                            person_info['name'] = name_link.text.strip()
                            person_info['link'] = name_link['href']
                    
                    # Extract role/position - it's in the format: <strong><a>Name</a></strong>, <a>Role</a>
                    # Get all a tags in this li
                    a_tags = li.find_all('a')
                    if len(a_tags) >= 2:
                        # The second a tag should be the role
                        person_info['role'] = a_tags[1].text.strip()
                    
                    if person_info:
                        people_list.append(person_info)
                        logger.debug(f"Found person: {person_info.get('name', 'Unknown')} - {person_info.get('role', 'Unknown role')}")
    
    if people_list:
        logger.info(f"Found {len(people_list)} people in this organization")
    
    return people_list

# TODO: Rename. I don't want to mention non-recursive anywhere.
def scrape_organization_non_recursive(start_url, max_entries=None):
    """
    Scrape organizations starting from a URL, but without recursively traversing the hierarchy.
    Instead, collect links to suborganizations for later processing.
    
    Args:
        start_url: The URL to start scraping from
        max_entries: Maximum number of entries to process (None for unlimited)
    
    Returns:
        A tuple of (list of organization details, list of suborganization links)
    """
    logger.info(f"Starting non-recursive organization scraping from: {start_url}")
    
    organizations = []
    to_process = [(start_url, "Regierung und Verwaltung", "")]  # (url, name, path)
    processed_urls = set()
    suborganization_links = []
    
    while to_process and (max_entries is None or len(organizations) < max_entries):
        url, org_name, path = to_process.pop(0)
        
        # Skip if already processed
        if url in processed_urls:
            logger.info(f"Already processed URL: {url}, skipping")
            continue
            
        processed_urls.add(url)
        logger.info(f"Processing organization: {org_name} at {url}")
        
        # Get the page content
        html = get_page(url)
        if not html:
            continue
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Skip the root level for adding to organizations list
        if org_name != "Regierung und Verwaltung":
            # Set the current organization's path
            current_path = path + "/" + org_name if path else org_name
            
            # Extract and add this organization's details
            details = extract_organization_details(url, org_name, current_path)
            if details:
                organizations.append(details)
                logger.info(f"Added organization {len(organizations)}/{max_entries if max_entries else 'unlimited'}: {org_name} (Path: {current_path})")
                
                # Check fields that should be filled
                missing_fields = []
                for field in ["Standortadresse", "Standort Postleitzahl und Ort", "Telefon", "E-Mail", "Webseite"]:
                    if not details.get(field):
                        missing_fields.append(field)
                
                if missing_fields:
                    logger.warning(f"Organization {org_name} is missing fields: {', '.join(missing_fields)}")
        else:
            # For the root level, don't include it in the path
            current_path = ""
        
        # Find all suborganizations
        suborg_links = find_unterorganisationen(soup)
        
        for suborg_name, suborg_href in suborg_links:
            suborg_url = urljoin(url, suborg_href)
            
            # Only add to processing queue if we haven't reached max_entries
            if max_entries is None or len(organizations) + len(to_process) < max_entries:
                to_process.append((suborg_url, suborg_name, current_path))
            
            # Always add to the list of suborganization links for later reference
            suborganization_links.append({
                "name": suborg_name,
                "parent_url": url,
                "parent_name": org_name,
                "url": suborg_url
            })
    
    return organizations, suborganization_links

def scrape_single_organization(url):
    """
    Scrape a single organization page and extract all relevant information.
    
    Args:
        url: The URL of the organization page to scrape
        
    Returns:
        A dictionary with the organization's details and people
    """
    logger.info(f"Scraping single organization at URL: {url}")
    
    # Get the page content
    html = get_page(url)
    if not html:
        logger.error(f"Failed to fetch page at {url}")
        return None
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extract the organization name from the title
    org_name = soup.find('h1', class_='main-title').text.strip() if soup.find('h1', class_='main-title') else "Unknown Organization"
    
    # Extract the path from the URL
    path_parts = url.split('/organization/')[-1].split('/')
    path = '/'.join(path_parts)
    
    # Extract organization details
    details = extract_organization_details(url, org_name, path)
    if not details:
        logger.error(f"Failed to extract details for organization at {url}")
        return None
    
    # Find people listed in the organization
    people = find_people(soup)
    
    # Find suborganizations
    suborganizations = find_unterorganisationen(soup)
    
    # Create a comprehensive data structure
    organization_data = {
        "details": details,
        "people": people,
        "suborganizations": [{
            "name": name,
            "url": urljoin(url, link)
        } for name, link in suborganizations]
    }
    
    # Print a summary
    logger.info(f"Organization: {org_name}")
    logger.info(f"Found {len(people)} people and {len(suborganizations)} suborganizations")
    
    return organization_data

def save_to_csv(data, filename):
    """Save the given data to a CSV file."""
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(data)
        logger.info(f"Data saved successfully to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        traceback.print_exc()
        return False

def save_to_json(data, filename):
    """Save the given data to a JSON file."""
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2, ensure_ascii=False)
        logger.info(f"Data saved successfully to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving to JSON: {e}")
        traceback.print_exc()
        return False

def main(url=None):
    """
    Main function to scrape the Staatskalender website.
    
    Args:
        url: Optional URL to scrape a single organization. If None, the full hierarchy will be scraped.
    """
    if url:
        # Scrape a single organization page
        logger.info(f"Starting single organization scraping for: {url}")
        
        try:
            # Scrape the single organization
            organization_data = scrape_single_organization(url)
            
            if organization_data:
                # Save the organization details to CSV
                if organization_data["details"]:
                    details_file = os.path.join(TMP_DIR, "organization_details.csv")
                    if save_to_csv([organization_data["details"]], details_file):
                        logger.info(f"Organization details saved to {details_file}")
                    else:
                        logger.error(f"Failed to save organization details to {details_file}")
                
                # Save the complete data to JSON for easier inspection
                json_file = os.path.join(TMP_DIR, "organization_data.json")
                if save_to_json(organization_data, json_file):
                    logger.info(f"All organization data saved to {json_file}")
                else:
                    logger.error(f"Failed to save organization data to {json_file}")
                
                # Print a summary of the data
                print("\n=== Organization Information ===")
                print(f"Name: {organization_data['details']['Bezeichnung']} ({url})")
                print(f"Address: {organization_data['details']['Standortadresse']}, {organization_data['details']['Standort Postleitzahl und Ort']}")
                
                if organization_data['people']:
                    print(f"\nPeople ({len(organization_data['people'])}):")
                    for person in organization_data['people']:
                        print(f"- {person.get('name', 'Unknown')} - {person.get('role', 'Unknown role')}")
                
                if organization_data['suborganizations']:
                    print(f"\nSuborganizations ({len(organization_data['suborganizations'])}):")
                    for suborg in organization_data['suborganizations']:
                        print(f"- {suborg['name']} ({suborg['url']})")
            else:
                logger.error("Failed to scrape organization data")
            
            logger.info("Scraping process completed")
            
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            traceback.print_exc()
    else:
        # Scrape the organization hierarchy non-recursively
        logger.info(f"Starting Staatskalender non-recursive scraping (limited to {MAX_ENTRIES} entries)")
        
        try:
            # Scrape organizations non-recursively
            organizations, suborganization_links = scrape_organization_non_recursive(BASE_URL, MAX_ENTRIES)
            logger.info(f"Found {len(organizations)} organizations and {len(suborganization_links)} suborganization links")
            
            # Map organization names to their URLs
            org_urls = {}
            for org in organizations:
                org_name = org["Bezeichnung"]
                # Extract the URL from suborganization_links or build it from PATH
                url_found = False
                for link in suborganization_links:
                    if link["name"] == org_name:
                        org_urls[org_name] = link["url"]
                        url_found = True
                        break
                if not url_found:
                    # Build URL from BASE_URL and PATH
                    path_components = org["PATH"].split("/")
                    url = BASE_URL
                    if path_components:
                        for component in path_components:
                            if component:
                                url = f"{url}/{component}"
                    org_urls[org_name] = url
            
            # Save organizations to CSV
            if save_to_csv(organizations, OUTPUT_FILE):
                logger.info(f"Organization data saved to {OUTPUT_FILE}")
            else:
                logger.error(f"Failed to save organization data to {OUTPUT_FILE}")
            
            # Save suborganization links to JSON for later processing
            sublinks_file = os.path.join(TMP_DIR, "suborganization_links.json")
            if save_to_json(suborganization_links, sublinks_file):
                logger.info(f"Suborganization links saved to {sublinks_file}")
            else:
                logger.error(f"Failed to save suborganization links to {sublinks_file}")
            
            # Analyze missing fields by organization
            missing_fields_by_org = {}
            for org in organizations:
                org_name = org["Bezeichnung"]
                missing = []
                for field in FIELDS:
                    if field not in ["ID", "PATH", "Bezeichnung", "Titel", "Beschreibung", "Schlagworte", "Sammlungstyp", "Weitere Telefonnummer oder Fax"]:
                        if not org.get(field):
                            missing.append(field)
                if missing:
                    missing_fields_by_org[org_name] = missing
            
            # Create a detailed report of missing fields
            missing_fields_file = os.path.join(TMP_DIR, "missing_fields_report.txt")
            try:
                # Ensure the directory exists
                os.makedirs(os.path.dirname(os.path.abspath(missing_fields_file)), exist_ok=True)
                
                with open(missing_fields_file, "w", encoding="utf-8") as f:
                    f.write("=== MISSING FIELDS REPORT ===\n\n")
                    if missing_fields_by_org:
                        f.write("Organizations with missing fields:\n\n")
                        for org_name, fields in missing_fields_by_org.items():
                            url = org_urls.get(org_name, "Unknown URL")
                            f.write(f"* {org_name} ({url}):\n")
                            for field in fields:
                                f.write(f"  - {field}\n")
                            f.write("\n")
                    else:
                        f.write("All organizations have complete data.\n")
                logger.info(f"Missing fields report saved to {missing_fields_file}")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")
                traceback.print_exc()
            
            # Create a report of missing fields by field type, excluding "Weitere Telefonnummer oder Fax"
            missing_fields_by_type = {}
            for field in FIELDS:
                if field not in ["ID", "PATH", "Bezeichnung", "Titel", "Beschreibung", "Schlagworte", "Sammlungstyp", "Weitere Telefonnummer oder Fax"]:
                    orgs_missing_field = []
                    for org in organizations:
                        if not org.get(field):
                            orgs_missing_field.append(org["Bezeichnung"])
                    if orgs_missing_field:
                        missing_fields_by_type[field] = orgs_missing_field
            
            # Print a summary of the data
            print(f"\n=== Scraping Summary ===")
            print(f"Processed {len(organizations)} organizations")
            print(f"Collected {len(suborganization_links)} suborganization links for future processing")
            print(f"All output files saved to {TMP_DIR}/ directory")
            
            # Report on missing fields
            print("\nField completion status:")
            for field, orgs in missing_fields_by_type.items():
                print(f"- {field}: Missing in {len(orgs)}/{len(organizations)} organizations")
                print(f"   - {'\n   - '.join([f'{org} ({org_urls.get(org, 'Unknown URL')})' for org in orgs])}")
            
            logger.info("Scraping process completed")
            
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    # Call main without a URL to use the non-recursive scraping
    main(None)
