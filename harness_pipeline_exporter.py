# Harness Pipeline Exporter
# This script fetches pipeline data from a Harness account and exports it to a CSV file.
#
# Prerequisites:
# 1. Python 3.6+
# 2. `requests` library: Install using `pip install requests`
#
# How to Run:
# 1. Save this script as `harness_pipeline_exporter.py`.
# 2. Set Environment Variables (Recommended for API Token & Account ID):
#    export HARNESS_API_TOKEN="your_harness_bearer_token_here" # e.g., a Personal Access Token (PAT)
#    export HARNESS_ACCOUNT_ID="your_harness_account_id_here"
#    (Optional) export HARNESS_API_BASE_URL="your_on_prem_harness_url" # If not using SaaS
# 3. Run the script: `python harness_pipeline_exporter.py`
# 4. If environment variables are not set, you will be prompted to enter them.
# 5. A CSV file named `harness_pipelines_report.csv` will be generated,
#    containing columns: Organization, Project, Pipeline Count.

import requests
import csv
import os
import getpass

# --- Configuration ---
# HARNESS_API_BASE_URL = "https://app.harness.io" # For SaaS
# For on-premise, replace with your Harness base URL
HARNESS_API_BASE_URL = os.environ.get("HARNESS_API_BASE_URL", "https://app.harness.io")
HARNESS_ACCOUNT_ID_ENV_VAR = "HARNESS_ACCOUNT_ID"
HARNESS_API_TOKEN_ENV_VAR = "HARNESS_API_TOKEN" # Recommended for security
CSV_FILENAME = "harness_pipelines_report.csv"


# --- Helper Functions ---

def get_harness_account_id():
    """
    Retrieves the Harness Account ID.
    Priority:
    1. Environment variable (HARNESS_ACCOUNT_ID)
    2. User prompt
    """
    account_id = os.environ.get(HARNESS_ACCOUNT_ID_ENV_VAR)
    if not account_id:
        account_id = input("Enter your Harness Account ID: ").strip()
    if not account_id:
        print("Error: Harness Account ID is required.")
        exit(1)
    return account_id


def get_harness_api_token():
    """
    Retrieves the Harness API Token.
    Priority:
    1. Environment variable (HARNESS_API_TOKEN)
    2. User prompt (securely using getpass)
    """
    api_token = os.environ.get(HARNESS_API_TOKEN_ENV_VAR)
    if not api_token:
        print("Harness API Token not found in environment variables.")
        api_token = getpass.getpass("Enter your Harness API Token (PAT): ").strip()
    if not api_token:
        print("Error: Harness API Token is required.")
        exit(1)
    return api_token


def make_api_request(url, headers, params=None):
    """
    Makes a GET request to the Harness API and handles common errors.
    Returns the JSON response or None if an error occurs.
    """
    if params is None:
        params = {}
    try:
        # print(f"DEBUG: Requesting URL: {url} with params: {params}")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - {response.text}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected error occurred: {req_err}")
    return None


def fetch_all_with_pagination(url, headers, account_id, key_name, page_size=100):
    """
    Fetches all items from a paginated Harness API endpoint.
    `key_name` is the key in the response content that holds the list of items.
    """
    all_items = []
    page_index = 0
    total_pages = 1 # Initial assumption, will be updated by API response

    while page_index < total_pages:
        params = {
            "accountIdentifier": account_id,
            "page": page_index,
            "size": page_size
        }
        # Some endpoints might use slightly different pagination params, adjust if needed
        # print(f"DEBUG: Fetching page {page_index} for {key_name} from {url}")
        data = make_api_request(url, headers, params)

        if data and "data" in data and key_name in data["data"]:
            items_on_page = data["data"][key_name]
            if not items_on_page and page_index > 0: # No items on a subsequent page means we're done
                # print(f"DEBUG: No more items for {key_name} on page {page_index}.")
                break
            all_items.extend(items_on_page)

            # Harness NG API typically includes pagination info like this:
            total_pages = data.get("data", {}).get("totalPages", total_pages)
            # print(f"DEBUG: {key_name} - Page {page_index+1}/{total_pages}. Items on page: {len(items_on_page)}. Total items so far: {len(all_items)}")

            if not items_on_page and total_pages == 0 and page_index == 0: # No items at all
                 break
            if len(items_on_page) < page_size: # Optimization: if less items than page size, it's the last page
                break

        elif data and isinstance(data.get("data"), list) and key_name == "data_list_direct": # For APIs that return list directly in data
            items_on_page = data["data"]
            if not items_on_page and page_index > 0:
                break
            all_items.extend(items_on_page)
            total_pages = data.get("totalPages", total_pages) # NG API might use this structure for some list APIs
            if not items_on_page and total_pages == 0 and page_index == 0:
                 break
            if len(items_on_page) < page_size:
                break
        else:
            print(f"Warning: Could not fetch {key_name} or unexpected response structure from {url} on page {page_index}.")
            # print(f"DEBUG: Data received: {data}")
            break # Stop if there's an issue or data is not in expected format

        page_index += 1
        if page_index >= total_pages: # Ensure we don't loop if total_pages was correctly set from response
            break

    return all_items


def get_organizations(api_base_url, headers, account_id):
    """ Fetches all organizations for the given account. """
    # API: GET /ng/api/organizations?accountIdentifier={{accountIdentifier}}
    # Note: This API might not use 'content' as the key, but rather the list is directly in 'data' or similar.
    # Adjusting `key_name` based on typical NG API list responses.
    # The organizations list is often directly under a "data" key, and might not be nested further.
    # Let's assume the list of orgs is directly in response.data or response.data.content
    # Based on typical Harness API: /ng/api/organizations?accountIdentifier=<accountId>
    # The response is usually: { status: "SUCCESS", data: { content: [ {organization}, ... ] } } or similar
    # Or sometimes { status: "SUCCESS", data: [ {organization}, ... ] }

    # Let's try a common pattern first, assuming 'content' holds the list.
    # If this API is different, we might need to adjust `key_name` or the parsing in `fetch_all_with_pagination`.
    url = f"{api_base_url}/ng/api/organizations"
    print(f"Fetching organizations for account: {account_id}...")
    # The organizations endpoint returns a list of organizations directly in the 'data' field
    # and it's usually not nested under a 'content' key like other list APIs.
    # It also doesn't seem to follow the standard page/size pagination in all environments/versions.
    # Let's try a direct request first and then adapt.

    params = {"accountIdentifier": account_id}
    response_data = make_api_request(url, headers, params)

    if response_data and "data" in response_data and isinstance(response_data["data"], list):
        orgs = response_data["data"]
        print(f"Successfully fetched {len(orgs)} organizations.")
        return orgs
    elif response_data and "data" in response_data and "content" in response_data["data"] and isinstance(response_data["data"]["content"], list):
        # Fallback for a more common paginated structure if the above direct list isn't found
        orgs = response_data["data"]["content"]
        # This would ideally use fetch_all_with_pagination if it were paginated with 'content'
        print(f"Successfully fetched {len(orgs)} organizations (from data.content).")
        return orgs
    else:
        print(f"Could not fetch organizations or response format is unexpected. Response: {response_data}")
        return []


def get_projects(api_base_url, headers, account_id, org_identifier):
    """ Fetches all projects for a given organization. """
    # API: GET /ng/api/projects?accountIdentifier={{accountIdentifier}}&orgIdentifier={{orgIdentifier}}
    # This API typically returns data in { data: { content: [ ... ] } }
    url = f"{api_base_url}/ng/api/projects"
    print(f"Fetching projects for organization: {org_identifier}...")

    # Add orgIdentifier to params for fetch_all_with_pagination
    # The generic fetch_all_with_pagination adds accountId, page, size. We need to pass others.
    # Modifying fetch_all_with_pagination to accept arbitrary additional_params might be cleaner,
    # but for now, we'll handle it by constructing the URL with orgIdentifier for project fetching.
    # Actually, the standard project list API uses query parameters.

    all_projects = []
    page_index = 0
    total_pages = 1
    page_size = 100

    while page_index < total_pages:
        params = {
            "accountIdentifier": account_id,
            "orgIdentifier": org_identifier,
            "page": page_index,
            "size": page_size
        }
        # print(f"DEBUG: Fetching projects page {page_index} for org {org_identifier}")
        response_data = make_api_request(url, headers, params)

        if response_data and "data" in response_data and "content" in response_data["data"]:
            projects_on_page = response_data["data"]["content"]
            if not projects_on_page and page_index > 0:
                break
            all_projects.extend(projects_on_page)

            total_pages = response_data.get("data", {}).get("totalPages", total_pages)
            # print(f"DEBUG: Projects - Page {page_index+1}/{total_pages}. Items: {len(projects_on_page)}. Total: {len(all_projects)}")

            if not projects_on_page and total_pages == 0 and page_index == 0:
                 break
            if len(projects_on_page) < page_size:
                break
        else:
            print(f"Warning: Could not fetch projects or unexpected response structure for org {org_identifier} on page {page_index}.")
            # print(f"DEBUG: Project data received: {response_data}")
            break

        page_index += 1
        if page_index >= total_pages:
            break

    if all_projects:
        print(f"Successfully fetched {len(all_projects)} projects for organization: {org_identifier}.")
    return all_projects


def get_pipelines(api_base_url, headers, account_id, org_identifier, project_identifier):
    """ Fetches all pipelines for a given project within an organization. """
    # API: GET /ng/api/pipelines?accountIdentifier={{accountIdentifier}}&orgIdentifier={{orgIdentifier}}&projectIdentifier={{projectIdentifier}}
    # This API also typically returns data in { data: { content: [ ... ] } } structure for pipelines
    url = f"{api_base_url}/ng/api/pipelines"
    print(f"Fetching pipelines for project: {project_identifier} in org: {org_identifier}...")

    all_pipelines = []
    page_index = 0
    total_pages = 1
    page_size = 100 # Adjust if needed, Harness default is often 50 or 100

    while page_index < total_pages:
        params = {
            "accountIdentifier": account_id,
            "orgIdentifier": org_identifier,
            "projectIdentifier": project_identifier,
            "page": page_index,
            "size": page_size
        }
        # print(f"DEBUG: Fetching pipelines page {page_index} for project {project_identifier}")
        response_data = make_api_request(url, headers, params)

        if response_data and "data" in response_data and "content" in response_data["data"]:
            pipelines_on_page = response_data["data"]["content"]
            if not pipelines_on_page and page_index > 0: # No pipelines on a subsequent page
                break
            all_pipelines.extend(pipelines_on_page)

            total_pages = response_data.get("data", {}).get("totalPages", total_pages)
            # print(f"DEBUG: Pipelines - Page {page_index+1}/{total_pages}. Items: {len(pipelines_on_page)}. Total: {len(all_pipelines)}")

            if not pipelines_on_page and total_pages == 0 and page_index == 0: # No pipelines at all
                 break
            if len(pipelines_on_page) < page_size: # Optimization: if less items than page size, it's the last page
                break
        else:
            print(f"Warning: Could not fetch pipelines or unexpected response structure for project {project_identifier} on page {page_index}.")
            # print(f"DEBUG: Pipeline data received: {response_data}")
            break # Stop if there's an issue

        page_index += 1
        if page_index >= total_pages: # Ensure we don't loop if total_pages was correctly set from response
            break

    if all_pipelines:
        print(f"Successfully fetched {len(all_pipelines)} pipelines for project: {project_identifier}.")
    return all_pipelines


def get_pipeline_count(api_base_url, headers, account_id, org_identifier, project_identifier):
    """
    Fetches the count of pipelines for a given project within an organization.
    It attempts to do this by making a minimal request and checking for total item count
    in the response metadata.
    """
    url = f"{api_base_url}/ng/api/pipelines"
    print(f"Fetching pipeline count for project: {project_identifier} in org: {org_identifier}...")

    params = {
        "accountIdentifier": account_id,
        "orgIdentifier": org_identifier,
        "projectIdentifier": project_identifier,
        "page": 0,  # First page
        "size": 1   # Minimal size, we only need metadata
    }

    response_data = make_api_request(url, headers, params)

    if response_data and "data" in response_data:
        # Harness NG APIs typically provide total item count in 'totalItems' or 'totalElements'
        # within the 'data' object when pagination is involved.
        if "totalItems" in response_data["data"]:
            count = response_data["data"]["totalItems"]
            print(f"Successfully fetched pipeline count: {count} for project: {project_identifier}.")
            return count
        elif "totalElements" in response_data["data"]: # Another common key for total count
            count = response_data["data"]["totalElements"]
            print(f"Successfully fetched pipeline count: {count} for project: {project_identifier}.")
            return count
        elif "content" in response_data["data"] and isinstance(response_data["data"]["content"], list):
            # Fallback: if totalItems isn't there, but we got a content list (even if small due to size=1)
            # and if there's no totalPages or a very small one, this might be a small project.
            # For a more accurate count if metadata is missing, we'd have to paginate fully.
            # However, for this optimization, we'll check if it's the only page.
            total_pages = response_data.get("data", {}).get("totalPages", 0)
            if total_pages <= 1: # If totalPages is 0 or 1, the current page content length is the count
                count = len(response_data["data"]["content"])
                print(f"Fetched pipeline count (fallback using content length on single page): {count} for project: {project_identifier}.")
                return count
            else:
                # If totalPages > 1 and no totalItems/totalElements, we'd have to iterate all pages.
                # For this function's purpose (optimized count), we'll indicate this requires full fetch.
                print(f"Warning: Pipeline count metadata (totalItems/totalElements) not found for project {project_identifier}, and multiple pages exist. Full pagination would be needed for exact count. Returning -1 (or consider fetching all).")
                # To get an exact count in this scenario, you would call the original get_pipelines and get its length.
                # For simplicity in this modification, we'll return a marker or could call get_pipelines.
                # Let's call the original get_pipelines and return its length as a robust fallback.
                print(f"Performing full fetch for project {project_identifier} to get accurate count...")
                all_pipelines_list = get_pipelines(api_base_url, headers, account_id, org_identifier, project_identifier) # Call original
                return len(all_pipelines_list)

    print(f"Warning: Could not determine pipeline count for project {project_identifier}. Response: {response_data}")
    return 0 # Default to 0 if count cannot be determined


# --- Main Script Logic ---
def main():
    """ Main function to orchestrate fetching and writing pipeline data. """
    print("Starting Harness pipeline export process...")

    account_id = get_harness_account_id()
    api_token = get_harness_api_token()

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Harness-Account": account_id, # Some NG APIs require this header
        "Content-Type": "application/json"
    }

    all_pipeline_data = []
    processed_org_count = 0
    processed_project_count = 0
    processed_pipeline_count = 0

    organizations = get_organizations(HARNESS_API_BASE_URL, headers, account_id)

    if not organizations:
        print("No organizations found or failed to fetch organizations. Exiting.")
        return

    print(f"\nFound {len(organizations)} organizations. Processing each...")

    for org_data in organizations:
        org_identifier = org_data.get("organization", {}).get("identifier")
        org_name = org_data.get("organization", {}).get("name", org_identifier) # Fallback to ID if name is missing
        if not org_identifier:
            print(f"Warning: Skipping organization with missing identifier. Data: {org_data}")
            continue

        processed_org_count += 1
        print(f"\nProcessing Organization {processed_org_count}/{len(organizations)}: {org_name} (ID: {org_identifier})")

        projects = get_projects(HARNESS_API_BASE_URL, headers, account_id, org_identifier)
        if not projects:
            print(f"No projects found in organization: {org_name}. Skipping.")
            continue

        for project_data in projects:
            project_identifier = project_data.get("project", {}).get("identifier")
            project_name = project_data.get("project", {}).get("name", project_identifier) # Fallback
            if not project_identifier:
                print(f"Warning: Skipping project with missing identifier in org {org_name}. Data: {project_data}")
                continue

            processed_project_count +=1
            print(f"  Processing Project: {project_name} (ID: {project_identifier})")

            pipeline_count = get_pipeline_count(HARNESS_API_BASE_URL, headers, account_id, org_identifier, project_identifier)

            # Add data for this project, even if pipeline_count is 0
            all_pipeline_data.append({
                "Project": project_name,
                "Organization": org_name,
                "Pipeline Count": pipeline_count
            })
            processed_pipeline_count += pipeline_count # Accumulate total pipelines for summary

            if pipeline_count == 0:
                print(f"  No pipelines found in project: {project_name}.")
            else:
                print(f"  Found {pipeline_count} pipelines in project: {project_name}.")


    print(f"\n--- Summary ---")
    print(f"Processed Organizations: {processed_org_count}")
    print(f"Processed Projects: {processed_project_count}")
    print(f"Total Pipelines Found across all processed projects: {processed_pipeline_count}") # Clarified summary

    if not all_pipeline_data: # This check might be less relevant if we always add a row per project
        print("\nNo project data collected. CSV file will not be generated.") # Adjusted message
        return

    # This part will be moved to a dedicated function in the next step
    # write_data_to_csv(all_pipeline_data, CSV_FILENAME)

    print(f"\nPipeline data collection complete. {len(all_pipeline_data)} pipelines recorded.")
    # Placeholder for CSV writing call
    print(f"Next step: Write data to {CSV_FILENAME}") # This will be replaced by the actual call
    write_data_to_csv(all_pipeline_data, CSV_FILENAME)


# --- CSV Writing ---
def write_data_to_csv(data, filename):
    """ Writes the collected pipeline data to a CSV file. """
    if not data:
        print("No data to write to CSV.")
        return

    print(f"\nWriting data to CSV file: {filename}...")
    try:
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            # Define fieldnames based on the keys in our data dictionaries
            fieldnames = ["Organization", "Project", "Pipeline Count"] # Updated fieldname
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in data:
                writer.writerow(row)
        print(f"Successfully wrote {len(data)} records to {filename}")
    except IOError as e:
        print(f"Error writing to CSV file {filename}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV writing: {e}")


# Placeholder for where the script execution will start
if __name__ == "__main__":
    main()

# Final print statement to remove after full implementation
# print("Harness pipeline exporter script - CSV writing function added and integrated.")
