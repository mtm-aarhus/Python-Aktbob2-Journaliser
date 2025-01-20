"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import requests
from requests_ntlm import HttpNtlmAuth

from datetime import datetime
import time
import io
import os
import json
import uuid
import xml.etree.ElementTree as ET


# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    sharepoint_site_url = orchestrator_connection.get_constant("AktbobSharePointUrl").value
    go_api_url = orchestrator_connection.get_constant("GOApiURL").value
    go_api_login = orchestrator_connection.get_credential("GOAktApiUser")
    robot_user = orchestrator_connection.get_credential("Robot365User")
    username = robot_user.username
    password = robot_user.password
    go_username = go_api_login.username
    go_password = go_api_login.password
    
    json_queue = json.loads(queue_element.data)
    Overmappenavn = json_queue.get("Overmappenavn")
    Aktindsigtssag = json_queue.get("Aktindsigtssag")

    sharepoint_folders = [
    #f"Delte dokumenter/Dokumentlister/{Overmappenavn}",
    f"Delte dokumenter/Aktindsigter/{Overmappenavn}"
    ]
    session = create_session(go_api_url, go_username, go_password)

    process_sharepoint_folders(sharepoint_site_url, sharepoint_folders, go_api_url, username, password, session, orchestrator_connection, Aktindsigtssag)
    

def sharepoint_client(username: str, password: str, sharepoint_site_url: str, orchestrator_connection: OrchestratorConnection) -> ClientContext:
    """
    Creates and returns a SharePoint client context.
    """
    # Authenticate to SharePoint
    ctx = ClientContext(sharepoint_site_url).with_credentials(UserCredential(username, password))

    # Load and verify connection
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()

    orchestrator_connection.log_info(f"Authenticated successfully. Site Title: {web.properties['Title']}")
    return ctx

def fetch_files_in_folder(ctx: ClientContext, folder_url, base_folder=""):
    files_array = []
    folder = ctx.web.get_folder_by_server_relative_url(folder_url).execute_query()
    files = folder.files.get().execute_query()
    folders = folder.folders.get().execute_query()


    for file in files:
        files_array.append({
            "ServerRelativeUrl": file.serverRelativeUrl,
            "UniqueId": file.unique_id,
            "Name": file.name,
            "FolderPath": base_folder
        })

    for subfolder in folders:
        subfolder_name = os.path.join(base_folder, subfolder.name)
        files_array.extend(fetch_files_in_folder(ctx, subfolder.serverRelativeUrl, subfolder_name))

    return files_array

# Create payload for document upload
def make_payload_document(ows_dict: dict, caseID: str, FolderPath: str, byte_arr: list, filename):
    ows_str = ' '.join([f'ows_{k}="{v}"' for k, v in ows_dict.items()])
    MetaDataXML = f'<z:row xmlns:z="#RowsetSchema" {ows_str}/>'

    return {
        "Bytes": byte_arr,
        "CaseId": caseID,
        "ListName": "Dokumenter",
        "FolderPath": FolderPath.replace("\\","/"),
        "FileName": filename,
        "Metadata": MetaDataXML,
        "Overwrite": True
    }

# Upload document to GO
def upload_document_go(go_api_url, payload, session):
    url = f"{go_api_url}/_goapi/Documents/AddToCase"
    response = session.post(url, data=payload, timeout=1200)
    response.raise_for_status()
    return response.json()

def create_session (APIURL, Username, PasswordString):
    # Create a session
    session = requests.Session()
    session.auth = HttpNtlmAuth(Username, PasswordString)
    session.post(APIURL, timeout=500)
    return session

def print_download_progress(offset, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info("Downloaded '{0}' bytes...".format(offset))

# Process SharePoint folders and upload files
def process_sharepoint_folders(sharepoint_site_url, folders, go_api_url, username, password, session, orchestrator_connection: OrchestratorConnection, case_id):
    ctx = sharepoint_client(username, password, sharepoint_site_url, orchestrator_connection)

    created_folders = set()  # Keep track of created folders
    today_date = datetime.now().strftime("%d-%m-%Y")

    for folder_url in folders:
        orchestrator_connection.log_info(f"Processing top-level folder: {folder_url}")
        files = fetch_files_in_folder(ctx, folder_url)

        # Group files by their subfolder paths
        files_by_subfolder = {}
        for file in files:
            folder_path = file["FolderPath"]
            if folder_path not in files_by_subfolder:
                files_by_subfolder[folder_path] = []
            files_by_subfolder[folder_path].append(file)

        # Process each subfolder separately
        for folder_path, folder_files in files_by_subfolder.items():
            orchestrator_connection.log_info(f"Processing subfolder: {folder_path}")

            folder_doc_ids = []

            for file in folder_files:
                orchestrator_connection.log_info(f"Uploading file: {file['Name']} in {folder_path}")

                # Download the file content
                try:      
                    sp_file = File.open_binary(ctx, file['ServerRelativeUrl'])
                    file_content = sp_file.content
                except:
                    orchestrator_connection.log_info("Downloading file failed, trying large file download from unique id")
                    large_file = ctx.web.get_file_by_id(file['UniqueId'])
                    local_filename = file['Name']
                    
                    # Download large file to local storage
                    with open(local_filename, "wb") as local_file:
                        large_file.download_session(local_file, print_download_progress(1024*1024, orchestrator_connection)).execute_query()
                    
                    # Read the file content from the saved file
                    with open(local_filename, "rb") as local_file:
                        file_content = local_file.read()
                    
                    os.remove(local_filename)
                                
                
                byte_array = list(file_content)

                # Prepare metadata
                ows_dict = {
                    "Title": os.path.splitext(file['Name'])[0],  # Filename without extension
                    "CaseID": case_id,  # Replace with your case ID
                    "Beskrivelse": "Uploadet af Aktbob",  # Add relevant description
                    "Korrespondance": "Udgående",
                    "Dato": today_date,
                    "CCMMustBeOnPostList": "0"
                }

                # Create payload
                payload = make_payload_document(ows_dict, case_id, folder_path, byte_array, file['Name'])

                try:
                    if (len(file_content) / (1024 * 1024)) > 10:
                        raise Exception("File is larger than 10 MB, skipping normal upload to avoid errors")
                    # Attempt upload
                    response = upload_document_go(go_api_url, payload, session)
                    if "DocId" in response:
                        folder_doc_ids.append((response["DocId"], file['Name']))
                        if folder_path not in created_folders:
                            created_folders.add(folder_path)
                    else:
                        raise Exception("No DocId")
                except Exception as e:
                    orchestrator_connection.log_info(f"Failed to upload {file['Name']}: {e}")
                    # Retry with large upload
                    orchestrator_connection.log_info("Retrying with large upload...")
                    if folder_path not in created_folders:
                        orchestrator_connection.log_info(f"Creating folder: {folder_path}")
                        create_and_delete_placeholder(go_api_url, case_id, str(folder_path).replace("\\","/"), session, orchestrator_connection)
                        created_folders.add(folder_path)

                    large_response = upload_large_document(go_api_url, payload, session, file_content, orchestrator_connection)
                    large_response_json = json.loads(large_response)  # Parse the JSON string
                    if "DocId" in large_response_json:
                        folder_doc_ids.append((large_response_json["DocId"], file['Name']))
                    else:
                        raise Exception(f"Failed upload for file: {file['Name']} in {folder_path}")

            # Sort files by ascending order of their filenames
            folder_doc_ids.sort(key=lambda x: x[1])

            # Extract only DocIds after sorting
            doc_ids_for_journalization = [doc_id for doc_id, _ in folder_doc_ids]

            # Journalize the documents for this subfolder
            if doc_ids_for_journalization:
                try:
                    journalize_documents(go_api_url, doc_ids_for_journalization, session, orchestrator_connection)
                    orchestrator_connection.log_info(f"Successfully journalized documents for subfolder {folder_path}: {doc_ids_for_journalization}")
                except Exception as e:
                    orchestrator_connection.log_info(f"Failed to journalize documents for subfolder {folder_path}: {e}")


# Journalize the uploaded documents
def journalize_documents(go_api_url, doc_ids, session: requests.session, orchestrator_connection: OrchestratorConnection):
    try:
        payload = {"DocumentIds": doc_ids}
        url = f"{go_api_url}/_goapi/Documents/MarkMultipleAsCaseRecord/ByDocumentId"
        response = session.post(url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        response.raise_for_status()
        orchestrator_connection.log_info("Documents journalized successfully.")
    except Exception as e:
        orchestrator_connection.log_info(f"Failed to journalize documents: {e}")
        
def delete_document_go(go_api_url, doc_id, session: requests.session):
    url = f"{go_api_url}/_goapi/Documents/ByDocumentId"
    payload = {
        "DocId": doc_id,
        "ForceDelete": True
    }
    response = session.delete(url, json=payload, timeout=1200)
    response.raise_for_status()


def create_and_delete_placeholder(go_api_url, case_id, folder_path, session: requests.session, orchestrator_connection: OrchestratorConnection):
    # Create a small binary file (e.g., "CreateFolder.txt" containing a single letter "A")
    file_content = b"A"
    byte_array = list(file_content)

    # Prepare metadata for the placeholder file
    ows_dict = {
        "Beskrivelse": "Leveret af Aktbob",
        "CCMMustBeOnPostList": "0"
    }
    metadata_xml = ' '.join([f'ows_{k}="{v}"' for k, v in ows_dict.items()])
    metadata = f'<z:row xmlns:z="#RowsetSchema" {metadata_xml}/>'

    # Create the payload for uploading the placeholder file
    payload = {
        "Bytes": byte_array,
        "CaseId": case_id,
        "ListName": "Dokumenter",
        "FolderPath": folder_path,
        "FileName": "CreateFolder.txt",
        "Metadata": metadata,
        "Overwrite": True
    }

    try:
        # Upload the placeholder file
        orchestrator_connection.log_info("Uploading placeholder file...")
        upload_response = upload_document_go(go_api_url, payload, session)
        doc_id = upload_response.get("DocId")
        orchestrator_connection.log_info(f"Placeholder file uploaded with DocId: {doc_id}")

        # Delete the placeholder file
        orchestrator_connection.log_info("Deleting placeholder file...")
        delete_document_go(go_api_url, doc_id, session)

    except Exception as e:
        orchestrator_connection.log_info(f"Error during create/delete placeholder operation: {e}")
        
#Below is for uploading large/failed files
def chunked_file_upload(APIURL, case_url, binary, file_name, session, request_digest, folder_path, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info(f'Folder path: {folder_path}')
    orchestrator_connection.log_info(f'File name: {file_name}')
    chunk_size_bytes = 1024 * 10240
    session.headers.update({
        'X-FORMS_BASED_AUTH_ACCEPTED': 'f',
        'X-RequestDigest': request_digest
    })
    orchestrator_connection.log_info(request_digest)

    web_url = APIURL+"/"+case_url
    if folder_path != None:
        target_folder_url = "/"+case_url+"/Dokumenter/"+folder_path
    else:
        target_folder_url = "/"+case_url + "/Dokumenter"

    target_folder_url = target_folder_url.replace("/", "%2F")


    create_file_request_url = f"{web_url}/_api/web/GetFolderByServerRelativePath(DecodedUrl='{target_folder_url}')/Files/add(url='{file_name}',overwrite=true)"
    response = session.post(create_file_request_url)
    response.raise_for_status()  # Ensure file creation is successful

    target_url = f"{target_folder_url}%2F{file_name}"

    upload_id = str(uuid.uuid4())  # Unique upload session ID
    offset = 0
    total_size = len(binary)

    with io.BytesIO(binary) as input_stream:
        first_chunk = True

        while True:
            buffer = input_stream.read(chunk_size_bytes)
            if not buffer:
                break  # End of file reached

            if first_chunk and len(buffer) == total_size:
                # If the file fits in a single chunk, handle it differently
                # StartUpload and FinishUpload in one step
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl='{target_url}')/startUpload(uploadId=guid'{upload_id}')"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()

                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl='{target_url}')/finishUpload(uploadId=guid'{upload_id}',fileOffset={offset})"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()
                break  # Upload complete
            elif first_chunk:
                # StartUpload: Initiating the upload session for large files
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl='{target_url}')/startUpload(uploadId=guid'{upload_id}')"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()
                first_chunk = False
            elif input_stream.tell() == total_size:
                # FinishUpload: Upload the final chunk for large files
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl='{target_url}')/finishUpload(uploadId=guid'{upload_id}',fileOffset={offset})"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()
            else:
                # ContinueUpload: Upload subsequent chunks
                endpoint_url = f"{web_url}/_api/web/GetFileByServerRelativePath(DecodedUrl='{target_url}')/continueUpload(uploadId=guid'{upload_id}',fileOffset={offset})"
                orchestrator_connection.log_info(endpoint_url)
                response = session.post(endpoint_url, data=buffer)
                response.raise_for_status()

            offset += len(buffer)
            chunk_uploaded(offset, total_size, orchestrator_connection)  # Callback for tracking progress

def request_form_digest(APIURL, case_url, session: requests.session):
    endpoint_url = f"{APIURL}/{case_url}/_api/contextinfo"
    session.headers.update({
        'Accept': 'application/json; odata=verbose'
    })
    response = session.post(endpoint_url)
    response.raise_for_status()
    data = response.json()
    return data['d']['GetContextWebInformation']['FormDigestValue']

def get_docid(file_name, APIURL, case_url, folder_path, session: requests.session, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info(f'Fetching doc_id for {file_name}')

    sags_url = f'{APIURL}/{case_url}/_goapi/Administration/GetLeftMenuCounter'

    # Make the GET request using the session
    response = session.get(sags_url)
    response.raise_for_status()
    data = response.json()

    ViewId = None
    for item in data:
        if item.get("ViewName") == "AllItems.aspx" and item.get("ListName") == "Dokumenter":
            ViewId = item.get("ViewId")

    if ViewId is None:
        raise ValueError(f"ViewId for AllItems.aspx not found.")


    list_url = f"'/{case_url}/Dokumenter'"
    if folder_path is None:
        root_folder = f"/{case_url}/Dokumenter"
    else:
        folder_path = folder_path.replace("''", "'")
        root_folder = f"/{case_url}/Dokumenter/{folder_path}"

    headers = {
        'content-type': 'application/json;odata=verbose'
    }



    url = f"{APIURL}/{case_url}/_api/web/GetList(@listUrl)/RenderListDataAsStream?@listUrl={list_url}&View={ViewId}&RootFolder={root_folder}"

    while True:
        # Define the payload as a Python dictionary
        payload_dict = {
            "parameters": {
                "__metadata": {
                    "type": "SP.RenderListDataParameters"
                },
                "ViewXml": (
                    "<View>"
                    "<Query>"
                    "<Where>"
                    "<Eq>"
                    "<FieldRef Name=\"UniqueId\" />"
                    f"<Value Type=\"Guid\">{str(uuid.uuid4())}</Value>"
                    "</Eq>"
                    "</Where>"
                    "</Query>"
                    "<RowLimit Paged=\"TRUE\">100</RowLimit>"
                    "</View>"
                )
            }
        }

        # Convert the dictionary to a JSON string
        payload = json.dumps(payload_dict, indent=4)

        # Make the API request
        response = session.post(url, headers=headers, data=payload)
        response.raise_for_status()

        data = response.json()

        # Loop through the rows to find the document
        for row in data.get('Row', []):
            if str(row.get('FileLeafRef')).lower() == str(file_name).lower():
                orchestrator_connection.log_info(f'DocID: {row.get("DocID")}')
                return row.get('DocID')

        # If no match found and there's a next href, update the URL and repeat
        next_href = data.get('NextHref')
        if next_href:
            # Replace "?" with "&" in the next_href
            next_href = next_href.replace("?", "&", 1)
            url = f"{APIURL}/{case_url}/_api/web/GetList(@listUrl)/RenderListDataAsStream?@listUrl={list_url}{next_href}"
            orchestrator_connection.log_info(f"Fetching next page: {url}")
        else:
            # No more pages and DocID not found
            orchestrator_connection.log_info("DocID not found.")
            return None
        
        orchestrator_connection.log_info(url)
        response = session.post(url, headers=headers, data=payload)
        response.raise_for_status()

        data = response.json()
        for row in data['Row']:
            if str(row.get('FileLeafRef')).lower() == str(file_name).lower():
                orchestrator_connection.log_info(f'DocID: {row.get("DocID")}')
                return row.get('DocID')
        return None

# Example usage
def chunk_uploaded(offset, total_size, orchestrator_connection: OrchestratorConnection):
    orchestrator_connection.log_info(f"Uploaded {offset} out of {total_size} bytes")

def get_case_type(APIURL, session, case_id):
    response = session.get(f"{APIURL}/_goapi/Cases/Metadata/{case_id}/False")
    # Parse the XML data in Metadata
    metadata = response.json()["Metadata"]

    # Parse the XML string and find the 'row' element
    root = ET.fromstring(metadata)
    case_url = root.attrib.get('ows_CaseUrl')
    return case_url

def update_metadata(APIURL, docid, session, metadata, orchestrator_connection: OrchestratorConnection):
    # Find the part of the string that contains ows_Dato
    start_index = metadata.find('ows_Dato="') + len('ows_Dato="')
    end_index = metadata.find('"', start_index)

    # Extract the date value
    date_str = metadata[start_index:end_index]

    # Split the date by '-'
    day, month, year = date_str.split('-')

    # Construct the new date in mm-dd-yyyy format
    flipped_date = f'{month}-{day}-{year}'

    # Replace the original date with the new one in the metadata string
    metadata = metadata.replace(date_str, flipped_date)

    payload = {"DocId": docid,
               "MetadataXml": metadata}

    response = session.post(f'{APIURL}/_goapi/Documents/Metadata', data=payload, timeout=600)
    response.raise_for_status()

def upload_large_document(APIURL, payload, session, binary, orchestrator_connection: OrchestratorConnection):
    case_id = payload["CaseId"]
    folder_path = payload["FolderPath"]
    file_name = payload["FileName"]
    file_name2 = file_name
    metadata = payload["Metadata"]
    case_url = get_case_type(APIURL, session, case_id)
    request_digest = request_form_digest(APIURL, case_url, session)
    file_name = file_name.replace("'", "''")
    file_name = file_name.replace("%", "%25")
    file_name = file_name.replace("+", "%2B")
    file_name = file_name.replace("/", "%2F")
    file_name = file_name.replace("?", "%3F")
    file_name = file_name.replace("#", "%23")
    file_name = file_name.replace("&", "%26")

    folder_path = folder_path.replace("'", "''")
    folder_path = folder_path.replace("%", "%25")
    folder_path = folder_path.replace("+", "%2B")
    folder_path = folder_path.replace("/", "%2F")
    folder_path = folder_path.replace("?", "%3F")
    folder_path = folder_path.replace("#", "%23")
    folder_path = folder_path.replace("&", "%26")

    chunked_file_upload(APIURL, case_url, binary, file_name, session, request_digest, folder_path, orchestrator_connection)
    time.sleep(5)
    docid = get_docid(file_name2, APIURL, case_url, folder_path, session, orchestrator_connection)
    if docid is not None:
        update_metadata(APIURL, docid, session, metadata, orchestrator_connection)
        # Return the success message with DocId
        return f'{{"DocId":{docid}}}'
    else:
        return 'Failed to get DocId'