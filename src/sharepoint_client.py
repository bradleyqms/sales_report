import os
import msal
import requests
import urllib.parse

class SharePointHandler:
    def __init__(self, site_url, client_id, client_secret, quiet=False):
        """
        Initialize SharePoint connection using Microsoft Graph API.
        
        Args:
            site_url (str): SharePoint site URL
            client_id (str): Azure AD app client ID
            client_secret (str): Azure AD app client secret
            quiet (bool): Suppress verbose output (default: False)
        """
        self.site_url = site_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.quiet = quiet
        
        # Extract tenant from site URL or default to common
        # Ideally should be provided, but we can guess or use common
        self.tenant = "qmsmedicosmetics.onmicrosoft.com" 
        self.authority = f"https://login.microsoftonline.com/{self.tenant}"
        self.scope = ["https://graph.microsoft.com/.default"]
        
        self.access_token = None
        self.site_id = None
        
        # Authenticate immediately
        self._authenticate()
        self._get_site_id()

    def _authenticate(self):
        """Acquire token via MSAL"""
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret
        )
        result = app.acquire_token_for_client(scopes=self.scope)
        
        if "access_token" in result:
            self.access_token = result['access_token']
            self.headers = {
                'Authorization': 'Bearer ' + self.access_token,
                'Content-Type': 'application/json'
            }
        else:
            raise Exception(f"Authentication failed: {result.get('error_description')}")

    def _get_site_id(self):
        """Get Graph Site ID from URL"""
        # Parse hostname and relative path from URL
        parsed = urllib.parse.urlparse(self.site_url)
        hostname = parsed.netloc
        site_path = parsed.path.strip('/')
        
        # Graph API endpoint to get site by path
        endpoint = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
        
        response = requests.get(endpoint, headers=self.headers)
        if response.status_code == 200:
            self.site_id = response.json()['id']
            if not self.quiet:
                print(f"Connected to site: {response.json().get('displayName')} (ID: {self.site_id})")
        else:
            raise Exception(f"Failed to get site ID: {response.text}")

    def download_file(self, sharepoint_path, local_path):
        """
        Download a file from SharePoint using Graph API.
        
        Args:
            sharepoint_path (str): Server relative path (e.g. /sites/SiteName/Shared Documents/Folder/file.csv)
            local_path (str): Local path to save file
        """
        # We need to find the drive (document library) and item
        # Graph API format: /sites/{site-id}/drive/root:/{path-relative-to-root}:/content
        
        # Extract path relative to site
        # sharepoint_path is full server relative path: /sites/DATAANDREPORTING/Shared Documents/SAP Extracts/file.csv
        # We need to map this to a Drive.
        # "Shared Documents" is usually the default Drive.
        
        # Let's try to resolve the item by path directly from the site
        # Endpoint: /sites/{site-id}/drive/root:/{path-in-library}
        
        # We need to strip the site prefix from the path to get path relative to library?
        # Actually, simpler: /sites/{site-id}/lists/{list-name}/items... no.
        
        # Best way: Get the drive item by server relative path
        # But Graph doesn't support server relative path lookup easily across drives.
        # We assume "Shared Documents" is the default drive.
        
        # Parse path: /sites/DATAANDREPORTING/Shared Documents/SAP Extracts/file.csv
        # Remove /sites/DATAANDREPORTING/
        parsed_site = urllib.parse.urlparse(self.site_url)
        site_prefix = parsed_site.path # /sites/DATAANDREPORTING
        
        if sharepoint_path.startswith(site_prefix):
            relative_path = sharepoint_path[len(site_prefix):].strip('/')
        else:
            relative_path = sharepoint_path.strip('/')
            
        # If path starts with "Shared Documents", that's the default drive
        if relative_path.startswith("Shared Documents/"):
            item_path = relative_path[len("Shared Documents/"):]
            # Encode path components but keep slashes
            encoded_path = urllib.parse.quote(item_path)
            endpoint = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{encoded_path}:/content"
        else:
            # Fallback: try to find drive by name? For now assume default drive
            encoded_path = urllib.parse.quote(relative_path)
            endpoint = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{encoded_path}:/content"
            
        if not self.quiet:
            print(f"Downloading from: {endpoint}")
        response = requests.get(endpoint, headers=self.headers)
        
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            if not self.quiet:
                print(f"Downloaded {sharepoint_path} to {local_path}")
        elif response.status_code == 404:
             # Try to find if "SAP Extracts" is a separate drive
             if not self.quiet:
                 print("File not found in default drive. Checking other drives...")
             drives_endpoint = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
             drives_response = requests.get(drives_endpoint, headers=self.headers)
             
             if drives_response.status_code == 200:
                 drives = drives_response.json().get('value', [])
                 # Check if first part of path matches a drive name
                 path_parts = relative_path.split('/')
                 if path_parts:
                     potential_drive = path_parts[0]
                     for drive in drives:
                         if drive['name'] == potential_drive:
                             if not self.quiet:
                                 print(f"Found drive: {drive['name']}")
                             # Construct new path relative to this drive
                             new_relative_path = '/'.join(path_parts[1:])
                             new_endpoint = f"https://graph.microsoft.com/v1.0/drives/{drive['id']}/root:/{new_relative_path}:/content"
                             if not self.quiet:
                                 print(f"Retrying download from: {new_endpoint}")
                             
                             retry_response = requests.get(new_endpoint, headers=self.headers)
                             if retry_response.status_code == 200:
                                 with open(local_path, 'wb') as f:
                                     f.write(retry_response.content)
                                 if not self.quiet:
                                     print(f"Downloaded {sharepoint_path} to {local_path}")
                                 return
                             else:
                                 if not self.quiet:
                                     print(f"Retry failed: {retry_response.status_code}")
             
             raise Exception(f"Failed to download file: {response.status_code} {response.text}")
        else:
            raise Exception(f"Failed to download file: {response.status_code} {response.text}")

    def upload_file(self, local_path, sharepoint_path):
        """
        Upload a file to SharePoint using Graph API.
        """
        # Similar path logic
        parsed_site = urllib.parse.urlparse(self.site_url)
        site_prefix = parsed_site.path
        
        if sharepoint_path.startswith(site_prefix):
            relative_path = sharepoint_path[len(site_prefix):].strip('/')
        else:
            relative_path = sharepoint_path.strip('/')
            
        if relative_path.startswith("Shared Documents/"):
            item_path = relative_path[len("Shared Documents/"):]
            encoded_path = urllib.parse.quote(item_path)
            endpoint = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{encoded_path}:/content"
        else:
            encoded_path = urllib.parse.quote(relative_path)
            endpoint = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/root:/{encoded_path}:/content"
            
        if not self.quiet:
            print(f"Uploading to: {endpoint}")
        
        with open(local_path, 'rb') as f:
            content = f.read()
            
        # Use PUT to upload content
        headers = self.headers.copy()
        headers['Content-Type'] = 'application/octet-stream'
        
        response = requests.put(endpoint, headers=headers, data=content)
        
        if response.status_code in [200, 201]:
            if not self.quiet:
                print(f"Uploaded {local_path} to {sharepoint_path}")
        else:
            raise Exception(f"Failed to upload file: {response.status_code} {response.text}")

def download_inputs(sp_handler, sharepoint_paths, temp_dir):
    local_paths = {}
    for key, sp_path in sharepoint_paths.items():
        local_path = os.path.join(temp_dir, os.path.basename(sp_path))
        sp_handler.download_file(sp_path, local_path)
        local_paths[key] = local_path
    return local_paths

def upload_outputs(sp_handler, local_base_path, sharepoint_output_folder, base_filename):
    extensions = ['.csv', '.txt', '.html']
    for ext in extensions:
        local_path = f"{local_base_path}{ext}"
        sharepoint_path = f"{sharepoint_output_folder}{base_filename}{ext}"
        sp_handler.upload_file(local_path, sharepoint_path)