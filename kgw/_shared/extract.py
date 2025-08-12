import bz2
import hashlib
import math
import os
import re
import subprocess
import tarfile
import time
import zipfile

import requests
from bs4 import BeautifulSoup


# Basic OS functionality


def run_shell_command(command):
    result = subprocess.run(
        command, check=True, capture_output=True, text=True, shell=True
    )
    return result.stdout


def create_dir(dirpath):
    os.makedirs(dirpath, exist_ok=True)


def delete_file(filepath):
    try:
        os.remove(filepath)
    except OSError:
        pass


def get_file_size(filepath):
    try:
        size = os.path.getsize(filepath)
    except FileNotFoundError:
        size = 0
    return size


def extract_bz2(filepath):
    out_filepath, _ = os.path.splitext(filepath)
    with bz2.open(filepath, 'rb') as f_in, open(out_filepath, 'wb') as f_out:
        f_out.write(f_in.read())


def extract_tar_gz(filepath):
    directory = os.path.dirname(filepath)
    with tarfile.open(filepath, "r:gz") as tar:
        tar.extractall(path=directory)


def extract_zip(filepath):
    directory = os.path.dirname(filepath)
    with zipfile.ZipFile(filepath, "r") as zip_ref:
        zip_ref.extractall(path=directory)


# Basic web functionality


def get_request_with_retries(url, num_retries=3, delay_in_sec=15, **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = 5

    for _ in range(num_retries + 1):
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            last_exception = e
        time.sleep(delay_in_sec)
    else:
        raise last_exception
    return response


def has_internet_connection():
    urls = ["https://1.1.1.1", "https://8.8.8.8"]
    for url in urls:
        response = get_request_with_retries(url, num_retries=2, delay_in_sec=10)
        if response.status_code == 200:
            return True
    return False


def ensure_internet_connection():
    if not has_internet_connection():
        raise Exception("No internet connection.")


def get_remote_file_size(url):
    ensure_internet_connection()

    try:
        # Attempt 1: HEAD request with "content-length" response attribute
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) "
            "Gecko/20100101 Firefox/127.0"
        }
        response = requests.head(url, headers=headers, allow_redirects=True, timeout=10)
        remote_size = int(response.headers.get("content-length", 0))

        # Attempt 2: GET request with Range header and "content-range" response attribute
        if remote_size == 0:
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) "
                "Gecko/20100101 Firefox/127.0",
                "Range": "bytes=0-1",
            }
            response = requests.get(
                url, headers=headers, allow_redirects=True, timeout=10
            )
            content_range = response.headers.get("content-range")
            try:
                remote_size = int(content_range.split("/")[-1])
            except Exception:
                return 0
        return remote_size
    except requests.exceptions.RequestException as e:
        print(f"Error fetching remote size: {str(e)}")
        return 0


def download_file(url, filepath, local_size=0):
    headers = {"Range": f"bytes={local_size}-"}
    response = requests.get(url, headers=headers, stream=True)
    with open(filepath, "ab") as f:
        for data in response.iter_content(chunk_size=1024):
            f.write(data)


def fetch_file(url, filepath):
    remote_size = get_remote_file_size(url)
    local_size = get_file_size(filepath)
    if local_size == 0:
        # Found no local copy of the file. Starting the download.
        download_file(url, filepath, local_size)
    elif remote_size == 0:
        # Found a local copy but couldn't determine the remote file size.
        # Starting a fresh download to overwrite the possibly partial local file.
        delete_file(filepath)
        local_size = 0
        download_file(url, filepath, local_size)
    else:
        if remote_size == local_size:
            # Found a full local copy of the file
            pass
        elif remote_size > local_size:
            # Found a partial local copy of the file. Resuming the download.
            download_file(url, filepath, local_size)
        else:
            # Found a local copy but it is larger than the remote file.
            # Starting a fresh download to overwrite the local file.
            delete_file(filepath)
            local_size = 0
            download_file(url, filepath, local_size)


def is_valid_file_by_md5(filepath, md5_hash):
    with open(filepath, "rb") as f:
        bytes = f.read()
    md5_hash_calc = hashlib.md5(bytes).hexdigest()
    return md5_hash == md5_hash_calc


def is_valid_file_by_sha256(filepath, sha256_hash):
    with open(filepath, "rb") as f:
        bytes = f.read()
    sha256_hash_calc = hashlib.sha256(bytes).hexdigest()
    return sha256_hash == sha256_hash_calc


# Access to data repositories on web


def get_versions_from_figshare(dataset_id):
    url = f"https://api.figshare.com/v2/articles/{dataset_id}/versions"
    response = get_request_with_retries(url)
    raw_data = response.json()
    versions = [str(entry["version"]) for entry in raw_data]
    return versions


def get_metadata_from_figshare(dataset_id, version):
    url = f"https://api.figshare.com/v2/articles/{dataset_id}/versions/{version}"
    response = get_request_with_retries(url)
    raw_data = response.json()
    data = {}
    date = raw_data["created_date"]
    for entry in raw_data["files"]:
        name = entry["name"]
        url = entry["download_url"]
        size = entry["size"]
        md5 = entry["computed_md5"]
        data[name] = dict(version=version, date=date, url=url, md5=md5, size=size)
    return data


def get_versions_from_mendeley(dataset_id):
    url = f"https://data.mendeley.com/public-api/datasets/{dataset_id}/versions"
    response = get_request_with_retries(url)
    raw_data = response.json()
    versions = [str(entry["version"]) for entry in raw_data]
    return versions


def get_metadata_from_mendeley(dataset_id, version):
    url = (
        "https://data.mendeley.com/public-api/datasets/"
        f"{dataset_id}/files?folder_id=root&version={version}"
    )
    response = get_request_with_retries(url)
    raw_data = response.json()
    data = {}
    for entry in raw_data:
        name = entry["filename"]
        date = entry["content_details"]["created_date"]
        url = entry["content_details"]["download_url"]
        size = entry["size"]
        sha256 = entry["content_details"]["sha256_hash"]
        data[name] = dict(version=version, date=date, url=url, size=size, sha256=sha256)
    return data


def get_versions_from_monarch():
    url = "https://data.monarchinitiative.org/monarch-kg/index.html"
    response = get_request_with_retries(url)
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all("a")
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    url_pattern = re.compile(
        r"^https://data.monarchinitiative.org/monarch-kg/\d{4}-\d{2}-\d{2}/index.html$"
    )
    versions = []
    for link in links:
        href = link.get("href")
        if href:
            text = str(link.get_text(strip=True))
            if date_pattern.match(text) and url_pattern.match(href) and text in href:
                versions.append(text)
    return versions


def get_metadata_from_monarch(version):
    url = f"https://data.monarchinitiative.org/monarch-kg/{version}/index.html"
    response = get_request_with_retries(url)
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all("a")
    data = {}
    for link in links:
        href = link.get("href")
        if href:
            text = link.get_text(strip=True)
            if "." in text and ".." not in text:
                name = text
                data[name] = dict(version=version, date=version, url=href)
    return data


def get_metadata_from_primekg():
    # Caution: Uses a custom mirror because Harvard Dataverse does not enable
    # reliable programmatic access
    url = (
        "https://raw.githubusercontent.com/robert-haas/"
        "primekg-mirror/main/metadata.json"
    )
    response = get_request_with_retries(url)
    data = response.json()
    return data


def get_metadata_from_hetionet():
    # Caution: Uses hard coded data because there is only one version
    data = {
        "hetionet-v1.0.json.bz2": {
            "url": "https://github.com/hetio/hetionet/raw/refs/heads/main/hetnet/json/hetionet-v1.0.json.bz2",
            "md5": "cd6268d361592de9d2b2f4639a34a3c7",
        }
    }
    return data


# Filtering


def is_informative_value(value):
    """Find out if a value is not None, not an empty string, and not NaN."""
    if value is None or value == "":
        return False

    try:
        # Check if value is NaN
        return not math.isnan(value)
    except TypeError:
        # Value is not a float or does not support NaN, assume valid
        return True
