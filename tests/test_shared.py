import os

import pytest
import requests

from kgw import _shared as shared


@pytest.mark.parametrize(
    "command, expected_result",
    [
        ("echo 'hi there'", "hi there"),
        ("python --version", "."),
        ("docker --help", "docker"),
    ],
)
def test_run_shell_command(command, expected_result):
    result = shared.extract.run_shell_command(command)
    assert expected_result in result


def test_create_dir():
    dirname = "some_new_directory"
    shared.extract.create_dir(dirname)
    assert os.path.isdir(dirname)
    os.rmdir(dirname)


def test_get_file_size_and_delete_file():
    filename = "some_new_file"
    shared.extract.run_shell_command(f"echo 'hello world' > {filename}")
    assert os.path.isfile(filename)
    assert shared.extract.get_file_size(filename) > 0
    shared.extract.delete_file(filename)
    assert not os.path.isfile(filename)


def test_has_internet_connection_and_get_request_with_retries():
    assert shared.extract.has_internet_connection(), "No internet connection"

    shared.extract.ensure_internet_connection()

    url = "https://www.google.com"
    response = shared.extract.get_request_with_retries(url)
    assert "<" in response.text and ">" in response.text

    url = "https://www.somenonsense-urllajsfnalfkjajf1231231knsdfölsf.at"

    with pytest.raises(requests.exceptions.ConnectionError):
        shared.extract.get_request_with_retries(url, num_retries=1, delay_in_sec=0.1)


def test_get_remote_file_size():
    url = "https://www.google.com/robots.txt"
    assert shared.extract.get_remote_file_size(url) > 0

    url = "https://www.google.com/nonsensicalfilethatdoesnotexist12319182391.txt"
    assert shared.extract.get_remote_file_size(url) == 0


def test_download_file_and_fetch_file():
    url = "https://www.google.com/robots.txt"
    filename = "stobor_elgoog.txt"
    for func in (shared.extract.download_file, shared.extract.fetch_file):
        func(url, filename)
        assert os.path.isfile(filename)
        assert shared.extract.get_file_size(filename) > 0
        shared.extract.delete_file(filename)
        assert not os.path.isfile(filename)


def test_is_informative_value():
    for val in ["a", 123, 3.14, ("a", "b"), [1, 2], -1]:
        assert shared.extract.is_informative_value(val)

    for val in [None, "", float("nan")]:
        assert not shared.extract.is_informative_value(val)


def test_clean():
    assert shared.load.clean(1) == "1"
    assert shared.load.clean(3.14) == "3.14"
    assert shared.load.clean("a c") == '"a c"'
    assert shared.load.clean([1, 3.14, "a c"]) == r'"[1,3.14,\"a c\"]"'
