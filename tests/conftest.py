import os

import pytest

import kgw


# Fixtures are automatically recognized by pytest, no need to import them


@pytest.fixture(scope="session")
def workdir():
    workdir = "test_kgw_workdir"

    # Setup: remove the directory if it already exists
    # Commented out to enable reuse of partial results
    # if os.path.exists(workdir):
    #    shutil.rmtree(workdir)

    # Create a fresh directory
    os.makedirs(workdir, exist_ok=True)

    # Yield control to the test
    yield workdir

    # Teardown: remove the directory after the test completes
    # Commented out to enable reuse and sharing of results
    # if os.path.exists(workdir):
    #    shutil.rmtree(workdir)


@pytest.fixture(
    params=[
        (
            "hald",
            kgw.biomedicine.Hald,
            kgw.biomedicine.Hald.get_versions()[-1],
            12_257,
            116_495,
        ),
        (
            "oregano",
            kgw.biomedicine.Oregano,
            kgw.biomedicine.Oregano.get_versions()[-1],
            88_937,
            819_884,
        ),
        (
            "primekg",
            kgw.biomedicine.PrimeKg,
            kgw.biomedicine.PrimeKg.get_versions()[-1],
            129_375,
            8_100_498,
        ),
        (
            "monarchkg",
            kgw.biomedicine.MonarchKg,
            kgw.biomedicine.MonarchKg.get_versions()[-1],
            1_028_155,
            11_076_689,
        ),
        (
            "ckg",
            kgw.biomedicine.Ckg,
            kgw.biomedicine.Ckg.get_versions()[-1],
            14_543_042,
            188_266_233,
        ),
    ]
)
def project_params_full(request):
    return request.param


@pytest.fixture(
    params=[
        (
            "hald",
            kgw.biomedicine.Hald,
            kgw.biomedicine.Hald.get_versions()[-1],
            12_257,
            116_495,
        ),
        (
            "oregano",
            kgw.biomedicine.Oregano,
            kgw.biomedicine.Oregano.get_versions()[-1],
            88_937,
            819_884,
        ),
    ]
)
def project_params(request):
    return request.param
