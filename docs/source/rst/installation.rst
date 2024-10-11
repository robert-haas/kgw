Installation Guide
##################

This page explains the installation of the Python 3 package
:doc:`kgw <package_references>`
and its optional dependencies.



Required: kgw
=============

The package
`kgw <https://pypi.org/project/kgw>`__
is available on the
`Python Package Index (PyPI) <https://pypi.org>`__
and therefore can be easily installed with Python's
default package manager
`pip <https://pypi.org/project/pip>`__ by using the following
command in a shell:

.. code-block:: console

   $ pip install kgw

Additional remarks:

- kgw is compatible with
  `Python 3.6 upwards <https://www.python.org/downloads>`_.
- Using an environment manager like
  `virtualenv <https://virtualenv.pypa.io>`__ or
  `conda <https://docs.conda.io>`__
  is a good idea in almost any case. These tools allow to create a
  `virtual environment <https://packaging.python.org/tutorials/installing-packages/#creating-virtual-environments>`__
  into which pip can install the package in an isolated fashion instead
  of globally on your system. Thereby it does not interfere with the
  installation of other projects, which may require a different version
  of some shared dependency.


Optional: docker
================

Currently the project "Clinical Knowledge Graph (CKG)" requires a Docker installation,
because its data comes in a format that requires a particular Neo4j installation with
some quite specific addons and configurations that can be best handled with a reproducible
system setup inside a Docker image.

Two installations steps are required:

1. Install the
   `Docker program <https://docs.docker.com/get-started/get-docker/>`__
   according to the official instructions for your operating system.
2. Install the
   `Docker SDK for Python <https://pypi.org/project/docker/>`__
   with Python's default package manager
   `pip <https://pypi.org/project/pip>`__:

   .. code-block:: console

      $ pip install docker