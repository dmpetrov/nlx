|Banner|

`Website <https://dvc.org>`_
• `Docs <https://dvc.org/doc>`_
• `Blog <http://blog.dataversioncontrol.com>`_
• `Twitter <https://twitter.com/DVCorg>`_
• `Chat (Community & Support) <https://dvc.org/chat>`_
• `Tutorial <https://dvc.org/doc/get-started>`_
• `Mailing List <https://sweedom.us10.list-manage.com/subscribe/post?u=a08bf93caae4063c4e6a351f6&id=24c0ecc49a>`_

|Release| |CI| |Maintainability| |Coverage| |Donate| |DOI|

|PyPI| |Packages| |Brew| |Conda| |Choco| |Snap|

|

**Data Version Control** or **DVC** is an **open-source** tool for data science and machine
learning projects. Key features:

#. Simple **command line** Git-like experience. Does not require installing and maintaining
   any databases. Does not depend on any proprietary online services.

#. Management and versioning of **datasets** and **machine learning models**. Data is saved in
   S3, Google cloud, Azure, Alibaba cloud, SSH server, HDFS, or even local HDD RAID.

#. Makes projects **reproducible** and **shareable**; helping to answer questions about how
   a model was built.

#. Helps manage experiments with Git tags/branches and **metrics** tracking.

**DVC** aims to replace spreadsheet and document sharing tools (such as Excel or Google Docs)
which are being used frequently as both knowledge repositories and team ledgers.
DVC also replaces both ad-hoc scripts to track, move, and deploy different model versions;
as well as ad-hoc data file suffixes and prefixes.

.. contents:: **Contents**
  :backlinks: none

How DVC works
=============

We encourage you to read our `Get Started <https://dvc.org/doc/get-started>`_ guide to better understand what DVC
is and how it can fit your scenarios.

The easiest (but not perfect!) *analogy* to describe it: DVC is Git (or Git-LFS to be precise) & Makefiles
made right and tailored specifically for ML and Data Science scenarios.

#. ``Git/Git-LFS`` part - DVC helps store and share data artifacts and models, connecting them with a Git repository.
#. ``Makefile``\ s part - DVC describes how one data or model artifact was built from other data and code.

DVC usually runs along with Git. Git is used as usual to store and version code (including DVC meta-files). DVC helps
to store data and model files seamlessly out of Git, while preserving almost the same user experience as if they
were stored in Git itself. To store and share the data cache, DVC supports multiple remotes - any cloud (S3, Azure,
Google Cloud, etc) or any on-premise network storage (via SSH, for example).

|Flowchart|

The DVC pipelines (computational graph) feature connects code and data together. It is possible to explicitly
specify all steps required to produce a model: input dependencies including data, commands to run,
and output information to be saved. See the quick start section below or
the `Get Started <https://dvc.org/doc/get-started>`_ tutorial to learn more.

Quick start
===========

Please read `Get Started <https://dvc.org/doc/get-started>`_ guide for a full version. Common workflow commands include:

+-----------------------------------+-------------------------------------------------------------------+
| Step                              | Command                                                           |
+===================================+===================================================================+
| Track data                        | | ``$ git add train.py``                                          |
|                                   | | ``$ dvc add images.zip``                                        |
+-----------------------------------+-------------------------------------------------------------------+
| Connect code and data by commands | | ``$ dvc run -d images.zip -o images/ unzip -q images.zip``      |
|                                   | | ``$ dvc run -d images/ -d train.py -o model.p python train.py`` |
+-----------------------------------+-------------------------------------------------------------------+
| Make changes and reproduce        | | ``$ vi train.py``                                               |
|                                   | | ``$ dvc repro model.p.dvc``                                     |
+-----------------------------------+-------------------------------------------------------------------+
| Share code                        | | ``$ git add .``                                                 |
|                                   | | ``$ git commit -m 'The baseline model'``                        |
|                                   | | ``$ git push``                                                  |
+-----------------------------------+-------------------------------------------------------------------+
| Share data and ML models          | | ``$ dvc remote add myremote -d s3://mybucket/image_cnn``        |
|                                   | | ``$ dvc push``                                                  |
+-----------------------------------+-------------------------------------------------------------------+

Installation
============

There are four options to install DVC: ``pip``, Homebrew, Conda (Anaconda) or an OS-specific package.
Full instructions are `available here <https://dvc.org/doc/get-started/install>`_.

Snap (Snapcraft/Linux)
----------------------

|Snap|

.. code-block:: bash

   snap install dvc --classic

This corresponds to the latest tagged release.
Add ``--beta`` for the latest tagged release candidate,
or ``--edge`` for the latest ``master`` version.

Choco (Chocolatey/Windows)
--------------------------

|Choco|

.. code-block:: bash

   choco install dvc

Brew (Homebrew/Mac OS)
----------------------

|Brew|

.. code-block:: bash

   brew install dvc

Conda (Anaconda)
----------------

|Conda|

.. code-block:: bash

   conda install -c conda-forge mamba # installs much faster than conda
   mamba install -c conda-forge dvc

Depending on the remote storage type you plan to use to keep and share your data, you might need to
install optional dependencies: `dvc-s3`, `dvc-azure`, `dvc-gdrive`, `dvc-gs`, `dvc-oss`, `dvc-ssh`.

pip (PyPI)
----------

|PyPI|

.. code-block:: bash

   pip install dvc

Depending on the remote storage type you plan to use to keep and share your data, you might need to specify
one of the optional dependencies: ``s3``, ``gs``, ``azure``, ``oss``, ``ssh``. Or ``all`` to include them all.
The command should look like this: ``pip install dvc[s3]`` (in this case AWS S3 dependencies such as ``boto3``
will be installed automatically).

To install the development version, run:

.. code-block:: bash

   pip install git+git://github.com/iterative/dvc

Package
-------

|Packages|

Self-contained packages for Linux, Windows, and Mac are available. The latest version of the packages
can be found on the GitHub `releases page <https://github.com/iterative/dvc/releases>`_.

Ubuntu / Debian (deb)
^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   sudo wget https://dvc.org/deb/dvc.list -O /etc/apt/sources.list.d/dvc.list
   sudo apt-get update
   sudo apt-get install dvc

Fedora / CentOS (rpm)
^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   sudo wget https://dvc.org/rpm/dvc.repo -O /etc/yum.repos.d/dvc.repo
   sudo yum update
   sudo yum install dvc

Comparison to related technologies
==================================

#. *Data Engineering* tools such as `AirFlow <https://airflow.apache.org/>`_, `Luigi <https://github.com/spotify/luigi>`_ and
   others - in DVC data, model and ML pipelines represent a single ML project and focuses on data scientist' experience while
   data engineering pipeline orchestrates multiple data projects and focuses on efficent execution. DVC project can be used
   from data pipeline as a single execution step. `AirFlow DVC <https://github.com/covid-genomics/airflow-dvc>`_ is an example
   of such integration.

#. `Git-annex <https://git-annex.branchable.com/>`_ - DVC uses the idea of storing the content of large files (which should
   not be in a Git repository) in a local key-value store, and uses file hardlinks/symlinks instead of
   copying/duplicating files.

#. `Git-LFS <https://git-lfs.github.com/>`_ - DVC is compatible with any remote storage (S3, Google Cloud, Azure, SSH,
   etc). DVC also uses reflinks or hardlinks to avoid copy operations on checkouts; thus handling large data files
   much more efficiently.

#. *Makefile* (and analogues including ad-hoc scripts) - DVC tracks dependencies (in a directed acyclic graph).

#. `Workflow Management Systems <https://en.wikipedia.org/wiki/Workflow_management_system>`_ - DVC is a workflow
   management system designed specifically to manage machine learning experiments. DVC is built on top of Git.

#. `DAGsHub <https://dagshub.com/>`_ - online service of Git+DVC repositories with pipeline and metrics visualization and
   DVC-specific cloud storage.

#. `DVC Studio <https://studio.iterative.ai/>`_ - online service of DVC repositories visualisation from DVC team. Also,
   integrated with `CML (CI/CD for ML) <https://cml.dev/>`_ for training models in clouds and Kubernetes.


Contributing
============

|Maintainability| |Donate|

Contributions are welcome! Please see our `Contributing Guide <https://dvc.org/doc/user-guide/contributing/core>`_ for more
details.

Mailing List
============

Want to stay up to date? Want to help improve DVC by participating in our occasional polls? Subscribe to our `mailing list <https://sweedom.us10.list-manage.com/subscribe/post?u=a08bf93caae4063c4e6a351f6&id=24c0ecc49a>`_. No spam, really low traffic.

Copyright
=========

This project is distributed under the Apache license version 2.0 (see the LICENSE file in the project root).

By submitting a pull request to this project, you agree to license your contribution under the Apache license version
2.0 to this project.

Citation
========

|DOI|

Iterative, *DVC: Data Version Control - Git for Data & Models* (2020)
`DOI:10.5281/zenodo.012345 <https://doi.org/10.5281/zenodo.3677553>`_.

Barrak, A., Eghan, E.E. and Adams, B. `On the Co-evolution of ML Pipelines and Source Code - Empirical Study of DVC Projects <https://mcis.cs.queensu.ca/publications/2021/saner.pdf>`_ , in Proceedings of the 28th IEEE International Conference on Software Analysis, Evolution, and Reengineering, SANER 2021. Hawaii, USA.


.. |Banner| image:: https://dvc.org/img/logo-github-readme.png
   :target: https://dvc.org
   :alt: DVC logo

.. |Release| image:: https://img.shields.io/badge/release-ok-brightgreen
   :target: https://travis-ci.com/iterative/dvc/branches
   :alt: Release

.. |CI| image:: https://github.com/iterative/dvc/workflows/Tests/badge.svg?branch=master
   :target: https://github.com/iterative/dvc/actions
   :alt: GHA Tests

.. |Maintainability| image:: https://codeclimate.com/github/iterative/dvc/badges/gpa.svg
   :target: https://codeclimate.com/github/iterative/dvc
   :alt: Code Climate

.. |Coverage| image:: https://codecov.io/gh/iterative/dvc/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/iterative/dvc
   :alt: Codecov

.. |Donate| image:: https://img.shields.io/badge/patreon-donate-green.svg?logo=patreon
   :target: https://www.patreon.com/DVCorg/overview
   :alt: Donate

.. |Snap| image:: https://img.shields.io/badge/snap-install-82BEA0.svg?logo=snapcraft
   :target: https://snapcraft.io/dvc
   :alt: Snapcraft

.. |Choco| image:: https://img.shields.io/chocolatey/v/dvc?label=choco
   :target: https://chocolatey.org/packages/dvc
   :alt: Chocolatey

.. |Brew| image:: https://img.shields.io/homebrew/v/dvc?label=brew
   :target: https://formulae.brew.sh/formula/dvc
   :alt: Homebrew

.. |Conda| image:: https://img.shields.io/conda/v/conda-forge/dvc.svg?label=conda&logo=conda-forge
   :target: https://anaconda.org/conda-forge/dvc
   :alt: Conda-forge

.. |PyPI| image:: https://img.shields.io/pypi/v/dvc.svg?label=pip&logo=PyPI&logoColor=white
   :target: https://pypi.org/project/dvc
   :alt: PyPI

.. |Packages| image:: https://img.shields.io/github/v/release/iterative/dvc?label=deb|pkg|rpm|exe&logo=GitHub
   :target: https://github.com/iterative/dvc/releases/latest
   :alt: deb|pkg|rpm|exe

.. |DOI| image:: https://img.shields.io/badge/DOI-10.5281/zenodo.3677553-blue.svg
   :target: https://doi.org/10.5281/zenodo.3677553
   :alt: DOI

.. |Flowchart| image:: https://dvc.org/img/flow.gif
   :target: https://dvc.org/img/flow.gif
   :alt: how_dvc_works
