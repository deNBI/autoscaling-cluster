Development Guide
=================

This document covers development practices for the Autoscaling Cluster project.

Code Structure
--------------

The project is organized into the following modules:

- ``autoscaling.config`` - Configuration management
- ``autoscaling.core`` - Core scaling logic
- ``autoscaling.scheduler`` - Scheduler abstraction
- ``autoscaling.cloud`` - Cloud API access
- ``autoscaling.data`` - Database/History
- ``autoscaling.utils`` - Utility functions
- ``autoscaling.cluster`` - Cluster API
- ``autoscaling.cli`` - Command line interface

Running Tests
-------------

.. code-block:: bash

   pytest tests/ -v

Running with coverage:

.. code-block:: bash

   pytest --cov=autoscaling tests/

Building Documentation
----------------------

.. code-block:: bash

   cd docs
   make html

Code Style
----------

- Follow PEP 8 style guidelines
- Type hints are required for all functions
- Docstrings should follow Google style
