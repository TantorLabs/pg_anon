# pg_anon stateless REST API service

## Overview
This service is designed to integrate `pg_anon` functionality into any system via HTTP requests.
It works just as a wrapper of CLI version of `pg_anon`. REST API calls will prepare CLI params and run CLI version of pg_anon. 
It doesn’t keep state or store data into DB, so it can be scaled easily without extra setup.
However, this means that the system integrating pg_anon must implement its own storage for dictionaries, dump tasks, and restore tasks.

⚠️ Not suitable for fully autonomous operation.
The creation of a standalone version service is in the plans and will not affect the current stateless service.

## Installation Guide

### Preconditions

1. The tool supports Python3.11 and higher versions. The code is hosted on the following repository: [pg_anon repository on Github](https://github.com/TantorLabs/pg_anon).
2. ❗ All dumps will be stored in directory `/path_to_pg_anon/output`. If REST API service will be scaled, you must create symlink for this directory on shared disk. Because restore operations also will get dumps from `/path_to_pg_anon/output`. 

### Installation Instructions
1. Setup virtual environment and install `pg_anon` dependencies to follow [pg_anon installation guide](https://github.com/TantorLabs/pg_anon?tab=readme-ov-file#installation-guide)
2. Open directory of the service: `cd rest_api`
3. Install the dependencies of REST API service: `pip install -r requirements.txt`

### Configuring
If you want to work with multiple versions of PostgreSQL, you need to create `/path_to_pg_anon/config.yml` by [this guide](https://github.com/TantorLabs/pg_anon?tab=readme-ov-file#configuring-of-pg_anon)
This config will be used automatically.

## Usage
1. Run service - `python -m uvicorn rest_api.api:app`
2. Open API documentation - http://0.0.0.0:8000/docs#/
