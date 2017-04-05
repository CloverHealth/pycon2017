# PyCon 2017 ETL Workshop

## Requirements

* python 3.5.3
* Postgres 9.6.x

## Setup

_TODO_: Change instructions to be Docker-based

Run the following:

    pip install -r requirements.txt
    pip install -r test_requirements.txt
    
## Running tests

To run tests normally:

    pytest src/

However, for faster repeated execution, you can reuse the same test database with models using the `--keepdb` flag

    pytest --keepdb src/
    ...
    pytest --keepdb src/
    ...
    # when finished or if the model schema has changed, run the following
    rm -rf .test_database
