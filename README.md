# PyCon 2017 ETL Workshop

## Requirements

* python 3.5+
* Postgres 9.5+

## Setup

Run the following:

    pip install -r requirements.txt
    pip install -r test_requirements.txt
    pip install -r dev_requirements.txt
    
## Running tests

To run tests normally:

    pytest src/

However, for faster repeated execution, you can reuse the same test database with models using the `--keepdb` flag

    pytest --keepdb src/
    ...
    pytest --keepdb src/
    ...
    # when finished or if the model schema has changed, just run the following
    rm -rf .test_database

## Running the notebook

Start up Jupyter Notebook

    jupyter notebook

Open the `etl_workshop` notebook (`etl_workshop.ipynb`) and enjoy!
