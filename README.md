# TODO

## Requirements

* python 3.5+
* Postgres 9.5+


## Setup

We recommend using a pyenv environment

    pyenv virtualenv 3.5.3 clover-pycon
    pyenv local clover-pycon

Then run the following to install your packages:

    pip install -r requirements.txt

## Running tests

To run the unit tests normally:

    pytest test_suite/

## Notebook

Create a fixed development database:

    createdb VincentLa

To start the notebook:

    jupyter notebook

Open and run `data/generate_data.ipynb`

Open and run `em_upcoming/EM Upcoding.ipynb`
