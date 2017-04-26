# PyCon 2017 ETL Workshop

## Requirements

* python 3.5+
* Postgres 9.5+
* graphviz

## Setup

Run the following:

    pip install -r requirements.txt
    
## Running tests

To run tests normally:

    pytest src/tests/

However, for faster repeated execution, you can reuse the same test database with models using the `--keepdb` flag.
This will create a re-useable database with pre-created model tables (but no data) in the `.test_db` subdirectory.

    # first run will take a normal amount of time
    pytest --keepdb src/tests/

    # subsequent runs will be faster since database will not need to be re-created
    pytest --keepdb src/tests/
    pytest --keepdb src/tests/
    ...

When your finished or if the model schema has changed, just run the following

    rm -rf .test_db

## Generating data sets

1. Edit `conf/perfdata.conf.json` to describe the data you want as a scenario (e.g. named `myscenario`).

2. Generate your scenarios

To create all scenarios, run the following

    python src/main.py generate

To generate only a single scenario (e.g. `myscenario`), just run:

    python src/main.py generate myscenario

## Performance testing

1. Edit `conf/processors.conf.json` to select how you want to process your data (e.g. use named configuration `large-chunks`)

2. Process the responses from JSON to the key value table

Run the following:

    python src/main.py process myscenario large-chunks

#### SQL Logging

If you need to see what SQLAlchemy is sending to Postgres for making optimization queries, do the following:

    python src/main.py --debug-sql process . . . 2>sql.log
    cat sql.log

#### Profiling for timing

Run the following to generate profiling output:

    python -m cProfile -o timing.stats src/main.py . . .

Then run the following to visualize the profiler output:

    gprof2dot -f pstats timing.stats | dot -Tpng > timing.png
    open timing.png

#### Profiling for memory

To see the high-level incremental memory usage at the processor level, use the `--debug-mem` option as follows:

    python src/main.py process --debug-mem . . .

If you need to see graphical output, run the following to generate the profiling output:

    mprof run python src/main.py . . .

Then run the following to visualize the output:

    mprof plot

Since `memory_profile` creates output files as (`mprofile_*.dat`), you can remove them all using:

    mprof clean

NOTE: Due to an [installation issue](http://stackoverflow.com/questions/21784641/installation-issue-with-matplotlib-python)
with the `matplotlib` pip package on OS X, please run the following once before the visualization step above:

    mkdir -p ~/.matplotlib
    echo 'backend: TkAgg' >> ~/.matplotlib/matplotlibrc


## Running the notebook

Start up Jupyter Notebook

    jupyter notebook

Open the `etl_workshop` notebook (`etl_workshop.ipynb`) and enjoy!
