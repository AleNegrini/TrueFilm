## TrueFilm

<p align="left">
        <img src="https://img.shields.io/github/v/tag/AleNegrini/TrueFilm"/>
        <img src="https://img.shields.io/github/workflow/status/AleNegrini/TrueFilm/cicd_truefilm"/>
</p>

`TrueFilm` is an insteresting challenge that embraces all the skills a modern Data Engineer should have:
* software engineering skills
* (big) data framework knowledge
* software testing skills
* DB knowledge
* software automation and virtualization knowledge
* problem solving skills

Beyond the technical issues, there is also the limited ``time``.
At the beginning you're asked to provide an estimation about the time it will take it to be solved: it could be 
a way to measure how good you are estimating projects.


Unfortunately, I underestimated the time needed, since I didn't have time to properly packetize and virtualize as I
expected: I preferred to stay on deadlines at the expense of a poorer automation part (you'll find further details in
the next section). 

## Run

The solution in place is based on a `bash` script, called `run-script.sh` and it orchestrates a few steps. 
Before running it, you should make sure you the following requirements are satisfied. 

#### Requirements
* [docker](https://www.docker.com/products/docker-desktop)
* [docker-compose](https://docs.docker.com/compose/install/)
* [brew](https://brew.sh/)
* [virtualenv]()

#### Run the script
If requirements are met, just type:
```
./run-script.sh
```

The program logs something during its execution:
```
2021-01-22 18:38:33,391 - INFO - Initializing TrueFilm logging using verbosity level: 20
2021-01-22 18:38:33,391 - INFO - Detected environment variabile POSTGRES_HOST with value 127.0.0.1
2021-01-22 18:38:33,391 - INFO - Detected environment variabile POSTGRES_PORT with value 5432
21/01/22 18:38:34 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2021-01-22 18:38:37,013 - INFO - Spark Session has been successfully inizialized with log level WARN
2021-01-22 18:38:37,017 - INFO - Detected movies_metadata in folder resources/final
2021-01-22 18:38:37,017 - INFO - Detected enwiki-latest-abstract in folder resources/final
2021-01-22 18:38:37,017 - INFO - Detected enwiki-latest-abstract.xml in folder resources/raw
2021-01-22 18:38:37,017 - INFO - Detected enwiki-latest-abstract.xml in folder resources/raw
2021-01-22 18:38:37,017 - INFO - Detected a CLEANED version of movies dataset
2021-01-22 18:38:37,017 - INFO - Detected a CLEANED version of Wikipedia dataset
2021-01-22 18:38:37,017 - INFO - Detected movies_metadata in folder resources/final
2021-01-22 18:38:37,017 - INFO - Detected enwiki-latest-abstract in folder resources/final
2021-01-22 18:38:39,076 - INFO - Dataframe has been correctly read from resources/final/movies_metadata (PARQUET data format)
2021-01-22 18:38:39,241 - INFO - Dataframe has been correctly read from resources/final/enwiki-latest-abstract (PARQUET data format)
21/01/22 18:38:41 WARN Utils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.debug.maxToStringFields' in SparkEnv.conf.
2021-01-22 18:38:43,792 - INFO - PRE-JOIN: Movies dataframe counts a total of 1000 rows
2021-01-22 18:38:44,049 - INFO - PRE-JOIN: Wikipedia dataframe counts a total of 6222442 rows
2021-01-22 18:38:49,300 - INFO - POST-JOIN-1: TrueFilm matched a total of 194 rows
2021-01-22 18:39:03,595 - INFO - POST-JOIN-2: TrueFilm matched a total of 258 rows
2021-01-22 18:39:37,494 - INFO - POST-JOIN-3: TrueFilm matched a total of 503 rows
2021-01-22 18:40:21,648 - INFO - Dataframe has been successfully saved to movies
2021-01-22 18:40:22,429 - INFO - Spark Session has been successfully stopped
```

This script should put the baseline for the program:
1) installs a bunch of propedeutical packages
2) setups the python virtualenv
3) starts three docker containers using the `docker-compose.yml` file (further details below)
4) downloads the source data from the clouds
5) finally, it runs the job

As said before, the process allowing to reproduce and to run automatically job, could be improved, using a more
robust approach. 
Instead of having a bash script, the pyspark application could be dockerized (using a `Dockerfile`) and orchestrated 
via `docker-compose` as did for the postreSQL server and clients. 
I would have followed this way, if I had had more time. 

#### Run Tests

Apart from running the code, you can also find a set of **unit tests** and **integration tests**.

**Unit Tests**
- ``unit tests``: 22 tests checking most of the job functions and methods

```
./run-tests.sh
...
test_spark_configurations (test.utils.test_spark.TestSpark) ... ok
test_spark_session_instance_type (test.utils.test_spark.TestSpark) ... ok
----------------------------------------------------------------------
Ran 22 tests in 66.246s

OK

```

**Unit Tests**
- ``unit tests``: 1 tests checking whether the two-way communication works or not.

```
./run-tests-it.sh
... 
----------------------------------------------------------------------
Ran 1 test in 10.986s

OK
```

Attention: before running integration tests, make sure the postreSQL dev container is up and running. 

#### Getting access to the data

Once the job ends its execution, you can check the results in two ways: 
1) directly connect to the postreSQL production container
2) use the postreSQL client directly on your computer

**Connect to the container**
Get the container id:
```
alessandro.negrini@MBPdiAlessandro TrueFilm (develop) $ docker ps
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                    NAMES
e0424c051f11        adminer             "entrypoint.sh docke…"   25 minutes ago      Up 25 minutes       0.0.0.0:8080->8080/tcp   truefilm_adminer_1
57fc65039844        postgres:alpine     "docker-entrypoint.s…"   4 hours ago         Up 44 minutes       0.0.0.0:5432->5432/tcp   truefilm_database_prod_1
10e3fe0da8ec        postgres:alpine     "docker-entrypoint.s…"   4 hours ago         Up 44 minutes       0.0.0.0:5433->5432/tcp   truefilm_database_dev_1
```

Look for the ID of the container with name 'truefilm_database_prod_1', in this case `57fc65039844`

Log into
```
alessandro.negrini@MBPdiAlessandro TrueFilm (develop) $ docker exec -it 57fc65039844 bash
bash-5.1#
```

Connect to the DB using the `psql` client
```
bash-5.1# psql --host=localhost --username=truefilmuser --dbname=truelayer_prod
psql (13.1)
Type "help" for help.

truelayer_prod=# 
```

Now you are ready to query your data as you prefer. 
```
truelayer_prod=# \dt
           List of relations
 Schema |  Name  | Type  |    Owner     
--------+--------+-------+--------------
 public | movies | table | truefilmuser
(1 row)

```
```
truelayer_prod=# select count(*) from movies;
 count 
-------
  1000
(1 row)
```

**Use the adminer local client**
Among the containers run by the `./run-script.sh`, there is a DB client that you can reach via browser at [127.0.0.1:8080](http://localhost:8080/). 
![DB Intro](resources/db_intro.png?raw=true "Login")

You can find the password in clear inside the `database.env`

Once you're in, you should view something like this

![DBin](resources/db_tables.png?raw=true "tables")


## What's behind the hood

If you're curious about how the `TrueFilm` job has been implemented, in this sections you'll find all the details
regarding technologies, frameworks, and so on. 

### Technology
- the solution is based on `Python` powered by the `PySpark` framework.
- virtualization tool 



### CICD

Everytime you push a commit in the ``develop`` branch, a `Github workflow` is triggered
and a build pipeline starts. 
The outcome of the workflow pipeline can che checked in the badge at the beginning of this README.

## Final consideration

### Datasources
- The original datasources (especially the wikipedia one) was quite big (uncompressed was near to 6GB). 
Therefore I opted to do a first preprocessing of the datasetsdoc, that implied: 
    - format homogenity: both sources have been converted to the `.parquet` format for better performances after
    - remove useless columns