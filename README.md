## TrueFilm

<p align="left">
        <img src="https://img.shields.io/github/v/tag/AleNegrini/TrueFilm"/>
        <img src="https://img.shields.io/github/workflow/status/AleNegrini/TrueFilm/cicd_truefilm"/>
</p>

`TrueFilm` is an insteresting challenge that embraces all the skills a modern Data Engineer should have:
* software engineering skills
* (big) data frameworks' knowledge
* software testing skills
* DB knowledge
* software automation and virtualization knowledge
* problem solving skills

Beyond the technical issues, another critical aspect is ``time``: at the beginning you're asked to provide 
an estimation of the time needed to solve the problem: it could be a way to measure how good you are at estimating 
projects.

Unfortunately, I underestimated the time needed: I didn't enough time to properly packetize and virtualize as I
expected: I preferred to stay on deadlines at the expense of a poorer automation part (you'll find further details in
the next section). 

## Run

The solution in place is based on a `bash` script, called `run-script.sh` and it orchestrates a few steps. 
Before running it, you should make sure the following requirements are satisfied. 

### Requirements
* [docker](https://www.docker.com/products/docker-desktop)
* [docker-compose](https://docs.docker.com/compose/install/)
* [brew](https://brew.sh/)
* [virtualenv]()

### Run the script
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

This script should set the baseline for the program:
1) installs a bunch of propedeutical packages
2) setups the python virtualenv
3) starts three docker containers using the `docker-compose.yml` file (further details below)
4) downloads the source data from the clouds
5) finally, it runs the job

As said before, the process allows to reproduce and run automatically job, could be improved, using a more
robust approach. 
Instead of having a bash script, the pyspark application could be dockerized (using a `Dockerfile`) and orchestrated 
via `docker-compose` as did for the postreSQL server and clients. 
I would have followed this way, if I had had more time. 

### Run Tests

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

### Getting access to the data

Once the job ends, you can check the results in two ways: 
1) directly connecting to the postreSQL production container
2) using the postreSQL client directly on your computer

####Connecting to the container

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

#### Using the adminer local client

Among the containers run by the `./run-script.sh`, there is a DB client that can be reached via browser at [127.0.0.1:8080](http://localhost:8080/). 
![DB Intro](resources/db_intro.png?raw=true "Login")

You can find the password in clear inside the `database.env`

Once you're in, you should view something like this

![DBin](resources/db_tables.png?raw=true "tables")

### What's behind the hood

#### Technology
- the solution is based on `Python` powered by the `PySpark` framework.
- `docker` and `docker-compose` for the virtualization and the container "orchestration" part
- `bash` for gluing everything 
- `Python unittest` for testing the functions' correctness

#### CI/CD

Whenever you push a commit directly to the ``develop`` branch (or you open a PR on develop), a `Github workflow` is 
triggered.   
The outcome of the workflow pipeline can be checked in the badge at the beginning of this README.

## Considerations

### Datasources
You may have noted that the datasources downloaded and used throughout the entire job are different from the original
ones. 
They both were in a text format (.csv and .xml), that's why I decided to preprocess and save them in a binary and 
columnar format 
(Parquet).

In addition to that, the wikipedia file was pretty big (uncompressed it was near to 7GB) and many of the columns were not
useful. 
The new wikipedia file is 10x smaller (near to 700MB). 

The two new sources were then uploaded on dropbox, so that they can be easily shared and downloaded (uploading data on Github
is not recommended). 

### Rules

#### Movie title
In order to increase the chances of the join between metadata and wikipedia datasets, the titles were standardized applying
the following transformations on both sources: 
1) make each letter lower
2) remove punctuation characters
3) remove double spaces 
4) titles were finally trimmed

#### Money columns: budget and revenues
As far as I know, both the budgets and the revenues of movies are in the order of millions. 
However some movies contain too low values that would have generated not reliable ``budget_revenue_ratio``. 
I therefore decided to remove all the movies having the budget less than 1M$ or revenues less than 1M$. 

#### Matching rules
The matching logic I implemented were fairly easy, and articulated in three phases:
1) a few wikipedia rows have a title that includes the ``film`` keyword and the `year`,
in the form of ``<title> (<year> film)``. These are the rows I used in this first step of the join. 
Movies metadata and movies info are joined on the pair of fields `[title, year]`
2) some other titles in the wikipedia file, are in the format of  ``<title> (film)`` and therefore
joined using the `[title]` field only
3) the rest of the wikipedia movies (both without 'film' and 'year' tag) are finally joined with the remaining 
ones on `[title]` again 

Obviously, the way I dealt with budget and revenue columns and the matching logic is quite rudimental, 
and the more time you have the more sophisticated the logic can become. 
I just wanted to give a first idea and to get a first result. 
