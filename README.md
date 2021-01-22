## TrueFilm

<p align="left">
        <img src="https://img.shields.io/github/v/tag/AleNegrini/TrueFilm"/>
        <img src="https://img.shields.io/github/workflow/status/AleNegrini/TrueFilm/cicd_truefilm"/>
</p>

`TrueFilm` is not a 

## Run

### Requirements

## What's behind the hood

If you're curious about how the `TrueFilm` job has been implemented, in this sections you'll find all the details
regarding technologies, frameworks, ...  

### Technology
- the solution is based on `Python` powered by the `PySpark` framework.
- 

### Tests
Apart from the code, in this repo you can also find a set of `unit tests` and `integration tests`:
- ``unit tests``: 22 tests checking most of the job functions and methods

You can manually run the unit tests by typing:
```
./run-tests.sh
...
test_spark_configurations (test.utils.test_spark.TestSpark) ... ok
test_spark_session_instance_type (test.utils.test_spark.TestSpark) ... ok
----------------------------------------------------------------------
Ran 22 tests in 66.246s

OK

```
- `integration tests` since the program is based on the integration of PostreSQL, it is a best practice testing the 
interoperability between the two. 

You can manually run the integration tests by typing:
```
./run-tests-it.sh
... 
----------------------------------------------------------------------
Ran 1 test in 10.986s

OK
```

### CICD

Everytime you push a commit in the ``develop`` branch, a `Github workflow` is triggered
and a build pipeline starts. 
The outcome of the workflow pipeline can che checked in the badge at the beginning of this README.

