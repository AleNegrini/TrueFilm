# TrueFilm

<p align="left">
        <img src="https://img.shields.io/github/v/tag/AleNegrini/TrueFilm"/>
        <img src="https://img.shields.io/github/workflow/status/AleNegrini/TrueFilm/cicd_truefilm"/>
</p>

### Requirements

Before running the job, please make sure your environment satisfies the following requirements:

- `Docker` ([download link](http://github.com))
- `Docker-compose` ([download link](http://github.com))

## Running mode

### Run the job

### Get Access to the data

Three are the containers started by launching the `docker-compose`.
If you want to check their details, just type:
```
docker ps
```

(*) container ids could be different 

#### Dev DB
```
docker exec -it <container_name> bash
```

Connect to local database instace using the `psql` client:
```
psql --host=localhost --user= --db=
```

#### Prod DB

```
docker exec -it <container_name> bash
```



### Deploy

### Tests

### References
