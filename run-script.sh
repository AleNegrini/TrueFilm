#!/usr/bin/env bash

function installPckgs() {
    brew install wget
    brew install unzip
    brew install python@3.7
    export PATH="/usr/local/opt/python@3.7/bin:$PATH"
    VENV=.penv/pyvenv.cfg
    if test -f "$VENV"; then
        echo "Virtualenv already exists."
    else
        virtualenv --python=python3.7 .penv
    fi
    source .penv/bin/activate
}

function runDockerCompose() {
    docker-compose up --detach
}

function installRequirements() {
    if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
}

function getMoviesData() {
    echo 'Downloading movies data'
    mkdir resources/final
    cd resources/final
    MOVIES=movies_metadata/_SUCCESS
    if test -f "$MOVIES"; then
        echo "$MOVIES exists."
    else
        wget https://www.dropbox.com/s/a2g4uti1xus69b9/movies_metadata.zip
        unzip movies_metadata.zip
        rm movies_metadata.zip
    fi
}

function getWikiData() {
    echo 'Downloading wiki data'
    WIKI=enwiki-latest-abstract/_SUCCESS
    if test -f "$WIKI"; then
        echo "$WIKI already exists."
    else
        wget https://www.dropbox.com/s/tcu01c7d0aflbr6/enwiki-latest-abstract.zip
        unzip enwiki-latest-abstract.zip
        rm enwiki-latest-abstract.zip
    fi
}

function runJob(){
    cd ../..

    echo 'Setting the environment variables'
    export POSTGRES_HOST='127.0.0.1'
    export POSTGRES_PORT='5432'

    echo 'Running the job'
    cp src/truefilm/run.py .
    python run.py local[*] truefilm 20 movies_metadata enwiki-latest-abstract
    rm ./run.py
}

installPckgs
runDockerCompose
installRequirements
getMoviesData
getWikiData
runJob
