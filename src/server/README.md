### Use huecker.io !

https://huecker.io/use.html

### Build image

Need to be done after changing requirements.txt

`docker-compose build`

### Init

`echo -e "AIRFLOW_UID=$(id -u)" > .env`

`docker-compose up airflow-init`

### Run

`docker-compose up`

access Airflow UI on http://0.0.0.0:8080/home

username/password: airflow

access Kafka UI on http://0.0.0.0:8090/

kafka brocker `kafka:29092`

zookeeper `zookeeper:2181`

### Restart

`docker-compose down`
`docker-compose up`

