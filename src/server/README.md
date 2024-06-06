### Init

`echo -e "AIRFLOW_UID=$(id -u)" > .env`

`docker-compose up airflow-init`

### Run

`docker-compose up --build`

access Airflow UI on http://0.0.0.0:8080/home

username/password: airflow

access Kafka UI on http://0.0.0.0:8090/

kafka brocker `kafka:29092`

zookeeper `zookeeper:2181`

### Restart

`docker-compose down`
`docker-compose up --build`

## Access to PgAdmin

http://localhost:5050

## Add a new server in PgAdmin

* Host name/address `greenplum` (as Docker-service name)
* Port `5432` (inside Docker)
* Maintenance database `gpdb` (as `POSTGRES_DB`)
* Username `greenplum` (as `POSTGRES_USER`)
* Password `greenplum` (as `POSTGRES_PASSWORD`)

## Explore volumes

### List all volumes

```shell
docker volume ls
```

### Delete specified volume

```shell
docker volume rm habr-pg-16_habrdb-data
docker volume rm habr-pg-16_pgadmin-data
```