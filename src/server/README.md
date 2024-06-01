### Use huecker.io !

https://huecker.io/use.html

### Build image

Need to be done after changing requirements.txt

`echo -e "AIRFLOW_UID=$(id -u)" > .env`

`docker-compose build`

### Init

echo -e "AIRFLOW_UID=$(id -u)" > .env

`docker-compose up airflow-init`

### Run

`docker-compose up`

access Airflow UI on http://0.0.0.0:8080/home

username/password: airflow

### Restart

`docker-compose down`
`docker-compose up`


