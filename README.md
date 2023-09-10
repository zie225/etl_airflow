

Créer un pipeline Extract Transform Load(ETL) à l'aide de python et automatiser avec airflow, prefect, kafka ou le deployer avec gitlab,github action,Jenkins pour le versioning.

Nous avons le fichier pollution_test.ipynb qui ne passe pas par la phase de l'automatisation, une des raisons que nous utilisons le pipeline etl à l'aide de script python pour rendre le pipeline scalable et maintainable.

![](image/chart.png)


Start a :
 - docker compose
 - Postgre SQL database (pg admin)

```shell
docker-compose -f docker-compose.yml up -d
```
down a :
  - docker
```shell
docker-compose down
```
postgreSQL database dans pg  admin

- user: airflow
- password: airflow

# postgresql docker compose
Le deploiement des conteneurs docker compose est requis pour avoir accès à pg admin et l'interface d'utilisateur d'apache airflow.

Tech Stack / Compétence utilisée :
1.  Python
2.  fichier CSV,shapefile et leur manipulation
3.  Docker
4.  PostgreSQL,Sqlite
5.  Apache Airflow 


Prérequis:

1. Connaissance de fichier CSV,shapefile et leur manipulation
2. Comprendre ce qu'est docker et docker-compose
3. Python et SQL intermédiaires
4. Une compréhension de base d'Airflow [cette video](https://www.youtube.com/watch?v=AHMm1wfGuHE&t=705s) ça peut aider.

## extract.py

Nous chargeons les données de pollution en CSV et de georeference en SHP.
Cette partie peut aussi etre utilisée pour extraire les données d'une base de donnée,d'un cloud( AWS, GCP,Microsoft AZURE) ou d'un API.

## transform.py

Ici, nous exportons le fichier d'extraction pour obtenir les données pour connaitre leur qualité et les prétraiter.

**def Data_Quality(load_df):** et **def Data_Quality_1(load_df_1):** Sont utilisés pour vérifier les données manquantes, appliquer des contraintes uniques, vérifier les valeurs nulles. Étant donné que ces données pourraient ruiner notre base de données, il est important que nous appliquions ces contrôles de qualité des données.

**def transform_df(load_df):** et **def transform_df_1(load_df_1):** Maintenant, nous écrivons une logique selon notre exigence ici. Nous pretraitons les données,nous supprimons les variables a valeurs manquantes.

## load_sqlite.py et load_postgres.py

Dans l'étape de chargement load, nous utilisons sqlalchemy, SQLite et postgreSQL pour charger nos données (ingestion de donnée) et enregistrer les données  dans la base de donnée postgresql.


Après avoir exécuté **load_sqlite.py** et **load_postgres.py**, 
- on peut voir qu'un fichier .sqlite est  dans le dossier du projet.pour vérifier les données à l'intérieur de l'en-tête du fichier, nous avons utiliser le notebook **pollution_test.ipynb** pour lire le ficher et avoir accès à la base donnée.

- on peut voir que les fichier CSV et SHP sont ingerés dans la base de donnée postgreSQL. 

## EXECUTION DE load_sqlite.py et load_postgres.py depuis la commande

```shell
cd Desktop
cd etl
python load_sqlite.py
python load_postgres.py

```

## RESULTAT DE SQLITE

![](image/sqlite_pollution.png)

![pollution data on SQLITE](image/sqlite_pollution1.png)

![](image/sqlite_georef.png)

![Georeference data on SQLITE](image/sqlite_georef1.png)

## RESULTAT DE PostgreSQL
![](image/pollution_postgres.png)

![](image/georeference_postgres.png)


## ORCHESTRATION: AUTOMATISATION AVEC AIRFLOW 

après avoir construit notre ETL, nous voulons l'automatiser par le biais d'apache airflow avec une orchestration.



Maintenant que nous avons configuré airflow et lancé le docker_compose, nous pouvons afficher l'interface utilisateur d'airflow en visitant le [port 8080] (http://localhost:8080/). Le nom d'utilisateur et le mot de passe seraient airflow.

Il est temps de créer le Dag requis pour notre projet. Mais avant de passer à DAG, comprenons ce que dag et DAG signifie Directed Acyclic Graph qui est un ensemble de tâches définies dans l'ordre d'exécution.

La construction du DAG est dans ce fichier:
```shell
etl_dags.py
```
Donc, à l'intérieur de notre dag, nous devons créer des tâches pour faire notre travail.

![](image/dags.png)


# Configuration de la connexion Postgres sur l'interface utilisateur Airflow :

![](image/connection.png)

1. **L'identifiant de connexion :** postgre_sql serait celui que nous avons utilisé dans notre code.
2. **Type de connexion :** Postgres
3. **Hôte :** Postgres
4. **Schéma :** airflow, vous pouvez utiliser votre propre nom, mais assurez-vous de vérifier dans la bonne base de données lors de la validation.
5. **Connexion :** airflow
6. **Mot de passe :** airflow
7. **Port :** 5432

![](image/connection_1.png)

# Resultat avec GRID et le LOG

1. GRID

![](image/grid.png)

Nous pouvons voir dans la partie entouré en noir le succès d'orchestration.

Il y'a succés quand il n'ya pas d'erreur dans le code et que l'execution est sans defaut.

Nous devons le verifier dans le LOG pour verifier les reponses de l'execution.


2. LOG

Ce resultat montre que la base donnée  de la  pollution a ete bien crée. 
S'il y'a des erreurs, nous pouvons le voir depuis log.

![](image/log_pollution.png)

Ce resultat montre que la la base donnée georeferencielle a ete bien crée. 

![](image/log_georef.png)

Ce resultat montre que l'ETL a été executé avec succès.
Pour la qualité de donnée, s'il y'a des valeurs manquantes on saura depuis le log.

![](image/log_etl.png)

## AFFICHAGE DE LA DATA DEPUIS L'ENVIRONNEMENT DOCKER

Pour avoir accès à la base de donnée posgresql,nous devons proceder comme suite:

```shell
docker container exec -it airflow_webserver bash

```


```shell
docker container exec -it airflow_webserver bash

```

```shell

python
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
pd.read_sql('SELECT * FROM pollution_data', engine)

pd.read_sql('SELECT * FROM georef_data', engine)

```
![](image/resultat_env_docker_poll_data.png)


![](image/resultat_env_docker_georef_data.png)


## AUTEUR

- [@Coulibaly Zie Mamadou](https://github.com/zie225)

