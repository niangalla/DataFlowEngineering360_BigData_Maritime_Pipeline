# DataFlowEngineering360 - Pipeline Big Data Maritime

![Architecture](architecture_dataflow360_final_paper_1764534128793.png)

## Présentation

**DataFlowEngineering360** est une plateforme complète de traitement de données conçue pour gérer à la fois des flux **batch** et **temps réel** dans le contexte maritime portuaire. Le projet simule un environnement professionnel de bout en bout, intégrant la génération, l’ingestion, le stockage, le traitement, le monitoring et l’exploitation des données du Port Autonome de Dakar.

Ce projet a été réalisé par **Alla NIANG**, apprenant en Développement DATA à **ODC (Orange Digital Center), Promo 7**.

## État d'avancement

🚧 **Projet en cours de développement : 85%**

Le pipeline end-to-end est fonctionnel. Les travaux restants concernent principalement l'optimisation des transformations, l'enrichissement des dashboards et la mise en place complète du pipeline CI/CD.

## Objectifs du projet

- **Pipeline Hybride** : Créer un pipeline de données capable de traiter des flux batch (historiques) et streaming (temps réel).
- **Architecture Moderne** : Mettre en œuvre les bonnes pratiques du Data Engineering (Data Lake, Data Warehouse, ELT/ETL).
- **Infrastructure** : Conteneuriser l’architecture complète via Docker pour une portabilité maximale.
- **Analytique** : Fournir des outils de BI et de Data Science pour l'aide à la décision.

## Fonctionnalités clés

- **Génération de données** : Simulation de trafic maritime, données météorologiques et logistiques (Python, Faker).
- **Ingestion multiformat** : Support de fichiers CSV, JSON, Excel, XML, YAML et flux API.
- **Stockage hétérogène** :
    - **Data Lake** : HDFS (via Hadoop) pour le stockage brut.
    - **NoSQL** : MongoDB (documents), Cassandra (séries temporelles), Neo4j (graphes).
    - **Data Warehouse** : PostgreSQL pour les données structurées et modélisées (schéma en étoile).
- **Orchestration** : Apache Airflow pour la gestion des workflows batch.
- **Streaming** : Apache Kafka pour le traitement des événements en temps réel.
- **Monitoring** : Stack ELK (Elasticsearch, Logstash, Kibana) et Grafana pour la supervision de l'infrastructure et des flux.
- **Valorisation** : Dashboards interactifs pour le suivi des KPIs portuaires.

## Architecture Technique

Le projet est structuré de manière modulaire :

```
DataFlow_Engineering360/
├── 01_collecte/          # Scripts de collecte et génération de données
├── 02_source_donnees/    # Données brutes et sources
├── 03_ingestion/         # Pipelines d'ingestion (Kafka, Spark)
├── 04_stockage_structuration/ # Scripts d'initialisation des BDD
├── 05_orchestration_automatisation/ # DAGs Airflow
├── 06_integration_transformation/ # Scripts de transformation (Spark, SQL)
├── 07_securite_optimisation/ # Gestion de la sécurité et optimisations
├── 08_monitoring/        # Configuration ELK et Grafana
├── docker-compose*.yml   # Fichiers d'orchestration Docker (split par service)
├── scripts/              # Scripts utilitaires (start-all.sh, stop-all.sh)
└── ...
```

### Technologies utilisées

| Domaine             | Outils                         |
|---------------------|-------------------------------|
| **Langages**        | Python, SQL, Shell            |
| **Génération**      | Faker, Pandas, Requests       |
| **Ingestion**       | Apache Kafka, Spark Streaming |
| **Traitement**      | Apache Spark (PySpark)        |
| **Orchestration**   | Apache Airflow                |
| **Stockage**        | PostgreSQL, MongoDB, HDFS     |
| **Monitoring**      | Elasticsearch, Logstash, Kibana, Grafana |
| **Infrastructure**  | Docker, Docker Compose        |

## Installation et Démarrage

### Prérequis

- Docker et Docker Compose installés.
- Une machine avec suffisamment de RAM (recommandé : 16GB+) car la stack complète est conséquente.

### Installation

1.  Cloner le dépôt :
    ```bash
    git clone https://github.com/niangalla/DataFlowEngineering360_BigData_Maritime_Pipeline.git
    cd DataFlowEngineering360_BigData_Maritime_Pipeline
    ```

2.  Configurer l'environnement :
    - Copier le fichier `.env.example` (si présent) vers `.env` et ajuster les variables si nécessaire.

### Lancement

Le projet utilise des scripts pour faciliter le démarrage des nombreux services :

```bash
# Démarrer tous les services
./start-all.sh

# Arrêter tous les services
./stop-all.sh
```

Vous pouvez également lancer des modules spécifiques via Docker Compose :

```bash
docker-compose -f docker-compose.core.yml up -d
docker-compose -f docker-compose.airflow.yml up -d
# ... autres fichiers compose
```

## Auteur

**Alla NIANG**
- **Email** : niangalla98@gmail.com
- **Formation** : Développement Data, Orange Digital Center (Promo 7)

---
*Ce projet est réalisé dans un but pédagogique et de démonstration de compétences en Data Engineering.*
