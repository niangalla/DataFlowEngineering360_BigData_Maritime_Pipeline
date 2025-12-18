# Bilan de Projet - Orange Digital Center

---

## I. Informations Générales

| Champ | Réponse |
|-------|---------|
| **1. Nom et prénom** | *Alla NIANG* |
| **2. Email** | *niangalla98@gmail.com* |
| **3. Numéro de téléphone** | *77 503 09 91* |
| **4. Promotion / Filière** | Développement Data P7 |

---

## II. Data Flow

### 7. Titre du projet
**DataFlow Engineering 360 - Pipeline Big Data pour l'Analyse du Trafic Portuaire**

---

### 8. Description brève
DataFlow Engineering 360 est une plateforme complète de Data Engineering simulant le trafic du Port Autonome de Dakar. Le projet vise à construire un pipeline de données end-to-end, de la collecte à la visualisation, en utilisant des technologies Big Data modernes. Il répond au besoin de centraliser, transformer et analyser des flux de données hétérogènes (navires, terminaux, météo) provenant de sources multiples (MySQL, MongoDB, APIs). Les utilisateurs cibles sont les analystes data, les gestionnaires portuaires et les décideurs souhaitant suivre les KPIs du port (volumes cargo, délais, conditions météo). Ce projet s'inscrit dans un contexte de formation en Data Engineering à l'Orange Digital Center.

---

### 9. Objectifs principaux
- Maîtriser l'architecture d'un Data Lake (zones Bronze/Silver/Gold) et d'un Data Warehouse (modèle en étoile)
- Implémenter des pipelines d'ingestion batch et streaming
- Orchestrer des workflows ETL/ELT complexes avec Airflow
- Mettre en place un monitoring temps réel avec la stack ELG (Elasticsearch, Logstash, Grafana)
- Conteneuriser l'infrastructure avec Docker pour une reproductibilité maximale

---

### 10. Problème ou besoin résolu
Le projet répond à la problématique de gestion et d'analyse de données portuaires dispersées dans plusieurs systèmes (bases relationnelles, NoSQL, fichiers). Il permet de :

- Centraliser les données dans un Data Lake unifié
- Automatiser les transformations et agrégations
- Fournir des dashboards décisionnels en temps réel
- Assurer la traçabilité et la qualité des données

---

### 11. Technologies et méthodes utilisées

| Domaine | Technologies |
|---------|-------------|
| **Collecte** | Python, Faker, APIs REST, Web Scraping |
| **Stockage** | MySQL, MongoDB, HDFS (Hadoop), PostgreSQL |
| **Ingestion** | Apache Kafka (streaming), Spark (batch) |
| **Transformation** | PySpark, modèle dimensionnel en étoile |
| **Orchestration** | Apache Airflow  |
| **Monitoring** | Stack ELG (Elasticsearch, Logstash, Grafana) |
| **DevOps** | Docker Compose (15+ services), scripts Shell, gestion des secrets, GitHub Actions (CI/CD) |

---

### 12. Principales difficultés rencontrées
- Synchronisation des données entre les zones HDFS et le Data Warehouse
- Gestion des schémas incohérents entre les sources (ex: codes terminaux vs noms)
- Configuration des healthchecks et dépendances entre services Docker
- Résolution des problèmes de jointures Spark (clés étrangères null, dates non alignées)
- Débogage des DAGs Airflow et gestion des échecs de tâches

---

### 13. Résultats et livrables produits
- Pipeline ETL/ELT complet fonctionnel (collecte → ingestion → transformation → warehouse)
- DAGs Airflow opérationnels pour l'orchestration
- Data Warehouse PostgreSQL avec 4 dimensions + 1 table de faits
- Infrastructure Docker Compose séparée sur des fichiers YAML avec au total 15+ services conteneurisés
- Dashboards Grafana pour le suivi des KPIs portuaires
- Scripts d'automatisation (start-all.sh, stop-all.sh)

---

### 14. État d'avancement actuel
**85%** - Le pipeline end-to-end est fonctionnel. Reste à finaliser l'optimisation des transformations et enrichir les dashboards Grafana.

---

### 15. Prochaines étapes prévues
- **DevOps & CI/CD** : Mise en place d'un pipeline CI/CD avec **GitHub Actions** pour automatiser les tests, le build et le déploiement
- Ajouter des tests unitaires et d'intégration (optionnel)
- Optimiser les performances Spark (partitionnement, caching)
- Enrichir les dashboards **Grafana** de la stack **ELG** pour le monitoring avancé
- Implémenter un système d'alerting automatisé avec **Prometheus** et **Grafana**
- Faire du Machine Learning sur les données portuaires pour prédire les retards des navires (optionnel)
- Documenter l'architecture et les APIs
- Déployer sur un cluster cloud (AWS/GCP)

---

## III. Profil de Sortie (Préparation du Bilan Annuel)

### 16. Profils choisis (par ordre de priorité)

| Priorité | Profil |
|----------|--------|
| 1 | **Data Engineer** |
| 2 | **DevOps/MLOps Engineer** |
| 3 | **Data Scientist** |

---

### 17. Pourquoi Data Engineer en priorité n°1 ?
Ce projet représente l'essence même du métier de Data Engineer : concevoir des pipelines de données robustes, orchestrer des workflows ETL complexes, gérer des infrastructures Big Data distribuées et assurer la qualité des données. C'est le profil qui correspond le plus à mes compétences développées et à mes aspirations professionnelles dans le court terme.

---

### 18. Compétences déjà acquises
- Conception d'architectures Data Lake/Data Warehouse
- Design d'architectures End-to-End
- Maîtrise de Spark, Kafka, Airflow, HDFS
- Conteneurisation avec Docker et Docker Compose
- Modélisation dimensionnelle (schéma en étoile)
- Scripting Python et Shell pour l'automatisation
- Monitoring et observabilité (ELG Stack)
- Bases en Machine Learning et Deep Learning (profil Data Scientist)

---

### 19. Compétences à développer
- Déploiement sur le cloud (AWS EMR, GCP Dataproc, Azure Data Factory)
- Infrastructure as Code (Terraform, Ansible)
- CI/CD pour pipelines de données (GitHub Actions)
- Optimisation avancée Spark (tuning, Catalyst optimizer)
- Data Governance et Data Quality (Great Expectations, dbt)
- MLOps : déploiement et monitoring de modèles ML en production
- Streaming avancé (Kafka Streams, Flink)

---

## IV. Sujet de Mémoire

### 25. Souhaitez-vous garder le sujet par défaut (Data Flow) ?
**Oui** je garde le sujet DataFlow Engineering 360 pour le moment dans l'attente d'une opportunité de stage.

---

### 26. Sujet de mémoire alternatif
*Non applicable* (je garde le sujet DataFlow)

---

### 27. Pertinence du sujet
Le projet **DataFlow Engineering 360** constitue un sujet de mémoire pertinent car il couvre l'ensemble du cycle de vie des données en entreprise : collecte multi-sources, ingestion batch/streaming, transformation, entreposage et visualisation. Ce projet reflète les besoins réels des entreprises en matière de gestion de données massives et permet de démontrer une maîtrise concrète des technologies Big Data les plus demandées sur le marché (Spark, Kafka, Airflow, HDFS). Dans l'attente de trouver un stage, ce projet me permet de construire un portfolio solide et d'approfondir mes compétences en Data Engineering tout en restant ouvert à adapter le mémoire si une opportunité de stage se présente.

---

## Stack Technique Complète
Technologies | Utilisation
--- | ---
Python | Programmation
Apache Spark | Traitement des données
Kafka | Streaming
Airflow | Orchestration
HDFS | Stockage Hadoop (Data Lake)
PostgreSQL | Stockage (Data Warehouse)
MySQL | Stockage
MongoDB | Stockage
Docker | Conteneurisation
Elasticsearch | Moteur de recherche
Logstash | Collecte et traitement des logs
Grafana | Visualisation
GitHub Actions | CI/CD
Shell | Automatisation

