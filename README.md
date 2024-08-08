### ATOS - STAGE DATA ENGINEERING

# PROJET 1 - Présentation d'Apache Spark / Configuration et Installation d'un Cluster Big Data (Hadoop, Spark, Hive, Postgres et Jupyter)
**Date:** 19/07/2024 - 15H00 GMT  
**Auteur:** Mouhamadou Adji BARRY

---

## Introduction
Ce projet consiste en la création et la configuration d'un cluster Big Data comprenant Hadoop, Spark, Hive, Postgres et Jupyter. Le but est de fournir un environnement complet pour le traitement et l'analyse des données, englobant le stockage, le traitement et l'analyse.

## Services

| Service         | Composant                       | Description                                                       | URL                           |
|-----------------|---------------------------------|-------------------------------------------------------------------|-------------------------------|
| **Hadoop**      |                                 |                                                                   |                               |
| namenode        | NameNode                        | Gestion des métadonnées                                           | http://localhost:9870, http://localhost:9010 |
| datanode        | DataNode                        | Stockage des données                                              | http://localhost:9864         |
| resourcemanager | ResourceManager                 | Gestion des ressources                                            | http://localhost:8088         |
| nodemanager1    | NodeManager                     | Gestion des conteneurs sur un noeud                               | http://localhost:8042         |
| historyserver   | HistoryServer                   | Interface utilisateur de l'historique de l'application            | http://localhost:8188         |
| **Spark**       |                                 |                                                                   |                               |
| spark-master    | Spark Master                    | Gestion des ressources Spark                                      | http://localhost:8080, http://localhost:7077 |
| spark-worker-1  | Spark Worker                    | Exécution des tâches Spark                                        | http://localhost:8081         |
| spark-worker-2  | Spark Worker                    | Un autre Spark Worker                                             | http://localhost:8083         |
| **Hive**        |                                 |                                                                   |                               |
| hive-server     | Hive Server                     | Interface JDBC pour Hive                                          | http://localhost:10000        |
| hive-metastore  | Hive Metastore                  | Stockage des métadonnées                                          | http://localhost:9083         |
| hive-metastore-postgresql | PostgreSQL            | Base de données backend pour Hive Metastore                       | -                             |
| **Jupyter**     |                                 |                                                                   |                               |
| Jupyter         | Jupyter Notebook                | Environnement de notebook pour PySpark et l'analyse                | http://localhost:8888         |



# PROJET 2 - Rapport de Stage / Pipeline de traitement batch utilisant Apache Spark et Hive
**Date:** 08/08/2024 - 13H15 GMT  
**Auteur:** Mouhamadou Adji BARRY

---

## Rapport de Stage et Projet
Un dossier contenant le rapport de stage détaillé ainsi qu'un projet de pipeline de traitement par lots sur les données COVID-19 a été ajouté. Le rapport de stage documente l'ensemble des activités réalisées, les défis rencontrés, et les solutions mises en œuvre.
Le rapport de stage détaille les activités menées, les technologies utilisées, les défis rencontrés et les solutions mises en œuvre. Voici les points clés abordés dans le rapport :

### Table des Matières
1. Introduction
2. Objectifs du Stage
3. Présentation de l'Entreprise
4. Description du Poste et des Missions
5. Méthodologie
6. Analyse et Réflexion
7. Résultats
8. Conclusion
9. Perspectives et Recommandations
10. Annexes
11. Bibliographie

### Objectifs du Stage
- **Objectifs Généraux** : Acquérir une expérience pratique en Data Engineering et développer des compétences techniques dans le domaine du Big Data.
- **Objectifs Spécifiques** : Apprendre à utiliser Apache Spark, installer et configurer un cluster Big Data, et développer une pipeline de traitement batch.

### Méthodologie
- **Méthodes de travail** : Documentation et Recherche, Prototypage, Collaboration, Feedback Régulier.
- **Outils et technologies** : Apache (Spark, Hadoop, Hive et Airflow), Python, PostgreSQL, Git, Teams.
- **Processus de travail** : Suivi rigoureux des normes de qualité et des délais.

### Résultats
- **Cluster Big Data Fonctionnel** : Mise en place d'un cluster Big Data intégrant Hadoop, Spark, Hive et Jupyter sur Docker.
- **Pipeline de Traitement** : Développement et déploiement d'une pipeline de traitement batch utilisant Apache Spark et Hive.
- **Projets de Test Réussis** : Réalisation de plusieurs projets de test démontrant l'efficacité et la robustesse des configurations et pipelines développés.

### Conclusion
- **Expérience enrichissante** : Acquisition de nouvelles compétences et mise en pratique des connaissances.
- **Compétences développées** : Compétences techniques et analytiques améliorées, meilleure compréhension des concepts et des outils du Big Data.

### Perspectives et Recommandations
- **Perspectives professionnelles** : Familiarisation avec les technologies et les pratiques du Big Data, préparation à une carrière dans ce domaine.
- **Recommandations pour les futurs stagiaires** : Se familiariser avec les outils et technologies du Big Data avant de commencer le stage.

Pour plus de détails, veuillez consulter le [Rapport de Stage](https://github.com/Team-Data-Engineering-Stage-2024-Atos/Presentations/tree/barryma-rapport-stage1).

## Documentation (Work in progress)

### 1. Introduction
Décrire les objectifs et l'importance du projet.

### 2. Pré-requis
Liste des outils et logiciels nécessaires :
- Docker
- Hadoop
- Spark
- Hive
- PostgreSQL
- Jupyter
- Java (Temurin pour OpenJDK)

### 3. Configuration de l'Environnement
#### a. Configuration de Docker
- Installer Docker et Docker Compose.
- Télécharger et configurer les images Docker pour chaque composant.

#### b. Configuration des Services
- **Hadoop** : Configurer NameNode, DataNode, ResourceManager, NodeManager, et HistoryServer.
- **Spark** : Configurer Spark Master et Spark Workers.
- **Hive** : Configurer Hive Server, Hive Metastore, et PostgreSQL pour le metastore.
- **Jupyter** : Configurer Jupyter Notebook pour l'analyse PySpark.

### 4. Déploiement du Cluster
- Étapes pour démarrer chaque service.
- Vérification de la connectivité entre les services.

### 5. Traitement des Données
#### a. Ingestion des Données
- Ingestion des datasets COVID-19 dans HDFS.

#### b. Traitement par Lots avec Spark
- Nettoyage et transformation des données avec Spark.
- Agrégation et analyse des données.

### 6. Stockage et Analyse des Données
- Stockage des données transformées dans Hive.
- Exécution de requêtes HiveQL pour l'analyse des données.

### 7. Documentation et Rapport
- Rédaction du rapport de stage.
- Documentation du pipeline de traitement des données.

### 8. Sécurité et Gestion des Accès
- Mise en place de la sécurité des données.
- Gestion des accès et des rôles pour différents utilisateurs (admins, data scientists, data engineers, visiteurs).

### 9. Conclusion
Résumé des réalisations, des défis rencontrés, et des perspectives d'amélioration.

---

## Auteur
**Mouhamadou Adji BARRY** - Student

## Superviseur
**Ibrahima Oumar LY** - Data Engineer

---
