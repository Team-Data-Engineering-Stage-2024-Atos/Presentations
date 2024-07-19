# ATOS - STAGE DATA ENGINEERING

## PROJECT 1 - Présentation d'Apache Spark / Configuration et Installation d'un cluster Big Data (Hadoop, Spark, Hive, Postgres et Jupyter)
## 19/07/2024 - 15H00 GMT
## Mouhamadou Adji BARRY

Un cluster big data utilisant Hadoop, Hive (Postgres comme metastore), Spark et Jupyter; conçue pour fournir un environnement complet pour le traitement des données et l'analytique, y compris le stockage, le traitement et l'analyse des données.


## Services

| Service | Composant | Description | URL |
|---------|-----------|-------------|-----|
| **Hadoop** | | | |
| | `namenode` | NameNode, gestion des métadonnées. | http://localhost:9870, http://localhost:9010 |
| | `datanode` | DataNode, stockage des données. | http://localhost:9864 |
| | `resourcemanager` | ResourceManager, gestion des ressources. | http://localhost:8088 |
| | `nodemanager1` | NodeManager, gestion des conteneurs sur un noeud. | http://localhost:8042 |
| | `historyserver` | HistoryServer, fournit l'interface utilisateur de l'historique de l'application. | http://localhost:8188 |
| **Spark** | | | |
| | `spark-master` |  Spark Master, gére les ressources Spark. | http://localhost:8080, http://localhost:7077 |
| | `spark-worker-1` | Spark Worker, execute les taches Spark. | http://localhost:8081 |
| | `spark-worker-2` | Un autre Spark Worker. | http://localhost:8083 |
| **Hive** | | | |
| | `hive-server` | Hive Server, fournit une interface JDBC | http://localhost:10000 |
| | `hive-metastore` | Hive Metastore, stocke les métadonnées | http://localhost:9083 |
| | `hive-metastore-postgresql` | PostgreSQL pour Hive Metastore, base de données backend. | - |
| **Jupyter** | | | |
| | `Jupyter` | Jupyter Notebook pour PySpark et l'analyse. | http://localhost:8888 |

## Author
BARRY, Mouhamadou Adji - Student

## Supervisor
LY, Ibrahima Oumar - Data Engineer