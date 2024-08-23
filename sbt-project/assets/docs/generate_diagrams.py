import os
import subprocess

output_dir = "img"
os.makedirs(output_dir, exist_ok=True)

def generate_mermaid_image(code, output_file):
    with open("temp.mmd", "w") as file:
        file.write(code)
    subprocess.run(["mmdc", "-i", "temp.mmd", "-o", output_file])
    os.remove("temp.mmd")

# Codes Mermaid.js pour les diagrammes
diagrams = {
    "cluster_architecture": """
    graph LR
        A[Spark Master Node] --> B[Spark Worker Node 1]
        A --> C[Spark Worker Node 2]
        A --> E[HDFS NameNode]
        E --> F[HDFS DataNode 1]
        E --> G[HDFS DataNode 2]
        A --> I[YARN ResourceManager]
        I --> J[YARN NodeManager 1]
        I --> K[YARN NodeManager 2]
        A --> M[PostgreSQL Database]
    """,
    "configuration_diagram": """
    flowchart TD
        subgraph Hadoop
            core[core-site.xml]
            hdfs[hdfs-site.xml]
            mapred[mapred-site.xml]
            yarn[yarn-site.xml]
            docker_compose[docker-compose.yml]
        end
        subgraph Spark
            env[spark-env.sh]
            defaults[spark-defaults.conf]
            docker_compose[docker-compose.yml]
        end
        subgraph PostgreSQL
            docker_compose[docker-compose.yml]
        end
    """,
    "execution_workflow": """
    sequenceDiagram
        participant User
        participant SparkMaster as Spark Master
        participant SparkWorker as Spark Worker Nodes
        participant HDFS as HDFS
        participant PostgreSQL as PostgreSQL
        User->>SparkMaster: Soumettre Application
        SparkMaster->>SparkWorker: Distribut Taches
        SparkWorker->>HDFS: Lire/Ecrire Données
        SparkWorker->>PostgreSQL: Requête/Mise à jour Données
        SparkWorker->>SparkMaster: Envoyer Resultats
        SparkMaster->>User: Retourner Resultats Finals
    """,
    "troubleshooting_diagram": """
    graph TD
        A[Problème] --> B[Le cluster ne démarre pas]
        B --> C[Conteneurs non démarrés?]
        C --> |Oui| D[Vérifier `docker-compose up`]
        C --> |Non| E[Problème de connectivité réseau?]
        E --> |Oui| F[Revérifier la configuration réseau]
        E --> |Non| G[Consulter les logs Docker]
        B --> H[Problèmes de performance]
        H --> I[Allouer plus de mémoire/CPU dans les fichiers de configuration]
    """
}

for name, code in diagrams.items():
    output_path = os.path.join(output_dir, f"{name}.png")
    generate_mermaid_image(code, output_path)
    print(f"Diagramme généré: {output_path}")
