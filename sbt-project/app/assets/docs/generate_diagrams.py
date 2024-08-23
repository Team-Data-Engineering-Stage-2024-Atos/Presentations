import os

def generate_mermaid_file(filename, content, scale=6, quality=120):
    mmd_path = os.path.join("graph/", f"{filename}.mmd")
    png_path = os.path.join("img/", f"{filename}.png")
    svg_path = os.path.join("img/", f"{filename}.svg")
    
    with open(mmd_path, "w") as file:
        file.write(content)
    
    os.system(f"mmdc -i {mmd_path} -o {png_path} -s {scale} -q {quality}")
    os.system(f"mmdc -i {mmd_path} -o {svg_path}")

# Style Mermaid pour augmenter la taille des boîtes et du texte
common_css = """
%%{init: {'themeVariables': {'primaryColor': '#ffcc00', 'edgeLabelBackground':'#ffffff', 'tertiaryColor': '#ffffff', 'fontSize': 24}}}%%
"""

main_architecture = """
flowchart TB
    subgraph Data_Sources ["Sources de Données"]
        direction LR
        A1[ecommerce-data.csv]
        A2[ccdata.csv]
        A3[amazon-product-listing-data.csv]
    end

    subgraph Data_Storage ["Stockage des Données"]
        B[HDFS]
    end

    subgraph Processing ["Traitement"]
        C1[Spark Cluster]
        C2[DataFusion Pipelines]
    end

    subgraph Data_Warehouse ["Entrepôt de Données"]
        D[PostgreSQL]
    end

    A1 -->|Stocker les Données Brutes| B
    A2 -->|Stocker les Données Brutes| B
    A3 -->|Stocker les Données Brutes| B

    B -->|Charger les Données| C1
    C1 -->|Traiter les Données| C2
    C2 -->|Stocker les Résultats| D
"""

anomaly_detection_pipeline = common_css + """
flowchart LR
    A([Chargement des Données Clients])
    B([Filtrer: CASH_ADVANCE > 0 ET BALANCE > CREDIT LIMIT])
    C([Sélectionner: CustomerID, BALANCE, CREDIT LIMIT, CASH ADVANCE])
    D([Sortie: Anomalies Détectées])

    A -->|Charger les Données| B -->|Appliquer le Filtre| C -->|Préparer les Données| D
"""

cltv_pipeline = common_css + """
flowchart LR
    A([Chargement des Données de Vente])
    B([Chargement des Données Clients])
    C([Agrégation par CustomerID])
    D([Calcul: TotalQuantity & TotalRevenue])
    E([Jointure avec les Données Clients])
    F([Calcul du CLTV])
    G([Sortie: CLTV par Client])

    A -->|Charger les Ventes| C --> D --> E
    B -->|Charger les Données Clients| E --> F --> G
"""

product_performance_pipeline = common_css + """
flowchart LR
    A([Chargement des Données de Vente])
    B([Chargement des Données Produits])
    C([Jointure sur StockCode/Sku])
    D([Agrégation: Prix Moyen, Ventes Totales, Notation Amazon])
    E([Tri par Ventes Totales])
    F([Sortie: Performance des Produits])

    A -->|Charger les Ventes| C
    B -->|Charger les Infos Produits| C --> D --> E --> F
"""

repeat_purchase_pipeline = common_css + """
flowchart LR
    A([Chargement des Données de Vente])
    B([Groupement par CustomerID])
    C([Calcul: Achats Totals & Quantité Totale])
    D([Tri par Achats Totals])
    E([Sortie: Achats Répétés])

    A -->|Charger les Données| B --> C --> D --> E
"""

sales_analysis_pipeline = common_css + """
flowchart LR
    A([Chargement des Données de Vente])
    B([Groupement par StockCode])
    C([Calcul: Quantité Totale, Revenus Totaux])
    D([Tri par Revenus Totaux])
    E([Sortie: Analyse des Ventes])

    A -->|Charger les Données| B --> C --> D --> E
"""

# Générer le schéma d'architecture avec les paramètres par défaut
generate_mermaid_file("main_architecture", main_architecture, scale=2, quality=120)

# Générer les pipelines avec un facteur de mise à l'échelle plus grand et une police augmentée
generate_mermaid_file("anomaly_detection_pipeline", anomaly_detection_pipeline, scale=12, quality=120)
generate_mermaid_file("cltv_pipeline", cltv_pipeline, scale=12, quality=120)
generate_mermaid_file("product_performance_pipeline", product_performance_pipeline, scale=12, quality=120)
generate_mermaid_file("repeat_purchase_pipeline", repeat_purchase_pipeline, scale=12, quality=120)
generate_mermaid_file("sales_analysis_pipeline", sales_analysis_pipeline, scale=12, quality=120)

print(f"Tous les diagrammes ont été générés et enregistrés dans le répertoire avec succès !")
