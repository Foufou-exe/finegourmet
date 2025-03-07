# main.py
import os
import logging

# Import des classes de l'ETL
from extract import DataExtractor
from transform import DataTransformer
from loader import DataLoader

# Import des fonctions et classes Spark
from pyspark.sql.functions import (
    col,
    lit,
    monotonically_increasing_id,
    coalesce,
    first,
    col,
    sum as _sum,
    coalesce,
    regexp_replace,
    round as spark_round,
    lower,
    trim,
    regexp_replace,
    row_number
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    # ----------------------------------------------------------------
    # 1) Définition des chemins
    # ----------------------------------------------------------------
    path_data = "./data"
    sfcc_folder = os.path.join(path_data, "salesforces")
    cegid_file = os.path.join(path_data, "cegid", "2024_cegid_sales.json")
    products_file = os.path.join(path_data, "product")
    boutiques_file = os.path.join(path_data, "boutiques", "2025_boutiques.csv")

    # JDBC MySQL
    jdbc_url = "jdbc:mysql://localhost:3306/finegourmet"
    jdbc_user = "root"
    jdbc_password = "root"
    jdbc_driver = "com.mysql.cj.jdbc.Driver"

    # ----------------------------------------------------------------
    # 2) Initialisation des objets ETL
    # ----------------------------------------------------------------
    # Création d'une session Spark avec des configurations optimisées
    extractor = DataExtractor(app_name="FineGourmet_ETL")
    transformer = DataTransformer()
    loader = DataLoader(
        jdbc_url=jdbc_url,
        user=jdbc_user,
        password=jdbc_password,
        database="finegourmet",
        driver=jdbc_driver,
    )

    # ----------------------------------------------------------------
    # 3) EXTRACT : Charger les données
    # ----------------------------------------------------------------
    logger.info("=== Extraction des données ===")
    df_sfcc = extractor.extract_sfcc(sfcc_folder)
    df_cegid = extractor.extract_cegid(cegid_file)
    df_products = extractor.extract_products(products_file)
    df_boutiques = extractor.extract_boutiques(boutiques_file)

    # Vérification des doublons
    print("Vérification des doublons avant transformation :")
    df_cegid.groupBy("Sale_ID").count().filter(col("count") > 1).show(10)
    df_sfcc.groupBy("Sale_ID").count().filter(col("count") > 1).show(10)

    # Ajout d'une colonne "Source" pour différencier les ventes en ligne (sfcc) et physiques (cegid)
    if df_sfcc is not None:
        df_sfcc = df_sfcc.withColumn("Source", lit("sfcc"))
    if df_cegid is not None:
        df_cegid = df_cegid.withColumn("Source", lit("cegid"))

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Appliquer les transformations
    # ----------------------------------------------------------------
    if df_products is not None:
        df_products = transformer.transform_products(df_products)
    if df_sfcc is not None:
        df_sfcc = transformer.transform_sfcc(df_sfcc, df_products)
    if df_cegid is not None:
        df_cegid = transformer.transform_cegid(df_cegid, df_products)
    if df_boutiques is not None:
        df_boutiques = transformer.transform_boutiques(df_boutiques)

    # ----------------------------------------------------------------
    # 5) UNIFICATION ET CREATION DES DIMENSIONS ET DE LA TABLE DE FAITS
    # ----------------------------------------------------------------
    # Dimension Produit (à partir de df_products)
    dim_products = (
        df_products.select("Product_ID", "Name", "Category", "Price")
        if df_products
        else None
    )

    # Dimension Boutique (à partir de df_boutiques)
    dim_stores = (
        df_boutiques.select("Store_ID", "Name", "Address") if df_boutiques else None
    )

    # Dimension Client
    dim_clients = None
    if df_sfcc is not None and df_cegid is not None:
        clients_sfcc = df_sfcc.select(
            "Email", "Last_Name", "First_Name", "Phone", "Address"
        )
        clients_cegid = (
            df_cegid.select("Email")
            .withColumn("Last_Name", lit(None).cast("string"))
            .withColumn("First_Name", lit(None).cast("string"))
            .withColumn("Phone", lit(None).cast("string"))
            .withColumn("Address", lit(None).cast("string"))
        )


        # Fusion des clients SFCC et CEGID en évitant les NULL
        dim_clients = (
            clients_sfcc.unionByName(clients_cegid, allowMissingColumns=True)
            .filter(col("Email").isNotNull())  # Exclure les clients sans email
            .groupBy("Email")
            .agg(
                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                first("First_Name", ignorenulls=True).alias("First_Name"),
                first("Phone", ignorenulls=True).alias("Phone"),
                first("Address", ignorenulls=True).alias("Address"),
            )
        )

        # Ajout d'un ID unique fiable avec row_number()
        window_spec = Window.orderBy("Email")
        dim_clients = dim_clients.withColumn("Client_ID", row_number().over(window_spec))

    elif df_sfcc is not None:
        dim_clients = (
            df_sfcc.select("Email", "Last_Name", "First_Name", "Phone", "Address")
            .groupBy("Email")
            .agg(
                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                first("First_Name", ignorenulls=True).alias("First_Name"),
                first("Phone", ignorenulls=True).alias("Phone"),
                first("Address", ignorenulls=True).alias("Address"),
            )
            .withColumn("Client_ID", monotonically_increasing_id())
        )

    elif df_cegid is not None:
        dim_clients = (
            df_cegid.select("Email")
            .withColumn("Last_Name", lit(None).cast("string"))
            .withColumn("First_Name", lit(None).cast("string"))
            .withColumn("Phone", lit(None).cast("string"))
            .withColumn("Address", lit(None).cast("string"))
            .groupBy("Email")
            .agg(
                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                first("First_Name", ignorenulls=True).alias("First_Name"),
                first("Phone", ignorenulls=True).alias("Phone"),
                first("Address", ignorenulls=True).alias("Address"),
            )
            .withColumn("Client_ID", monotonically_increasing_id())
        )

    fact_sales = df_sfcc.unionByName(df_cegid, allowMissingColumns=True)

    print("Lecture cegid")
    # fact_sales.show(500, truncate=False)

    if fact_sales is not None:
        fact_sales = fact_sales.withColumnRenamed("Transaction_Date", "Date")

    # TODO: Permet de récupérer l'ID de la boutique pour le lier à la vente
    # Correction du FK_Store_ID pour les ventes physiques
    if df_cegid is not None:
        fact_sales = fact_sales.withColumn("FK_Store_ID", col("Store_ID"))

    # ✅ Correction : Normalisation des emails pour éviter les erreurs de jointure
    if dim_clients is not None:
        # Normalisation des emails dans Dim_Client
        dim_clients = dim_clients.withColumn(
            "Email", lower(trim(regexp_replace(col("Email"), r"[^a-zA-Z0-9._%+-@]+", "")))
        )

        # Normalisation des emails dans Fact_Sales avant la jointure
        fact_sales = fact_sales.withColumn(
            "Email", lower(trim(regexp_replace(col("Email"), r"[^a-zA-Z0-9._%+-@]+", "")))
        )

        # Normalisation des emails AVANT la jointure pour éviter les erreurs
        dim_clients = dim_clients.withColumn(
            "Email", lower(trim(regexp_replace(col("Email"), r"[^a-zA-Z0-9._%+-@]+", "")))
        )

        fact_sales = fact_sales.withColumn(
            "Email", lower(trim(regexp_replace(col("Email"), r"[^a-zA-Z0-9._%+-@]+", "")))
        )

        # Faire la jointure en évitant les erreurs d'association
        fact_sales = fact_sales.join(
            dim_clients.select("Client_ID", "Email"), on="Email", how="left"
        ).withColumnRenamed("Client_ID", "FK_Client_ID")

        # S'assurer que FK_Client_ID a bien été attribué
        fact_sales = fact_sales.withColumn(
            "FK_Client_ID", col("FK_Client_ID").cast("string")
        )

        # Vérification des cas problématiques (DEBUG)
        fact_sales.filter(col("FK_Client_ID").isNull()).show(20, truncate=False)

        # ✅ Correction du type de FK_Client_ID pour éviter d'éventuelles erreurs
        fact_sales = fact_sales.withColumn(
            "FK_Client_ID", col("FK_Client_ID").cast("string")
        )

        # Jointure avec Dim_Product
        if dim_products is not None:
            fact_sales = fact_sales.join(
                dim_products.select(
                    col("Product_ID").alias("prod_id"), col("Name").alias("prod_name")
                ),
                fact_sales["Product_ID"] == col("prod_id"),
                "left",
            )
            fact_sales = fact_sales.withColumn(
                "FK_Product_ID", coalesce(col("Product_ID"), col("prod_id"))
            )
            fact_sales = (
                fact_sales.drop("Product_Name").drop("prod_id").drop("prod_name")
            )
        else:
            fact_sales = fact_sales.withColumnRenamed("Product_ID", "FK_Product_ID")

        # Sélection finale
        fact_sales = fact_sales.select(
            "Sale_ID",
            "Quantity",
            "Price",
            "Date",
            "FK_Client_ID",
            "FK_Product_ID",
            "FK_Store_ID",
        )

        # Vérification des doublons après unification
        print("🔎 Vérification détaillée des doublons après unification :")

        df_duplicates = fact_sales.groupBy("Sale_ID").count().filter(col("count") > 1)

        # Afficher les Sale_ID en doublon avec toutes leurs colonnes
        df_duplicated_sales = fact_sales.join(df_duplicates, on="Sale_ID", how="inner")

        # Afficher les doublons
        df_duplicated_sales.show(10, truncate=False)


    # TODO: Permet de supprimer les caractères spéciaux dans les emails
    dim_clients = dim_clients.withColumn(
        "Email", regexp_replace(col("Email"), r"[^a-zA-Z0-9._%+-@]+", "")
    )

    # ----------------------------------------------------------------
    # AFFICHAGE DES CLIENTS APRÈS TRANSFORMATION
    # ----------------------------------------------------------------
    if dim_clients is not None:
        logger.info("=== Aperçu des Clients (Dim_Client) ===")
        dim_clients.show(50, truncate=False)  # Affichage des 50 premiers clients
        logger.info(f"Nombre total de clients dans Dim_Client : {dim_clients.count()}")

    # Affichage des 1000 premières lignes du fact_sales
    fact_sales.show(1000, truncate=False)

    # ----------------------------------------------------------------
    # 6) LOAD : Chargement dans MySQL
    # ----------------------------------------------------------------
    logger.info("=== Chargement des données dans MySQL ===")

    # Dimension produits
    if dim_products is not None:
        loader.load_dim_product(dim_products)
    # Dimension boutiques
    if dim_stores is not None:
        loader.load_dim_store(dim_stores)

    # Dimension clients
    if dim_clients is not None:
        loader.load_dim_client(dim_clients)

    # Table de faits - approche par mini-batches
    if fact_sales is not None:
        loader.load_fact_sales(fact_sales)

    # ----------------------------------------------------------------
    # 7) Arrêt de Spark
    # ----------------------------------------------------------------
    try:
        extractor.stop()
        logger.info("Session Spark arrêtée avec succès")
    except Exception as e:
        logger.error(f"Erreur lors de l'arrêt de Spark: {str(e)}")

    logger.info("✅ ETL terminé avec succès.")


if __name__ == "__main__":
    main()
