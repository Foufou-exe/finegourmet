# main.py
import os
import logging
from extract import DataExtractor
from transform import DataTransformer
from loader import DataLoader
from pyspark.sql.functions import col, to_date, lit, monotonically_increasing_id, when, coalesce

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    # ----------------------------------------------------------------
    # 1) Définition des chemins
    # ----------------------------------------------------------------
    path_data = "./data"
    sfcc_folder = os.path.join(path_data, "salesforces")
    cegid_file = os.path.join(path_data, "cegid", "2024_cegid_sales.json")
    products_file = os.path.join(path_data, "product", "2025_product_reference.csv")
    boutiques_file = os.path.join(path_data, "boutiques", "2025_boutiques.csv")

    # JDBC MySQL
    jdbc_url = "jdbc:mysql://localhost:3306/finegourmet"
    jdbc_user = "root"
    jdbc_password = "root"
    jdbc_driver = "com.mysql.cj.jdbc.Driver"

    # ----------------------------------------------------------------
    # 2) Initialisation des objets ETL
    # ----------------------------------------------------------------
    extractor = DataExtractor(app_name="FineGourmet_ETL")
    transformer = DataTransformer()
    loader = DataLoader(jdbc_url=jdbc_url, user=jdbc_user, password=jdbc_password, database="finegourmet", driver=jdbc_driver)

    # ----------------------------------------------------------------
    # 3) EXTRACT : Charger les données
    # ----------------------------------------------------------------
    logger.info("=== Extraction des données ===")
    df_sfcc = extractor.extract_sfcc(sfcc_folder)
    df_cegid = extractor.extract_cegid(cegid_file)
    df_products = extractor.extract_products(products_file)
    df_boutiques = extractor.extract_boutiques(boutiques_file)

    # Ajout d'une colonne "Source" pour différencier les ventes en ligne (sfcc) et physiques (cegid)
    if df_sfcc is not None:
        df_sfcc = df_sfcc.withColumn("Source", lit("sfcc"))
    if df_cegid is not None:
        df_cegid = df_cegid.withColumn("Source", lit("cegid"))

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Appliquer les transformations
    # ----------------------------------------------------------------
    if df_sfcc is not None:
        df_sfcc = transformer.transform_sfcc(df_sfcc)
    if df_cegid is not None:
        df_cegid = transformer.transform_cegid(df_cegid)
    if df_products is not None:
        df_products = transformer.transform_products(df_products)
    if df_boutiques is not None:
        df_boutiques = transformer.transform_boutiques(df_boutiques)

    # ----------------------------------------------------------------
    # 5) UNIFICATION ET CREATION DES DIMENSIONS ET DE LA TABLE DE FAITS
    # ----------------------------------------------------------------
    # Dimension Produit (à partir de df_products)
    if df_products is not None:
        dim_products = df_products.select("Product_ID", "Name", "Category", "Price")
    else:
        dim_products = None

    # Dimension Boutique (à partir de df_boutiques)
    if df_boutiques is not None:
        dim_stores = df_boutiques.select("Store_ID", "Name", "Address")
    else:
        dim_stores = None

    # Dimension Client : fusionner les clients issus de SFCC et CEGID
    # Pour CEGID, on crée les colonnes manquantes avec des valeurs nulles
    dim_clients = None
    if df_sfcc is not None and df_cegid is not None:
        clients_sfcc = df_sfcc.select("Email", "Last_Name", "First_Name", "Phone", "Address")
        clients_cegid = df_cegid.select("Email") \
            .withColumn("Last_Name", lit(None).cast("string")) \
            .withColumn("First_Name", lit(None).cast("string")) \
            .withColumn("Phone", lit(None).cast("string")) \
            .withColumn("Address", lit(None).cast("string"))
        dim_clients = clients_sfcc.unionByName(clients_cegid, allowMissingColumns=True) \
                                   .dropDuplicates() \
                                   .withColumn("Client_ID", monotonically_increasing_id())
    elif df_sfcc is not None:
        dim_clients = df_sfcc.select("Email", "Last_Name", "First_Name", "Phone", "Address") \
                             .dropDuplicates() \
                             .withColumn("Client_ID", monotonically_increasing_id())
    elif df_cegid is not None:
        dim_clients = df_cegid.select("Email") \
            .withColumn("Last_Name", lit(None).cast("string")) \
            .withColumn("First_Name", lit(None).cast("string")) \
            .withColumn("Phone", lit(None).cast("string")) \
            .withColumn("Address", lit(None).cast("string")) \
            .dropDuplicates() \
            .withColumn("Client_ID", monotonically_increasing_id())

    # Table de faits Fact_Sales : fusion des données SFCC et CEGID
    fact_sales = None
    if df_sfcc is not None and df_cegid is not None:
        fact_sales = df_sfcc.unionByName(df_cegid, allowMissingColumns=True)
    elif df_sfcc is not None:
        fact_sales = df_sfcc
    elif df_cegid is not None:
        fact_sales = df_cegid

    if fact_sales is not None:
        # Renommer Transaction_Date en Date
        if "Transaction_Date" in fact_sales.columns:
            fact_sales = fact_sales.withColumnRenamed("Transaction_Date", "Date")

        # Pour les ventes physiques (cegid), affecter une valeur par défaut pour FK_Store_ID
        fact_sales = fact_sales.withColumn("FK_Store_ID", when(col("Source") == "cegid", lit("DEFAULT_STORE")).otherwise(lit(None)))

        # Jointure avec Dim_Client pour récupérer Client_ID via Email
        if dim_clients is not None:
            fact_sales = fact_sales.join(dim_clients.select("Client_ID", "Email"), on="Email", how="left")

        # Récupérer l'identifiant produit :
        # Pour SFCC, la colonne Product_ID existe déjà.
        # Pour CEGID, on dispose de Product_Name et on rejoint avec la dimension produit.
        if dim_products is not None:
            fact_sales = fact_sales.join(
                dim_products.select(col("Product_ID").alias("prod_id"), col("Name").alias("prod_name")),
                fact_sales.Product_Name == col("prod_name"),
                "left"
            )
            fact_sales = fact_sales.withColumn("FK_Product_ID", coalesce(col("Product_ID"), col("prod_id")))
            fact_sales = fact_sales.drop("Product_Name").drop("prod_id").drop("prod_name")
        else:
            fact_sales = fact_sales.withColumnRenamed("Product_ID", "FK_Product_ID")

        # Sélection finale en respectant l'ordre du nouveau schéma Fact_Sales
        fact_sales = fact_sales.select("Sale_ID", "Quantity", "Price", "Date", "Client_ID", "FK_Product_ID", "FK_Store_ID", "Source")
        fact_sales = fact_sales.drop("Source")  # La colonne Source n'est utilisée qu'en interne
    else:
        logger.warning("Aucune donnée factuelle disponible pour Fact_Sales.")

    # ----------------------------------------------------------------
    # 6) LOAD : Chargement dans MySQL
    # ----------------------------------------------------------------
    logger.info("=== Chargement des données dans MySQL ===")
    if dim_products is not None:
        loader.load_dim_product(dim_products, mode="append")
    if dim_stores is not None:
        loader.load_dim_store(dim_stores, mode="append")
    if dim_clients is not None:
        loader.load_dim_client(dim_clients, mode="append")
    # Les dimensions Dim_Date et Dim_Channel ne sont plus utilisées dans le nouveau schéma
    if fact_sales is not None:
        loader.load_fact_sales(fact_sales, mode="append")

    # ----------------------------------------------------------------
    # 7) Arrêt de Spark
    # ----------------------------------------------------------------
    extractor.stop()
    logger.info("✅ ETL terminé avec succès.")

if __name__ == "__main__":
    main()
