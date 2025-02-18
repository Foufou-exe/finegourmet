# main.py
import os
import logging
from extract import DataExtractor
from transform import DataTransformer
from loader import DataLoader
from pyspark.sql.functions import col, to_date, lit, monotonically_increasing_id

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
    jdbc_user = "finegourmet"
    jdbc_password = "finegourmet"
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

    if df_sfcc is not None:
        # Dimension Client (à partir de df_sfcc)
        dim_clients = df_sfcc.select("Email", "Last_Name", "First_Name", "Phone").dropna() \
                        .dropDuplicates() \
                        .withColumn("Client_ID", monotonically_increasing_id())
                        
    # Dimension Client (à partir de df_cegid)
    if df_cegid is not None:
        dim_clients = df_cegid.select("Email").dropna() \
                        .dropDuplicates() \
                        .withColumn("Client_ID", monotonically_increasing_id())
    else:
        dim_clients = None

    # Dimension Date (à partir de df_cegid, colonne transaction_date)
    if df_cegid is not None:
        dim_time = df_cegid.select(to_date(col("transaction_date"), "yyyy-MM-dd").alias("Date")) \
                    .withColumn("Date_ID", monotonically_increasing_id())
    else:
        dim_time = None

    # Dimension Channel statique
    spark = extractor.spark  # Utilisation de la session Spark existante
    dim_channels = spark.createDataFrame([
        (1, "En ligne"),
        (2, "En magasin")
    ], ["Channel_ID", "Type"])

    # Table de faits Fact_Sales (fusion des données SFCC et CEGID)
    fact_sales = None
    if df_sfcc is not None and df_cegid is not None:
        fact_sales = df_sfcc.unionByName(df_cegid, allowMissingColumns=True)
    elif df_sfcc is not None:
        fact_sales = df_sfcc
    elif df_cegid is not None:
        fact_sales = df_cegid

    if fact_sales is not None:
        fact_sales = fact_sales.select(
            "Sale_ID",
            "Quantity",
            "Price",
            "Transaction_Date",
            "Product_ID",
            "Email"
        )
        # Jointure avec Dim_Date pour obtenir FK_Date_ID
        if dim_time is not None:
            fact_sales = fact_sales.join(dim_time.select("Date_ID", "Date"),
                                         fact_sales.Transaction_Date == dim_time.Date,
                                         "left")
        # Jointure avec Dim_Client pour obtenir FK_Client_ID
        if dim_clients is not None:
            fact_sales = fact_sales.join(dim_clients.select("Client_ID", "Email"),
                                         on="Email",
                                         how="left")
        # On attribue une valeur statique pour FK_Channel_ID (par exemple 2 pour CEGID)
        fact_sales = fact_sales.withColumn("FK_Channel_ID", lit(2))
        # Pour les ventes en ligne, FK_Store_ID = NULL
        fact_sales = fact_sales.withColumn("FK_Store_ID", lit(None))
        # Sélection finale dans l'ordre du modèle Fact_Sales
        fact_sales = fact_sales.selectExpr(
            "Sale_ID",
            "Quantity",
            "Price",
            "Date_ID as FK_Date_ID",
            "Client_ID as FK_Client_ID",
            "Product_ID as FK_Product_ID",
            "FK_Channel_ID",
            "FK_Store_ID"
        )
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
    if dim_time is not None:
        loader.load_dim_date(dim_time, mode="append")
    if dim_channels is not None:
        loader.load_dim_channel(dim_channels, mode="append")
    if fact_sales is not None:
        loader.load_fact_sales(fact_sales, mode="append")

    # ----------------------------------------------------------------
    # 7) Arrêt de Spark
    # ----------------------------------------------------------------
    extractor.stop()
    logger.info("✅ ETL terminé avec succès.")

if __name__ == "__main__":
    main()
