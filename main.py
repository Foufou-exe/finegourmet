# main.py
import os
import logging

# Import des classes de l'ETL
from extract import DataExtractor
from transform import DataTransformer
from loader import DataLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Transformation individuelle sur chaque dataset
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
    # 5) UNIFICATION & CREATION DES DIMENSIONS ET DE LA TABLE DE FAITS
    # Cette étape est maintenant déléguée au transformateur.
    # ----------------------------------------------------------------
    dim_products = df_products.select("Product_ID", "Name", "Category", "Price") if df_products is not None else None
    dim_stores = df_boutiques.select("Store_ID", "Name", "Address") if df_boutiques is not None else None
    dim_clients = transformer.create_dim_clients(df_sfcc, df_cegid)
    fact_sales = transformer.create_fact_sales(df_sfcc, df_cegid, dim_clients, dim_products)

    # ----------------------------------------------------------------
    # 6) LOAD : Chargement dans MySQL
    # ----------------------------------------------------------------
    logger.info("=== Chargement des données dans MySQL ===")
    if dim_products is not None:
        loader.load_dim_product(dim_products)
    if dim_stores is not None:
        loader.load_dim_store(dim_stores)
    if dim_clients is not None:
        loader.load_dim_client(dim_clients)
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
