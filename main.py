import os
import logging

# Import des classes de l'ETL
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.loader import DataLoader

# Import de la librairie dotenv pour charger les variables d'environnement
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

# Configuration du logger
logging.basicConfig(level=logging.INFO, format=os.getenv('LOGGING_FORMAT'), datefmt=os.getenv('LOGGING_DATE_FORMAT'))
logger = logging.getLogger(__name__)

def main():
    """
    Main function to execute the ETL process.

    Steps:
    1. Define paths for input data.
    2. Initialize ETL objects (extractor, transformer, loader).
    3. Extract data from various sources.
    4. Transform individual datasets.
    5. Unify datasets and create dimensions and fact tables.
    6. Load transformed data into MySQL database.
    7. Stop Spark session.

    Environment Variables:
    - PATH_SALESFORCES: Path to the Salesforce data folder.
    - PATH_CEGID: Path to the Cegid data folder.
    - PATH_PRODUCT: Path to the product data file.
    - PATH_BOUTIQUES: Path to the boutiques data folder.
    - SPARK_APP_NAME: Name of the Spark application.
    - SPARK_MASTER: Spark master URL.
    - JDBC_URL: JDBC URL for the database connection.
    - JDBC_USER: Username for the database connection.
    - JDBC_PASSWORD: Password for the database connection.
    - JDBC_DATABASE: Database name.
    - JDBC_DRIVER: JDBC driver class name.

    Raises:
    - Exception: If there is an error stopping the Spark session.
    """

    # ----------------------------------------------------------------
    # 1) Définition des chemins
    # ----------------------------------------------------------------
    sfcc_folder = os.path.join(os.getenv('PATH_SALESFORCES'))
    cegid_file = os.path.join(os.getenv('PATH_CEGID'), "2024_cegid_sales.json")
    products_file = os.path.join(os.getenv('PATH_PRODUCT'))
    boutiques_file = os.path.join(os.getenv('PATH_BOUTIQUES'), "2025_boutiques.csv")

    # ----------------------------------------------------------------
    # 2) Initialisation des objets ETL
    # ----------------------------------------------------------------
    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader(
        jdbc_url=os.getenv('JDBC_URL'),
        user=os.getenv('JDBC_USER'),
        password=os.getenv('JDBC_PASSWORD'),
        database=os.getenv('JDBC_DATABASE'),
        driver=os.getenv('JDBC_DRIVER'),
    )

    # ----------------------------------------------------------------
    # 3) EXTRACT : Charger les données
    # ----------------------------------------------------------------
    logger.info("🔍 Extraction des données")
    df_sfcc = extractor.extract_sfcc(sfcc_folder)
    df_cegid = extractor.extract_cegid(cegid_file)
    df_products = extractor.extract_products(products_file)
    df_boutiques = extractor.extract_boutiques(boutiques_file)

    # ----------------------------------------------------------------
    # 4) TRANSFORM : Transformation individuelle sur chaque dataset
    # ----------------------------------------------------------------

    logger.info("🔧 Transformation des données")
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

    logger.info("🤖 Création des dimensions et de la table de faits")
    dim_products = df_products.select("Product_ID", "Name", "Category", "Price") if df_products is not None else None
    dim_stores = df_boutiques.select("Store_ID", "Name", "Address") if df_boutiques is not None else None
    dim_clients = transformer.create_dim_clients(df_sfcc, df_cegid)
    fact_sales = transformer.create_fact_sales(df_sfcc, df_cegid, dim_clients, dim_products)

    # ----------------------------------------------------------------
    # 6) LOAD : Chargement dans MySQL
    # ----------------------------------------------------------------
    logger.info("♻️ Chargement des données dans la base de données")
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
    exit(0)

if __name__ == "__main__":
    main()
