# extract.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import col, to_date

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataExtractor:
    def __init__(self, app_name="DataExtraction", master="local[*]"):
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .master(master) \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.driver.extraClassPath", "./database/connector/mysql-connector-j-9.1.0.jar") \
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
                .getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")


    def extract_sfcc(self, sfcc_folder):
        """
        Charge tous les fichiers CSV SFCC se terminant par "_sfcc_sales.csv" dans le dossier donné
        et renvoie un DataFrame unifié.
        """
        sfcc_files = [os.path.join(sfcc_folder, f) for f in os.listdir(sfcc_folder) if f.endswith("_sfcc_sales.csv")]

        df_sfcc = None
        for path in sfcc_files:
            if os.path.exists(path):
                logger.info(f"Traitement du fichier : {path}")
                df_temp = self.spark.read.option("header", "true") \
                                    .option("inferSchema", "true") \
                                    .csv(path)
                # Normalisation partielle des colonnes
                df_temp = df_temp.withColumnRenamed("sale_id", "Sale_ID") \
                                .withColumnRenamed("transaction_date", "Transaction_Date") \
                                .withColumnRenamed("product_id", "Product_ID") \
                                .withColumnRenamed("quantity", "Quantity") \
                                .withColumnRenamed("customer_id", "Customer_ID") \
                                .withColumnRenamed("customer_email", "Email") \
                                .withColumnRenamed("customer_last_name", "Last_Name") \
                                .withColumnRenamed("customer_first_name", "First_Name") \
                                .withColumnRenamed("customer_phone", "Phone") \
                                .withColumnRenamed("customer_address", "Address") \
                                .withColumnRenamed("email_optin", "Email_Optin") \
                                .withColumnRenamed("sms_optin", "Sms_Optin")

                # Conversion des types
                df_temp = df_temp.withColumn("Email_Optin", col("Email_Optin").cast(BooleanType())) \
                                .withColumn("Sms_Optin", col("Sms_Optin").cast(BooleanType())) \
                                .withColumn("Transaction_Date", to_date(col("Transaction_Date"), "yyyy-MM-dd"))

                # Union par nom de colonne en gérant les colonnes manquantes
                if df_sfcc is None:
                    df_sfcc = df_temp
                else:
                    df_sfcc = df_sfcc.unionByName(df_temp, allowMissingColumns=True)
        return df_sfcc

    def extract_cegid(self, cegid_file):
        """
        Charge le fichier JSON CEGID.
        """
        if os.path.exists(cegid_file):
            logger.info(f"Extraction du fichier CEGID : {cegid_file}")
            return self.spark.read.option("multiline", "true").json(cegid_file)
        else:
            logger.error(f"Fichier CEGID non trouvé : {cegid_file}")
            return None

    def extract_products(self, products_file):
        """
        Charge tous les fichiers CSV des produits dans le dossier donné
        et renvoie un DataFrame unifié.
        """
        product_files = [os.path.join(products_file, f) for f in os.listdir(products_file) if f.endswith(".csv")]

        df_products = None
        for path in product_files:
            if os.path.exists(path):
                logger.info(f"Traitement du fichier produit : {path}")
                df_temp = (
                    self.spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(path)
                )

                # Union par nom de colonne en gérant les colonnes manquantes
                if df_products is None:
                    df_products = df_temp
                else:
                    df_products = df_products.unionByName(df_temp, allowMissingColumns=True)

        if df_products is None:
            logger.error(f"Aucun fichier produit trouvé dans : {products_file}")

        return df_products

    def extract_boutiques(self, boutiques_file):
        """
        Charge le fichier boutiques à l'aide de la lecture en texte et extraction par regex.
        """
        if os.path.exists(boutiques_file):
            logger.info(f"Extraction du fichier boutiques : {boutiques_file}")
            df_raw = self.spark.read.text(boutiques_file)  # Chaque ligne dans la colonne "value"
            # Suppression de la ligne d'en-tête
            header = df_raw.first()[0]
            df_raw = df_raw.filter(df_raw.value != header)
            from pyspark.sql.functions import regexp_extract
            regex_pattern = r'^(.*?)\|(.*?)\|"(.*)"$'
            return df_raw.select(
                regexp_extract("value", regex_pattern, 1).alias("Store_ID"),
                regexp_extract("value", regex_pattern, 2).alias("Name"),
                regexp_extract("value", regex_pattern, 3).alias("Address"),
            )
        else:
            logger.error(f"Fichier boutiques non trouvé : {boutiques_file}")
            return None

    def stop(self):
        self.spark.stop()
