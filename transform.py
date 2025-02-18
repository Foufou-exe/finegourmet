# transform.py
import logging
from pyspark.sql.functions import (
    col, trim, regexp_replace, lower, to_date, when, length, lit, concat
)
from pyspark.sql.types import BooleanType, IntegerType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataTransformer:
    def __init__(self):
        pass

    def transform_sfcc(self, df_sfcc):
        """
        Transforme le DataFrame SFCC :
         - Mise en minuscules de l'email
         - Nettoyage de la colonne Address (suppression de guillemets et espaces en début/fin)
         - Standardisation de la colonne Phone au format +33XXXXXXXXX :
             * Suppression de tout caractère non numérique (sauf le +)
             * Retrait du préfixe +33 et des zéros initiaux
             * Si le résultat comporte exactement 9 chiffres, préfixer par +33, sinon NULL.
         - Conversion de Transaction_Date et Quantity.
        """
        # Nettoyage général des espaces et retours de ligne a déjà pu être appliqué dans extract,
        # mais on peut l'appliquer de nouveau ici si besoin.

        # Nettoyage général des valeurs : suppression des espaces, tabulations et sauts de ligne en trop
        for column in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn(column, trim(regexp_replace(col(column), r"[\t\r\n]+", " ")))

        # Mise en minuscules pour l'email afin d'uniformiser
        if "Email" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn("Email", lower(col("Email")))

        if "Address" in df_sfcc.columns:
            # Supprime guillemets et espaces en début/fin
            df_sfcc = df_sfcc.withColumn(
                "Address",
                trim(regexp_replace(col("Address"), r'^[\s"]+|[\s"]+$', ''))
            )

            # Si tu veux retirer TOUS les guillemets internes :
            df_sfcc = df_sfcc.withColumn(
                "Address",
                trim(regexp_replace(col("Address"), '"', ''))
            )

        # Standardisation du numéro de téléphone
        if "Phone" in df_sfcc.columns:
            # Supprimer les zéros en début de chaîne
            df_sfcc = df_sfcc.withColumn("Phone", regexp_replace(col("Phone"), r'^0+', ''))

            # Forcer le type string (au cas où ce serait inféré en float/double)
            df_sfcc = df_sfcc.withColumn("Phone", col("Phone").cast("string"))

            df_sfcc = df_sfcc.withColumn(
                "Phone",
                when(
                    (length(col("Phone")) == 9) & (col("Phone").rlike("^[0-9]+$")),
                    concat(lit("+33"), col("Phone"))
                ).otherwise(lit(None))
            )

        # Conversion des types
        if "Customer_ID" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn("Customer_ID", col("Customer_ID").cast(IntegerType()))

        df_sfcc.show(10, truncate=False)

        return df_sfcc

    def transform_cegid(self, df_cegid):
        # Transformation similaire à SFCC si besoin.
        df_cegid = df_cegid.withColumnRenamed("quantity", "Quantity") \
                            .withColumnRenamed("sale_id", "Sale_ID") \
                            .withColumnRenamed("email", "Email") \
                            .withColumnRenamed("transaction_date", "Transaction_Date") \
                            .withColumnRenamed("product_name", "Product_Name") \
                            .withColumnRenamed("price", "Price")

        if "Transaction_Date" in df_cegid.columns:
            df_cegid = df_cegid.withColumn("Transaction_Date", to_date(col("Transaction_Date"), "yyyy-MM-dd"))
        if "Quantity" in df_cegid.columns:
            df_cegid = df_cegid.withColumn("Quantity", col("Quantity").cast(IntegerType()))
        if "Price" in df_cegid.columns:
            df_cegid = df_cegid.withColumn("Price", col("Price").cast("double"))
        # Mise en minuscules pour l'email afin d'uniformiser + remplissage des valeurs nulles pour Email
        if "Email" in df_cegid.columns:
            df_cegid = df_cegid.withColumn("Email", lower(col("Email")))


        df_cegid.show(10, truncate=False)
        return df_cegid

    def transform_products(self, df_products):
        df_products = df_products.withColumnRenamed("product_id", "Product_ID") \
                        .withColumnRenamed("product_name", "Name") \
                        .withColumnRenamed("price", "Price") \
                        .withColumnRenamed("category", "Category")
        df_products = df_products.withColumn("Price", col("Price").cast("double"))
        df_products.show(10, truncate=False)
        return df_products

    def transform_boutiques(self, df_boutiques):
        if "Address" in df_boutiques.columns:
            df_boutiques = df_boutiques.withColumn(
                "Address",
                trim(regexp_replace(col("Address"), r'^[\s"]+|[\s"]+$', ''))
            )

        df_boutiques.show(10, truncate=False)
        return df_boutiques
