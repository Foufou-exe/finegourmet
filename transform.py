import logging
from pyspark.sql.functions import (
    col, trim, regexp_replace, lower, to_date, when, length, lit, concat, row_number,
    substring, concat_ws, first, isnan, round, expr, date_format
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import Window

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataTransformer:
    def __init__(self):
        pass

    # ---------------------------------------------------------------------------
    # Transformation du DataFrame SFCC
    # ---------------------------------------------------------------------------
    def transform_sfcc(self, df_sfcc, df_products):
        """
        Nettoie et transforme le DataFrame SFCC :
          - Définit Quantity à 1 (les ventes en ligne n'ayant pas cette info)
          - Supprime les espaces et caractères de contrôle dans toutes les colonnes
          - Met en minuscules les emails
          - Nettoie la colonne Address (suppression des guillemets et espaces superflus)
          - Standardise le numéro de téléphone au format +33XXXXXXXXX si possible
          - Convertit Customer_ID en entier
          - Jointure avec df_products pour récupérer le Price (le cas échéant) et conversion en double
        """
        # Ajout de la quantité par défaut (1)
        df_sfcc = df_sfcc.withColumn("Quantity", lit(1).cast(IntegerType()))

        # Nettoyage général de toutes les colonnes (suppression d'espaces, tabulations et sauts de ligne)
        for column in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn(column, trim(regexp_replace(col(column), r"[\t\r\n]+", " ")))

        # Uniformisation de l'email en minuscules
        if "Email" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn("Email", lower(col("Email")))

        # Nettoyage de l'adresse (suppression des guillemets et espaces en début/fin)
        if "Address" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn(
                "Address", trim(regexp_replace(col("Address"), r'^[\s"]+|[\s"]+$', ''))
            )
            df_sfcc = df_sfcc.withColumn(
                "Address", trim(regexp_replace(col("Address"), '"', ''))
            )

        # Standardisation du numéro de téléphone
        if "Phone" in df_sfcc.columns:
            # Supprime les zéros en début de chaîne
            df_sfcc = df_sfcc.withColumn("Phone", regexp_replace(col("Phone"), r'^0+', ''))
            # Conversion en string
            df_sfcc = df_sfcc.withColumn("Phone", col("Phone").cast("string"))
            # Format +33XXXXXXXXX si exactement 9 chiffres
            df_sfcc = df_sfcc.withColumn(
                "Phone",
                when(
                    (length(col("Phone")) == 9) & (col("Phone").rlike("^[0-9]+$")),
                    concat(lit("+33"), col("Phone"))
                ).otherwise(lit(None))
            )

        # Conversion de Customer_ID en entier (si présent)
        if "Customer_ID" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn("Customer_ID", col("Customer_ID").cast(IntegerType()))

        # Jointure avec df_products pour récupérer le Price à partir de Product_ID
        if df_products is not None:
            # On s'assure que chaque Product_ID est unique dans df_products
            df_products = df_products.dropDuplicates(["Product_ID"])
            df_sfcc = df_sfcc.join(
                df_products.select(col("Product_ID").alias("prod_id"), "Price"),
                df_sfcc["Product_ID"] == col("prod_id"),
                "left"
            ).drop("prod_id")
        # Conversion du Price en double
        df_sfcc = df_sfcc.withColumn("Price", col("Price").cast(DoubleType()))

        logger.info("Transformation SFCC terminée.")
        df_sfcc.show(10, truncate=False)
        return df_sfcc

    # ---------------------------------------------------------------------------
    # Transformation du DataFrame CEGID
    # ---------------------------------------------------------------------------
    def transform_cegid(self, df_cegid, df_products):
        """
        Nettoie et transforme le DataFrame CEGID (ventes physiques) :
          - Renomme les colonnes pour harmoniser avec SFCC
          - Convertit Transaction_Date en date et Quantity en entier
          - Corrige la colonne Price : calcule le prix unitaire et le prix total (price unitaire * Quantity)
            en cas de valeurs invalides, en récupérant en parallèle le prix depuis df_products.
          - Transforme le Sale_ID en extrayant des informations (store et date) et corrige les erreurs si besoin.
          - Corrige les doublons dans Sale_ID en ajoutant un suffixe unique uniquement si nécessaire.
          - Met en minuscules l'email
          - Sélectionne les colonnes finales.
        """
        # Renommage des colonnes pour uniformiser
        df_cegid = df_cegid.withColumnRenamed("quantity", "Quantity") \
                            .withColumnRenamed("sale_id", "Sale_ID") \
                            .withColumnRenamed("email", "Email") \
                            .withColumnRenamed("transaction_date", "Transaction_Date") \
                            .withColumnRenamed("product_name", "Product_Name") \
                            .withColumnRenamed("price", "Price")

        # Conversion des types pour Transaction_Date et Quantity
        if "Transaction_Date" in df_cegid.columns:
            df_cegid = df_cegid.withColumn("Transaction_Date", to_date(col("Transaction_Date"), "yyyy-MM-dd"))
        if "Quantity" in df_cegid.columns:
            df_cegid = df_cegid.withColumn("Quantity", col("Quantity").cast(IntegerType()))

        # --- Correction et calcul de Price ---
        if "Price" in df_cegid.columns and "Quantity" in df_cegid.columns and "Product_Name" in df_cegid.columns:
            # Calcul du prix unitaire à partir des valeurs existantes (lorsqu'elles sont valides)
            df_price_lookup = df_cegid.filter(
                (col("Price").isNotNull()) &
                (~isnan(col("Price"))) &
                (trim(col("Price")) != "") &
                (col("Price") != "X") &
                (col("Price").cast("double").isNotNull())
            ).withColumn(
                "price_unitaire", (col("Price") / col("Quantity")).cast(DoubleType())
            )

            # Agréger le premier prix unitaire valide pour chaque Product_Name
            df_price_lookup = df_price_lookup.groupBy("Product_Name").agg(
                first("price_unitaire", ignorenulls=True).alias("ref_price_unitaire")
            )

            # Jointure avec df_products pour récupérer le prix depuis la référence produit si besoin
            if df_products is not None:
                df_products = df_products.dropDuplicates(["Product_Name"])
                df_cegid = df_cegid.join(
                    df_products.select(col("Product_Name").alias("Name"), col("Price").alias("unit_price")),
                    df_cegid["Product_Name"] == col("Name"),
                    "left"
                ).drop("Name")
            # Calcul du prix unitaire de référence : si Price existait déjà, on le calcule; sinon, on prend unit_price
            df_cegid = df_cegid.withColumn(
                "ref_price_unitaire",
                when(
                    (col("Price").isNotNull()) & (~isnan(col("Price"))) & (trim(col("Price")) != "") &
                    (col("Price") != "X") & (col("Price").cast("double").isNotNull()),
                    col("Price").cast(DoubleType()) / col("Quantity")
                ).otherwise(col("unit_price"))
            )

            # Calcul du prix total en fonction de la quantité
            df_cegid = df_cegid.withColumn(
                "Price",
                when(
                    (col("Price").isNull()) | (trim(col("Price")) == "") | (col("Price") == "X") | (col("Price").cast("double").isNull()),
                    round(col("ref_price_unitaire") * col("Quantity"), 2)
                ).otherwise(col("Price").cast(DoubleType()))
            )

            # Suppression des colonnes temporaires
            df_cegid = df_cegid.drop("ref_price_unitaire", "unit_price")


                # 🚀 **4. Conversion `Product_Name` → `Product_ID`**
        df_cegid = df_cegid.join(
            df_products.select(col("product_name").alias("product_name"), col("Product_ID")),
            df_cegid["Product_Name"] == col("product_name"),
            "left"
        ).drop("product_name")

        # **⚠️ Vérification des produits non trouvés**
        missing_products = df_cegid.filter(col("Product_ID").isNull())
        if missing_products.count() > 0:
            print("⚠️ Attention : Certains produits CEGID ne sont pas trouvés dans Dim_Product !")
            missing_products.show(truncate=False)

        # --- Transformation de Sale_ID ---
        if "Sale_ID" in df_cegid.columns:
            # Extraire des parties du Sale_ID
            df_cegid = df_cegid.withColumn("store_id_from_sale", substring(col("Sale_ID"), 1, 4))
            df_cegid = df_cegid.withColumn("year_month_from_sale", substring(col("Sale_ID"), 5, 4))
            # Extraction de l'année et du mois à partir de Transaction_Date
            if "Transaction_Date" in df_cegid.columns:
                df_cegid = df_cegid.withColumn("year_month_from_date", date_format(col("Transaction_Date"), "yyyyMM"))
                df_cegid = df_cegid.withColumn("year_month_yy", substring(col("year_month_from_date"), 3, 4))
            # Liste des identifiants de boutiques valides
            valid_stores = ["PA01", "PA02", "PA03", "BO01", "BO02", "MO01", "LY01", "LY02", "MA01", "LI01", "RE01", "ST01", "CL01"]
            # Correction du store_id en fonction de store_id_from_sale
            df_cegid = df_cegid.withColumn("store_id",
                when(col("store_id_from_sale").isin(valid_stores), col("store_id_from_sale"))
                .when(col("store_id_from_sale").startswith("XXMO"), "MO01")
                .when(col("store_id_from_sale").startswith("XXCL"), "CL01")
                .when(col("store_id_from_sale").startswith("XXLI"), "LI01")
                .when(col("store_id_from_sale").startswith("XXRE"), "RE01")
                .when(col("store_id_from_sale").startswith("XXST"), "ST01")
                .when(col("store_id_from_sale").startswith("XXPA"), "PA01")
                .when(col("store_id_from_sale").startswith("XXBO"), "BO01")
                .when(col("store_id_from_sale").startswith("XXLY"), "LY01")
                .otherwise("UNKNOWN")
            )
            # Correction de Sale_ID si store_id ou l'année/mois ne correspondent pas
            df_cegid = df_cegid.withColumn("Sale_ID",
                when(
                    (~col("store_id_from_sale").isin(valid_stores)) |
                    ((col("year_month_from_sale") != col("year_month_yy")) & col("year_month_from_sale").isNotNull()),
                    concat_ws("", col("store_id"), col("year_month_yy"), substring(col("Sale_ID"), 9, 5))
                ).otherwise(col("Sale_ID"))
            )

            # --- Correction des doublons de Sale_ID ---
            # Utilisation d'une fenêtre pour numéroter les occurrences de chaque Sale_ID
            window_spec = Window.partitionBy("Sale_ID").orderBy("Transaction_Date")
            df_cegid = df_cegid.withColumn("row_num", row_number().over(window_spec))
            # Si row_num > 1, on ajoute un suffixe pour garantir l'unicité.
            df_cegid = df_cegid.withColumn(
                "Sale_ID",
                when(col("row_num") == 1, col("Sale_ID"))
                .otherwise(concat(col("Sale_ID"), lit("_"), col("row_num")))
            )
            df_cegid = df_cegid.drop("row_num")
            # Fin de la correction des doublons

        # Uniformisation de l'email en minuscules
        if "Email" in df_cegid.columns:
            df_cegid = df_cegid.withColumn("Email", when(col("Email").isNotNull(), lower(col("Email"))).otherwise(None))

        # Sélection des colonnes finales
        df_cegid = df_cegid.select("Product_Name", "Email", "Price", "Quantity", "Sale_ID", "Transaction_Date")
        logger.info("Transformation CEGID terminée.")
        df_cegid.show(10, truncate=False)
        return df_cegid

    # ---------------------------------------------------------------------------
    # Transformation du DataFrame des produits
    # ---------------------------------------------------------------------------
    def transform_products(self, df_products):
        """
        Renomme et convertit les colonnes du DataFrame des produits.
        """
        df_products = df_products.withColumnRenamed("product_id", "Product_ID") \
                                  .withColumnRenamed("product_name", "Name") \
                                  .withColumnRenamed("price", "Price") \
                                  .withColumnRenamed("category", "Category")
        df_products = df_products.withColumn("Price", col("Price").cast(DoubleType()))
        logger.info("Transformation des produits terminée.")
        df_products.show(10, truncate=False)
        return df_products

    # ---------------------------------------------------------------------------
    # Transformation du DataFrame des boutiques
    # ---------------------------------------------------------------------------
    def transform_boutiques(self, df_boutiques):
        """
        Nettoie la colonne Address du DataFrame des boutiques.
        """
        if "Address" in df_boutiques.columns:
            df_boutiques = df_boutiques.withColumn(
                "Address", trim(regexp_replace(col("Address"), r'^[\s"]+|[\s"]+$', ''))
            )
        logger.info("Transformation des boutiques terminée.")
        df_boutiques.show(10, truncate=False)
        return df_boutiques
