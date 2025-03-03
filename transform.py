# transform.py
import logging
import re
from pyspark.sql.functions import (
    col,
    trim,
    regexp_replace,
    lower,
    to_date,
    when,
    length,
    lit,
    concat,
    row_number,
    substring,
    concat_ws,
    first,
    isnan,
    round,
    expr,
    date_format,
    regexp_extract,
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql import Window

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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
            df_sfcc = df_sfcc.withColumn(
                column, trim(regexp_replace(col(column), r"[\t\r\n]+", " "))
            )

        # Uniformisation de l'email en minuscules et suppresion des caractères spéciaux
        if "Email" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn("Email", lower(col("Email")))


        # Nettoyage de l'adresse (suppression des guillemets et espaces en début/fin)
        if "Address" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn(
                "Address", trim(regexp_replace(col("Address"), r'^[\s"]+|[\s"]+$', ""))
            )
            df_sfcc = df_sfcc.withColumn(
                "Address", trim(regexp_replace(col("Address"), '"', ""))
            )

        # Standardisation du numéro de téléphone
        if "Phone" in df_sfcc.columns:
            # Supprime les zéros en début de chaîne
            df_sfcc = df_sfcc.withColumn(
                "Phone", regexp_replace(col("Phone"), r"^0+", "")
            )
            # Conversion en string
            df_sfcc = df_sfcc.withColumn("Phone", col("Phone").cast("string"))
            # Format +33XXXXXXXXX si exactement 9 chiffres
            df_sfcc = df_sfcc.withColumn(
                "Phone",
                when(
                    (length(col("Phone")) == 9) & (col("Phone").rlike("^[0-9]+$")),
                    concat(lit("+33"), col("Phone")),
                ).otherwise(lit(None)),
            )

        # Conversion de Customer_ID en entier (si présent)
        if "Customer_ID" in df_sfcc.columns:
            df_sfcc = df_sfcc.withColumn(
                "Customer_ID", col("Customer_ID").cast(IntegerType())
            )

        # Jointure avec df_products pour récupérer le Price à partir de Product_ID
        if df_products is not None:
            # On s'assure que chaque Product_ID est unique dans df_products
            df_products = df_products.dropDuplicates(["Product_ID"])
            df_sfcc = df_sfcc.join(
                df_products.select(col("Product_ID").alias("prod_id"), "Price"),
                df_sfcc["Product_ID"] == col("prod_id"),
                "left",
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
        - Harmonise les colonnes avec SFCC
        - Convertit Sale_ID et Store_ID correctement
        - Associe chaque `Product_Name` à un `Product_ID` (obligatoire pour Fact_Sales)
        - Vérifie que `Price` est valide et cohérent
        - Corrige les doublons sur Sale_ID
        """

        # 🟢 1. Renommage des colonnes
        df_cegid = (
            df_cegid.withColumnRenamed("quantity", "Quantity")
            .withColumnRenamed("sale_id", "Sale_ID")
            .withColumnRenamed("email", "Email")
            .withColumnRenamed("transaction_date", "Transaction_Date")
            .withColumnRenamed("product_name", "Product_Name")
            .withColumnRenamed("price", "Price")
        )

        # 🟢 2. Convertir `Transaction_Date` et `Quantity`
        df_cegid = df_cegid.withColumn(
            "Transaction_Date", to_date(col("Transaction_Date"), "yyyy-MM-dd")
        )
        df_cegid = df_cegid.withColumn("Quantity", col("Quantity").cast(IntegerType()))

        # 🟢 3. Vérification du `Price`
        df_cegid = df_cegid.withColumn(
            "Price",
            when(
                (col("Price").isNull())
                | (trim(col("Price")) == "")
                | (col("Price") == "X")
                | (col("Price").cast("double").isNull()),
                None,  # Si le prix est invalide, il sera NULL (on pourra l'exclure ou le corriger après)
            ).otherwise(col("Price").cast(DoubleType())),
        )

        # 🚀 **4. Conversion `Product_Name` → `Product_ID`**
        df_cegid = df_cegid.join(
            df_products.select(col("Name").alias("prod_name"), col("Product_ID")),
            df_cegid["Product_Name"] == col("prod_name"),
            "left",
        ).drop("prod_name")

        # 🚀 **4.1 Correction des prix manquants**
        df_cegid = df_cegid.join(
            df_products.select(
                col("Name").alias("prod_name"), col("Price").alias("Correct_Price")
            ),
            df_cegid["Product_Name"] == col("prod_name"),
            "left",
        ).drop("prod_name")

        df_cegid = df_cegid.withColumn(
            "Price",
            when(
                col("Price").isNull(), col("Correct_Price")
            ).otherwise(  # Remplace les valeurs NULL par le prix du produit
                col("Price")
            ),
        ).drop("Correct_Price")

        # **⚠️ Vérification des produits non trouvés**
        missing_products = df_cegid.filter(col("Product_ID").isNull())
        if missing_products.count() > 0:
            print(
                "⚠️ Attention : Certains produits CEGID ne sont pas trouvés dans Dim_Product !"
            )
            missing_products.show(truncate=False)

        # 🚀 **5. Transformation de Sale_ID et Store_ID**
        df_cegid = df_cegid.withColumn(
            "store_id_from_sale", substring(col("Sale_ID"), 1, 4)
        )
        # 🚀 **Correction du Sale_ID pour remplacer "XXMO" par "MO01" et autres codes similaires**
        df_cegid = df_cegid.withColumn(
            "Sale_ID",
            when(
                col("Sale_ID").startswith("XXMO"),
                concat(lit("MO01"), col("Sale_ID").substr(6, 100)),
            )
            .when(
                col("Sale_ID").startswith("XXCL"),
                concat(lit("CL01"), col("Sale_ID").substr(6, 100)),
            )
            .when(
                col("Sale_ID").startswith("XXLI"),
                concat(lit("LI01"), col("Sale_ID").substr(6, 100)),
            )
            .when(
                col("Sale_ID").startswith("XXRE"),
                concat(lit("RE01"), col("Sale_ID").substr(6, 100)),
            )
            .when(
                col("Sale_ID").startswith("XXST"),
                concat(lit("ST01"), col("Sale_ID").substr(6, 100)),
            )
            .when(
                col("Sale_ID").startswith("XXPA"),
                concat(lit("PA01"), col("Sale_ID").substr(6, 100)),
            )
            .when(
                col("Sale_ID").startswith("XXBO"),
                concat(lit("BO01"), col("Sale_ID").substr(6, 100)),
            )
            .when(
                col("Sale_ID").startswith("XXLY"),
                concat(lit("LY01"), col("Sale_ID").substr(6, 100)),
            )
            .otherwise(col("Sale_ID")),
        )

        # 🚀 **5.1 Correction des store_id invalides**
        store_ids = [
            "PA01",
            "PA02",
            "PA03",
            "BO01",
            "BO02",
            "MO01",
            "LY01",
            "LY02",
            "MA01",
            "LI01",
            "RE01",
            "ST01",
            "CL01",
        ]

        df_cegid = df_cegid.withColumn(
            "store_id",
            when(col("store_id_from_sale").isin(store_ids), col("store_id_from_sale"))
            .when(col("store_id_from_sale").startswith("XXMO"), "MO01")
            .when(col("store_id_from_sale").startswith("XXCL"), "CL01")
            .when(col("store_id_from_sale").startswith("XXLI"), "LI01")
            .when(col("store_id_from_sale").startswith("XXRE"), "RE01")
            .when(col("store_id_from_sale").startswith("XXST"), "ST01")
            .when(col("store_id_from_sale").startswith("XXPA"), "PA01")
            .when(col("store_id_from_sale").startswith("XXBO"), "BO01")
            .when(col("store_id_from_sale").startswith("XXLY"), "LY01")
            .otherwise(None),  # Pour diagnostic, permet d'identifier les erreurs
        ).drop("store_id_from_sale")

        # 🚀 **6. Correction des doublons `Sale_ID`**
        window_spec = Window.partitionBy("Sale_ID", "store_id").orderBy(
            "Transaction_Date"
        )
        df_cegid = df_cegid.withColumn("row_num", row_number().over(window_spec))
        df_cegid = df_cegid.withColumn(
            "Sale_ID",
            when(col("row_num") == 1, col("Sale_ID")).otherwise(
                concat_ws("_", col("Sale_ID"), col("row_num"))
            ),
        ).drop("row_num")

        # 🚀 **7. Uniformisation des emails**
        df_cegid = df_cegid.withColumn("Email", lower(col("Email")))

        # 🚀 **8. Sélection finale**
        df_cegid = df_cegid.select(
            "Product_ID",
            "Email",
            "Price",
            "Quantity",
            "Sale_ID",
            "Transaction_Date",
            "store_id",
            "Product_Name",
        )

        print("✅ Transformation CEGID terminée.")
        df_cegid.show(10, truncate=False)

        return df_cegid

    # ---------------------------------------------------------------------------
    # Transformation du DataFrame des produits
    # ---------------------------------------------------------------------------
    def transform_products(self, df_products):
        """
        Renomme et convertit les colonnes du DataFrame des produits.
        """
        df_products = (
            df_products.withColumnRenamed("product_id", "Product_ID")
            .withColumnRenamed("product_name", "Name")
            .withColumnRenamed("price", "Price")
            .withColumnRenamed("category", "Category")
        )
        df_products = df_products.withColumn("Price", col("Price").cast(DoubleType()))

        # TODO: Supprimer les produits en double
        df_products = df_products.dropDuplicates(["Product_ID"])
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
                "Address", trim(regexp_replace(col("Address"), r'^[\s"]+|[\s"]+$', ""))
            )
        logger.info("Transformation des boutiques terminée.")
        df_boutiques.show(10, truncate=False)
        return df_boutiques
