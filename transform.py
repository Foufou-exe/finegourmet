# transform.py
import logging
from pyspark.sql.functions import (
    col, trim, regexp_replace, lower, to_date, when, length, lit, concat, row_number, substring, concat_ws, first, isnan, round, expr
)
from pyspark.sql.types import BooleanType, IntegerType, DoubleType
from pyspark.sql import Window

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

##### Code transformation Thomas #######
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


        #############################################################################################
        ######################### Transformation et Correction de Price #############################
        #############################################################################################

        if "Price" in df_cegid.columns and "Quantity" in df_cegid.columns and "Product_Name" in df_cegid.columns:
            # 🏷️ Filtrer les valeurs valides de Price pour calculer price_unitaire
            df_price_lookup = df_cegid.filter(
                (col("Price").isNotNull()) &
                (~isnan(col("Price"))) &
                (trim(col("Price")) != "") &
                (col("Price") != "X") &
                (col("Price").cast("double").isNotNull())
            ).withColumn(
                "price_unitaire",
                (col("Price") / col("Quantity")).cast("double")
            )

            # 🏷️ Agréger le premier price_unitaire valide pour chaque Product_Name
            df_price_lookup = df_price_lookup.groupBy("Product_Name").agg(
                first("price_unitaire", ignorenulls=True).alias("ref_price_unitaire")
            )

            # 🏷️ Joindre la table des prix de référence sur le DataFrame principal
            df_cegid = df_cegid.join(df_price_lookup, on="Product_Name", how="left")

            # 🏷️ Remplacer les valeurs invalides de Price par ref_price_unitaire * Quantity
            df_cegid = df_cegid.withColumn("Price",
                when(
                    (col("Price").isNull()) |
                    (trim(col("Price")) == "") |
                    (col("Price") == "X") |
                    (col("Price").cast("double").isNull()),
                    round(col("ref_price_unitaire") * col("Quantity"), 2)  # Remplacement par `price_unitaire * Quantity`
                ).otherwise(col("Price").cast("double"))  # Sinon, garder la valeur existante
            )

            # 🏷️ Supprimer la colonne temporaire ref_price_unitaire
            df_cegid = df_cegid.drop("ref_price_unitaire")

        #############################################################################################
        ######################### Fin transformation de Price #######################################
        #############################################################################################



        ######################################################################################
        ######################### Transformation de Sale_ID ##################################
        ######################################################################################

        if "Sale_ID" in df_cegid.columns:
            # Extraire les parties du sale_id
            df_cegid = df_cegid.withColumn("store_id_from_sale", substring(col("Sale_ID"), 1, 4))
            df_cegid = df_cegid.withColumn("year_month_from_sale", substring(col("Sale_ID"), 5, 4))

            # Extraire l'année et le mois corrects depuis transaction_date
            if "Transaction_Date" in df_cegid.columns:
                df_cegid = df_cegid.withColumn("year_month_from_date", expr("date_format(Transaction_Date, 'yyyyMM')"))
                df_cegid = df_cegid.withColumn("year_month_yy", expr("substring(year_month_from_date, 3, 4)"))

            # Liste des stores valides
            store_ids = ["PA01", "PA02", "PA03", "BO01", "BO02", "MO01", "LY01", "LY02", "MA01", "LI01", "RE01", "ST01", "CL01"]

            # Correction des store_id invalides
            df_cegid = df_cegid.withColumn("store_id",
                when(col("store_id_from_sale").isin(store_ids), col("store_id_from_sale"))
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

            # Correction du Sale_ID si store_id ou year_month incorrect
            df_cegid = df_cegid.withColumn("Sale_ID",
                when(
                    (~col("store_id_from_sale").isin(store_ids)) |
                    ((col("year_month_from_sale") != col("year_month_yy")) & col("year_month_from_sale").isNotNull()),
                    concat_ws("", col("store_id"), col("year_month_yy"), substring(col("Sale_ID"), 9, 5))
                ).otherwise(col("Sale_ID"))
            )

                #############################################################################################
                ######################### Correction des doublons de Sales_ID ################################
                #############################################################################################

            # Création d'une fenêtre pour numéroter les doublons
            window_spec = Window.partitionBy("Sale_ID").orderBy("Transaction_Date")
            df_cegid = df_cegid.withColumn("row_num", row_number().over(window_spec))

            # Fonction pour incrémenter les doublons
            def increment_last_number(df):
                # Récupérer les Sale_ID uniques
                existing_sales_ids = set(row.Sale_ID for row in df.select("Sale_ID").distinct().collect())

                processed_df = df
                for sale_id in df.filter(col("row_num") > 1).select("Sale_ID").distinct().collect():
                    base_id = sale_id.Sale_ID[:-2]  # Prendre tout sauf les 2 derniers chiffres

                    # Trouver le prochain numéro disponible
                    i = 1
                    while base_id + str(i).zfill(2) in existing_sales_ids:
                        i += 1

                    # Mise à jour du Sale_ID avec un suffixe unique
                    processed_df = processed_df.withColumn(
                        "Sale_ID",
                        when(
                            (col("Sale_ID") == sale_id.Sale_ID) & (col("row_num") > 1),
                            expr(f"'{base_id}' || lpad('{i}', 2, '0')")
                        ).otherwise(col("Sale_ID"))
                    )

                    # Ajouter le nouveau Sale_ID dans l'ensemble existant
                    existing_sales_ids.add(base_id + str(i).zfill(2))

                return processed_df

            # Appliquer l'incrémentation
            df_cegid = increment_last_number(df_cegid)

            # Supprimer la colonne temporaire
            df_cegid = df_cegid.drop("row_num")

        ######################################################################################
        ######################### Fin transformation Sale_ID #################################
        ######################################################################################

        # Mise en minuscules pour l'email afin d'uniformiser et valeurs nulls conservées.
        if "Email" in df_cegid.columns:
            df_cegid = df_cegid.withColumn(
            "Email",
             when(col("Email").isNotNull(), lower(col("Email"))).otherwise(None)
             )

        df_cegid = df_cegid.select("Product_Name", "Email", "Price", "Quantity", "Sale_ID", "Transaction_Date")

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
