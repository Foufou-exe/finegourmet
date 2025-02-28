# main.py
import os
import logging

# Import des classes de l'ETL
from extract import DataExtractor
from transform import DataTransformer
from loader import DataLoader

# Import des fonctions et classes Spark
from pyspark.sql.functions import col, to_date, lit, monotonically_increasing_id, when, coalesce, first, col, sum as _sum, row_number, format_string, coalesce
from pyspark.sql import Window

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

    # Vérification des doublons
    print("Vérification des doublons avant transformation :")
    df_cegid.groupBy("Sale_ID").count().filter(col("count") > 1).show()
    df_sfcc.groupBy("Sale_ID").count().filter(col("count") > 1).show()

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
        df_cegid.show(100, truncate=False )
    if df_boutiques is not None:
        df_boutiques = transformer.transform_boutiques(df_boutiques)

    # TODO : Ajouter les produits manquants dans df_products
    if df_products is not None and df_cegid is not None:
        # Identifier les produits manquants dans df_cegid (lorsqu'aucun Product_ID n'a été trouvé)
        missing_names = df_cegid.filter(col("Product_ID").isNull()).select("Product_Name").distinct()
        if missing_names.count() > 0:
            # Extraire les ventes correspondant aux produits manquants et calculer le prix unitaire
            new_products = df_cegid.join(missing_names, on="Product_Name") \
                                    .groupBy("Product_Name") \
                                    .agg((_sum("Price") / _sum("Quantity")).alias("Price"))
            # Attribuer la catégorie en fonction du nom du produit
            new_products = new_products.withColumn(
                "Category",
                when(col("Product_Name") == "Vin Blanc Gewurztraminer 2020", lit("vin"))
                .when(col("Product_Name") == "Vin Rouge Côtes de Provence", lit("vin"))
                .when(col("Product_Name") == "Terrine de Sanglier aux Noisettes", lit("charcuterie"))
                .otherwise(lit("divers"))
            )
            # Générer un Product_ID unique au format "P000001"
            window_spec = Window.orderBy("Product_Name")
            new_products = new_products.withColumn("rn", row_number().over(window_spec))
            new_products = new_products.withColumn("Product_ID", format_string("P%06d", col("rn")))
            new_products = new_products.select("Product_ID", col("Product_Name").alias("Name"), "Category", "Price")

            # Fusionner ces nouveaux produits avec le DataFrame existant de produits
            df_products = df_products.unionByName(new_products)
            logger.info("Les produits manquants ont été ajoutés au référentiel produits.")
            df_products.show(truncate=False)

            # Optionnel : actualiser df_cegid pour réaffecter le Product_ID aux lignes concernées
            # (Si vous souhaitez réexécuter la jointure pour remplir les Product_ID manquants)
            df_cegid = transformer.transform_cegid(df_cegid.drop("Product_ID"), df_products)

    # Réaffecter le Product_ID dans df_cegid à partir du référentiel enrichi df_products
    if df_cegid is not None and df_products is not None:
        # On supprime l'ancienne colonne Product_ID de df_cegid (qui contient des null)
        df_cegid = df_cegid.drop("Product_ID")
        # Réaliser la jointure sur Product_Name (df_cegid) et Name (df_products)
        df_cegid = df_cegid.join(
            df_products.select(col("Name").alias("ref_product_name"), "Product_ID"),
            df_cegid["Product_Name"] == col("ref_product_name"),
            "left"
        ).drop("ref_product_name")
        # (Optionnel) Vérifier que les Product_ID ne sont plus null
        df_cegid.filter(col("Product_ID").isNull()).show(truncate=False)

    df_cegid.show(200, truncate=False )

    # Vérification des doublons
    print("Vérification des doublons après transformation :")
    df_cegid.groupBy("Sale_ID").count().filter(col("count") > 1).show()
    df_sfcc.groupBy("Sale_ID").count().filter(col("count") > 1).show()

    # ----------------------------------------------------------------
    # 5) UNIFICATION ET CREATION DES DIMENSIONS ET DE LA TABLE DE FAITS

    # ----------------------------------------------------------------
    # Dimension Produit (à partir de df_products)
    dim_products = df_products.select("Product_ID", "Name", "Category", "Price") if df_products else None

    # Dimension Boutique (à partir de df_boutiques)
    dim_stores = df_boutiques.select("Store_ID", "Name", "Address") if df_boutiques else None

    # Dimension Client
    dim_clients = None
    if df_sfcc is not None and df_cegid is not None:
        clients_sfcc = df_sfcc.select("Email", "Last_Name", "First_Name", "Phone", "Address")
        clients_cegid = df_cegid.select("Email") \
            .withColumn("Last_Name", lit(None).cast("string")) \
            .withColumn("First_Name", lit(None).cast("string")) \
            .withColumn("Phone", lit(None).cast("string")) \
            .withColumn("Address", lit(None).cast("string"))

        # Fusion des deux sources et suppression des doublons
        dim_clients = clients_sfcc.unionByName(clients_cegid, allowMissingColumns=True) \
                                .groupBy("Email").agg(
                                    first("Last_Name", ignorenulls=True).alias("Last_Name"),
                                    first("First_Name", ignorenulls=True).alias("First_Name"),
                                    first("Phone", ignorenulls=True).alias("Phone"),
                                    first("Address", ignorenulls=True).alias("Address")
                                ) \
                                .withColumn("Client_ID", monotonically_increasing_id())

    elif df_sfcc is not None:
        dim_clients = df_sfcc.select("Email", "Last_Name", "First_Name", "Phone", "Address") \
                            .groupBy("Email").agg(
                                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                                first("First_Name", ignorenulls=True).alias("First_Name"),
                                first("Phone", ignorenulls=True).alias("Phone"),
                                first("Address", ignorenulls=True).alias("Address")
                            ) \
                            .withColumn("Client_ID", monotonically_increasing_id())

    elif df_cegid is not None:
        dim_clients = df_cegid.select("Email") \
            .withColumn("Last_Name", lit(None).cast("string")) \
            .withColumn("First_Name", lit(None).cast("string")) \
            .withColumn("Phone", lit(None).cast("string")) \
            .withColumn("Address", lit(None).cast("string")) \
            .groupBy("Email").agg(
                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                first("First_Name", ignorenulls=True).alias("First_Name"),
                first("Phone", ignorenulls=True).alias("Phone"),
                first("Address", ignorenulls=True).alias("Address")
            ) \
            .withColumn("Client_ID", monotonically_increasing_id())


    # Table de faits Fact_Sales
    # fact_sales = None
    # if df_sfcc is not None and df_cegid is not None:
    #     fact_sales = df_sfcc.unionByName(df_cegid, allowMissingColumns=True)
    # elif df_sfcc is not None:
    #     fact_sales = df_sfcc
    # elif df_cegid is not None:
    #     fact_sales = df_cegid

    fact_sales = df_sfcc.unionByName(df_cegid, allowMissingColumns=True)


    print("Lecture cegid")
    # fact_sales.show(500, truncate=False)

    if fact_sales is not None:
        fact_sales = fact_sales.withColumnRenamed("Transaction_Date", "Date")

        # TODO: Permet de recupérer ID de la boutique poru le lier à la vente
        # Correction du FK_Store_ID pour les ventes physiques
        if df_cegid is not None:
            fact_sales = fact_sales.withColumn("FK_Store_ID", col("Store_ID"))


        # **Correction ici** : Jointure avec Dim_Client et renommage correct
        # Correction de FK_Client_ID pour éviter les doublons
        if dim_clients is not None:
            fact_sales = fact_sales.join(dim_clients.select("Client_ID", "Email"), on="Email", how="left") \
                                .withColumnRenamed("Client_ID", "FK_Client_ID")

        # Jointure avec Dim_Product
        if dim_products is not None:
            fact_sales = fact_sales.join(
                dim_products.select(col("Product_ID").alias("prod_id"), col("Name").alias("prod_name")),
                fact_sales["Product_ID"] == col("prod_id"),
                "left"
            )
            fact_sales = fact_sales.withColumn("FK_Product_ID", coalesce(col("Product_ID"), col("prod_id")))
            fact_sales = fact_sales.drop("Product_Name").drop("prod_id").drop("prod_name")
        else:
            fact_sales = fact_sales.withColumnRenamed("Product_ID", "FK_Product_ID")

        # Sélection finale
        fact_sales = fact_sales.select("Sale_ID", "Quantity", "Price", "Date", "FK_Client_ID", "FK_Product_ID", "FK_Store_ID")

        # Vérification des doublons après unification
        print("🔎 Vérification détaillée des doublons après unification :")

        df_duplicates = fact_sales.groupBy("Sale_ID").count().filter(col("count") > 1)

        # Afficher les Sale_ID en doublon avec toutes leurs colonnes
        df_duplicated_sales = fact_sales.join(df_duplicates, on="Sale_ID", how="inner")

    #     import pandas as pd
    #     df_pandas = df_duplicated_sales.toPandas()
    #     pd.set_option('display.max_rows', None)  # Afficher toutes les lignes
    #     pd.set_option('display.max_columns', None)  # Afficher toutes les colonnes
    #     pd.set_option('display.width', None)  # Éviter la coupure des lignes

    # print(df_pandas)




    print("Vérification des valeurs nulles filtre prix dans Fact_Sales :")
    fact_sales.filter(col("Price").isNull()).show(500, truncate=False)
    # fact_sales.show(500, truncate=False )


    print("Vérification des valeurs nulles dans Dim_Product :")
    # Récupérer les Product_ID manquants dans df_products
    missing_products = fact_sales.filter(col("Price").isNull()).select("FK_Product_ID").distinct()

    # Vérifier si ces Product_ID existent dans df_products
    df_missing_products = missing_products.join(
        df_products.select("Product_ID"),
        on=missing_products["FK_Product_ID"] == df_products["Product_ID"],
        how="left"
    ).filter(col("Product_ID").isNull())

    # Afficher les Product_ID manquants
    df_missing_products.select("FK_Product_ID").show(truncate=False)


    # ----------------------------------------------------------------
    # 6) LOAD : Chargement dans MySQL
    # ----------------------------------------------------------------
    logger.info("=== Chargement des données dans MySQL ===")
    # if dim_products is not None:
    #     loader.load_dim_product(dim_products)
    # if dim_stores is not None:
    #     loader.load_dim_store(dim_stores)
    # if dim_clients is not None:
    #     loader.load_dim_client(dim_clients)
    # if fact_sales is not None:
    #     loader.load_fact_sales(fact_sales)

    # ----------------------------------------------------------------
    # 7) Arrêt de Spark
    # ----------------------------------------------------------------
    extractor.stop()
    logger.info("✅ ETL terminé avec succès.")

if __name__ == "__main__":
    main()
    exit(0)
