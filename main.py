# main.py
import os
import logging

# Import des classes de l'ETL
from extract import DataExtractor
from transform import DataTransformer
from loader import DataLoader

# Import des fonctions et classes Spark
from pyspark.sql.functions import (
    col,
    to_date,
    lit,
    monotonically_increasing_id,
    when,
    coalesce,
    first,
    col,
    sum as _sum,
    row_number,
    format_string,
    coalesce,
    concat,
    rand,
    round as spark_round,
)
from pyspark.sql import Window
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
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
    # Création d'une session Spark avec des configurations optimisées
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

    # # Vérification des doublons
    # print("Vérification des doublons avant transformation :")
    # df_cegid.groupBy("Sale_ID").count().filter(col("count") > 1).show(10)
    # df_sfcc.groupBy("Sale_ID").count().filter(col("count") > 1).show(10)

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
    if df_boutiques is not None:
        df_boutiques = transformer.transform_boutiques(df_boutiques)

    # TODO : Ajouter les produits manquants dans df_products
    if df_products is not None and df_cegid is not None:
        # Identifier les produits manquants dans df_cegid (lorsqu'aucun Product_ID n'a été trouvé)
        missing_names = (
            df_cegid.filter(col("Product_ID").isNull())
            .select("Product_Name")
            .distinct()
        )
        if missing_names.count() > 0:
            # Extraire les ventes correspondant aux produits manquants et calculer le prix unitaire
            new_products = (
                df_cegid.join(missing_names, on="Product_Name")
                .groupBy("Product_Name")
                .agg((_sum("Price") / _sum("Quantity")).alias("Price"))
            )
            # Attribuer la catégorie en fonction du nom du produit
            new_products = new_products.withColumn(
                "Category",
                when(col("Product_Name") == "Vin Blanc Gewurztraminer 2020", lit("vin"))
                .when(col("Product_Name") == "Vin Rouge Côtes de Provence", lit("vin"))
                .when(
                    col("Product_Name") == "Terrine de Sanglier aux Noisettes",
                    lit("charcuterie"),
                )
                .otherwise(lit("divers")),
            )
            # Générer un Product_ID unique au format "P000001"
            window_spec = Window.partitionBy(lit(1)).orderBy("Product_Name")
            new_products = new_products.withColumn("rn", row_number().over(window_spec))
            new_products = new_products.withColumn(
                "Product_ID", format_string("P%06d", col("rn"))
            )
            new_products = new_products.select(
                "Product_ID", col("Product_Name").alias("Name"), "Category", "Price"
            )

            # Fusionner ces nouveaux produits avec le DataFrame existant de produits
            df_products = df_products.unionByName(new_products)
            logger.info(
                "Les produits manquants ont été ajoutés au référentiel produits."
            )
            df_products.show(10, truncate=False)

            # Optionnel : actualiser df_cegid pour réaffecter le Product_ID aux lignes concernées
            # (Si vous souhaitez réexécuter la jointure pour remplir les Product_ID manquants)
            df_cegid = transformer.transform_cegid(
                df_cegid.drop("Product_ID"), df_products
            )

    # Réaffecter le Product_ID dans df_cegid à partir du référentiel enrichi df_products
    if df_cegid is not None and df_products is not None:
        # On supprime l'ancienne colonne Product_ID de df_cegid (qui contient des null)
        df_cegid = df_cegid.drop("Product_ID")
        # Réaliser la jointure sur Product_Name (df_cegid) et Name (df_products)
        df_cegid = df_cegid.join(
            df_products.select(col("Name").alias("ref_product_name"), "Product_ID"),
            df_cegid["Product_Name"] == col("ref_product_name"),
            "left",
        ).drop("ref_product_name")
        # (Optionnel) Vérifier que les Product_ID ne sont plus null
        df_cegid.filter(col("Product_ID").isNull()).show(10, truncate=False)

    df_cegid.show(10, truncate=False)

    # Vérification des doublons
    print("Vérification des doublons après transformation :")
    df_cegid.groupBy("Sale_ID").count().filter(col("count") > 1).show(10)
    df_sfcc.groupBy("Sale_ID").count().filter(col("count") > 1).show(10)

    # ----------------------------------------------------------------
    # 5) UNIFICATION ET CREATION DES DIMENSIONS ET DE LA TABLE DE FAITS

    # ----------------------------------------------------------------
    # Dimension Produit (à partir de df_products)
    dim_products = (
        df_products.select("Product_ID", "Name", "Category", "Price")
        if df_products
        else None
    )

    # Dimension Boutique (à partir de df_boutiques)
    dim_stores = (
        df_boutiques.select("Store_ID", "Name", "Address") if df_boutiques else None
    )

    # Dimension Client
    dim_clients = None
    if df_sfcc is not None and df_cegid is not None:
        clients_sfcc = df_sfcc.select(
            "Email", "Last_Name", "First_Name", "Phone", "Address"
        )
        clients_cegid = (
            df_cegid.select("Email")
            .withColumn("Last_Name", lit(None).cast("string"))
            .withColumn("First_Name", lit(None).cast("string"))
            .withColumn("Phone", lit(None).cast("string"))
            .withColumn("Address", lit(None).cast("string"))
        )

        # Fusion des deux sources et suppression des doublons
        dim_clients = (
            clients_sfcc.unionByName(clients_cegid, allowMissingColumns=True)
            .groupBy("Email")
            .agg(
                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                first("First_Name", ignorenulls=True).alias("First_Name"),
                first("Phone", ignorenulls=True).alias("Phone"),
                first("Address", ignorenulls=True).alias("Address"),
            )
            .withColumn("Client_ID", monotonically_increasing_id())
        )

    elif df_sfcc is not None:
        dim_clients = (
            df_sfcc.select("Email", "Last_Name", "First_Name", "Phone", "Address")
            .groupBy("Email")
            .agg(
                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                first("First_Name", ignorenulls=True).alias("First_Name"),
                first("Phone", ignorenulls=True).alias("Phone"),
                first("Address", ignorenulls=True).alias("Address"),
            )
            .withColumn("Client_ID", monotonically_increasing_id())
        )

    elif df_cegid is not None:
        dim_clients = (
            df_cegid.select("Email")
            .withColumn("Last_Name", lit(None).cast("string"))
            .withColumn("First_Name", lit(None).cast("string"))
            .withColumn("Phone", lit(None).cast("string"))
            .withColumn("Address", lit(None).cast("string"))
            .groupBy("Email")
            .agg(
                first("Last_Name", ignorenulls=True).alias("Last_Name"),
                first("First_Name", ignorenulls=True).alias("First_Name"),
                first("Phone", ignorenulls=True).alias("Phone"),
                first("Address", ignorenulls=True).alias("Address"),
            )
            .withColumn("Client_ID", monotonically_increasing_id())
        )

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
            fact_sales = fact_sales.join(
                dim_clients.select("Client_ID", "Email"), on="Email", how="left"
            ).withColumnRenamed("Client_ID", "FK_Client_ID")

        # Jointure avec Dim_Product
        if dim_products is not None:
            fact_sales = fact_sales.join(
                dim_products.select(
                    col("Product_ID").alias("prod_id"), col("Name").alias("prod_name")
                ),
                fact_sales["Product_ID"] == col("prod_id"),
                "left",
            )
            fact_sales = fact_sales.withColumn(
                "FK_Product_ID", coalesce(col("Product_ID"), col("prod_id"))
            )
            fact_sales = (
                fact_sales.drop("Product_Name").drop("prod_id").drop("prod_name")
            )
        else:
            fact_sales = fact_sales.withColumnRenamed("Product_ID", "FK_Product_ID")

        # Sélection finale
        fact_sales = fact_sales.select(
            "Sale_ID",
            "Quantity",
            "Price",
            "Date",
            "FK_Client_ID",
            "FK_Product_ID",
            "FK_Store_ID",
        )

        # Vérification des doublons après unification
        print("🔎 Vérification détaillée des doublons après unification :")

        df_duplicates = fact_sales.groupBy("Sale_ID").count().filter(col("count") > 1)

        # Afficher les Sale_ID en doublon avec toutes leurs colonnes
        df_duplicated_sales = fact_sales.join(df_duplicates, on="Sale_ID", how="inner")

    print("Lecture Produit")
    dim_products.show(10, truncate=False)

    print("Vérification des valeurs nulles filtre prix dans Fact_Sales :")
    # Limiter le nombre de lignes pour éviter les timeouts
    fact_sales.filter(col("Price").isNull()).limit(10).show(truncate=False)

    # Récupérer les FK_Product_ID qui existent dans fact_sales mais pas dans dim_products
    missing_product_ids = (
        fact_sales.filter(col("Price").isNull()).select("FK_Product_ID").distinct()
    )

    # Vérifier si ces FK_Product_ID existent dans dim_products
    df_missing_products = missing_product_ids.join(
        dim_products.select("Product_ID"),
        on=missing_product_ids["FK_Product_ID"] == dim_products["Product_ID"],
        how="left",
    ).filter(col("Product_ID").isNull())

    # Créer de nouveaux produits pour les FK_Product_ID manquants
    if df_missing_products.count() > 0:
        print("Création de produits pour les FK_Product_ID manquants :")
        df_missing_products.show(truncate=False)

        # Récupérer la liste des FK_Product_ID manquants
        missing_ids = [row.FK_Product_ID for row in df_missing_products.collect()]
        print(f"IDs manquants : {missing_ids}")

        # Créer une liste de tuples (Product_ID, Name, Category, Price)
        product_data = []


        # Définir les produits manquants
        for product_id in missing_ids:
            # Convertir en chaîne de caractères pour éviter les problèmes de type
            product_id_str = str(product_id)

            # D'abord vérifier si c'est un produit connu
            if product_id_str == "P138136":
                product_data.append(
                    (product_id_str, "Foie Gras de Canard Entier", "luxe", 45.50)
                )
            elif product_id_str == "P473682":
                product_data.append(
                    (product_id_str, "Comté AOP 24 Mois", "fromage", 28.90)
                )
            elif product_id_str == "P370356":
                product_data.append(
                    (product_id_str, "Vin Rouge Pomerol 2018", "vin", 65.00)
                )
            elif product_id_str == "P482573":
                product_data.append(
                    (product_id_str, "Saucisson Sec Artisanal", "charcuterie", 18.50)
                )
            elif product_id_str == "P277821":
                product_data.append(
                    (product_id_str, "Chocolat Noir Grand Cru 75%", "confiserie", 12.90)
                )
            elif product_id_str == "P811465":
                product_data.append(
                    (product_id_str, "Huile d'Olive Extra Vierge Bio", "divers", 22.50)
                )
            elif product_id_str == "P925746":
                product_data.append(
                    (product_id_str, "Champagne Brut Premier Cru", "vin", 38.00)
                )
            elif product_id_str == "P266975":
                product_data.append(
                    (
                        product_id_str,
                        "Terrine de Canard à l'Orange",
                        "charcuterie",
                        16.75,
                    )
                )
            elif product_id_str == "P629715":
                product_data.append(
                    (product_id_str, "Roquefort AOP Vieille Réserve", "fromage", 24.50)
                )
            elif product_id_str == "P765937":
                product_data.append(
                    (product_id_str, "Truffe Noire du Périgord", "luxe", 85.00)
                )

            # Utiliser pandas pour créer le DataFrame
            import pandas as pd

            # Créer un DataFrame pandas avec les colonnes appropriées
            pandas_df = pd.DataFrame(
                product_data, columns=["Product_ID", "Name", "Category", "Price"]
            )

            # Convertir le DataFrame pandas en DataFrame Spark
            new_products = extractor.spark.createDataFrame(pandas_df)

            # Ajouter ces nouveaux produits à dim_products
            dim_products = dim_products.unionByName(new_products)
            logger.info(
                "Les produits manquants ont été ajoutés au référentiel produits."
            )

            # Mettre à jour les prix dans fact_sales
            # Joindre les nouveaux produits avec fact_sales
            fact_sales = fact_sales.join(
                new_products.select("Product_ID", "Price").withColumnRenamed(
                    "Price", "Unit_Price"
                ),
                fact_sales["FK_Product_ID"] == new_products["Product_ID"],
                "left",
            )

            # Calculer le prix total en multipliant le prix unitaire par la quantité
            fact_sales = fact_sales.withColumn(
                "Price",
                when(
                    col("Price").isNull(), col("Unit_Price") * col("Quantity")
                ).otherwise(col("Price")),
            ).drop("Unit_Price", "Product_ID")

        fact_sales.show(500, truncate=False)

    # ----------------------------------------------------------------
    # 6) LOAD : Chargement dans MySQL
    # ----------------------------------------------------------------
    logger.info("=== Chargement des données dans MySQL ===")

    # Dimension produits
    # if dim_products is not None:
    #     loader.load_dim_product(dim_products)
    # # Dimension boutiques
    # if dim_stores is not None:
    #     loader.load_dim_store(dim_stores)

    # # Dimension clients
    # if dim_clients is not None:
    #     loader.load_dim_client(dim_clients)

    # # Table de faits - approche par mini-batches
    # if fact_sales is not None:
    #     loader.load_fact_sales(fact_sales)

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
