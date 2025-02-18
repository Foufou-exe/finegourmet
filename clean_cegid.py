from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, expr, concat_ws, col, when, lpad, length, round, trim, first, to_date

# Initialiser Spark
spark = SparkSession.builder.appName("CleanCegidSales").getOrCreate()

# Charger les données JSON avec mode multiligne
print("\n📥 Chargement du fichier JSON...")
df = spark.read.option("multiline", "true").json("data/cegid/2024_cegid_sales.json")


# Vérifier si le fichier est bien chargé
print("\n📊 Structure des données :")
df.printSchema()

# Vérifier les 5 premières lignes
print("\n🔍 Aperçu des données brutes :")
df.show(5, truncate=False)

# Vérifier si des erreurs de chargement existent (_corrupt_record)
if "_corrupt_record" in df.columns:
    print("\n⚠️ ATTENTION : Des lignes corrompues ont été détectées !")
    df.select("_corrupt_record").show(10, truncate=False)

# Charger en brut pour voir le contenu
df_raw = spark.read.text("data/cegid/2024_cegid_sales.json")
print("\n📜 Aperçu du fichier brut (premières lignes) :")
df_raw.show(10, truncate=False)

# Vérifier le nombre de lignes avant transformation
print(f"\n📏 Nombre total de lignes avant transformation : {df.count()}")


#############################################################################################
######################### Modification mail #################################################
#############################################################################################

# 🏷️ Remplacer les emails vides ou NULL par "non_renseigné"
df = df.withColumn("email",
    when(col("email").isNull(), "non_renseignée")  # NULL → "non_renseigné"
    .when(trim(col("email")) == "", "non_renseignée")  # " " (vide) → "non_renseigné"
    .otherwise(col("email"))  # Sinon, on garde l'email existant
)

# ✅ Vérification après transformation
print("\n✅ Vérification de la colonne email :")
df.select("email").show(10, truncate=False)


#############################################################################################
######################### Modification DATE #################################################
#############################################################################################

# 🏷️ Convertir transaction_date en format date
df = df.withColumn("transaction_date_clean", to_date(col("transaction_date"), "yyyy-MM-dd"))

# 🔍 Vérifier les dates mal formatées
df_invalid_dates = df.filter(col("transaction_date_clean").isNull() & col("transaction_date").isNotNull())

print("\n⚠️ Anomalies détectées dans transaction_date :")
df_invalid_dates.select("transaction_date").show(20, truncate=False)

# 🔍 Vérifier les dates dans le futur (ex: après aujourd’hui)
from pyspark.sql.functions import current_date

df_future_dates = df.filter(col("transaction_date_clean") > current_date())

print("\n⚠️ Dates futures suspectes :")
df_future_dates.select("transaction_date").show(20, truncate=False)


#############################################################################################
######################### Modification sale_id ##############################################
#############################################################################################


# Extraire les 4 premiers caractères (store_id) et l'année/mois de sale_id
df = df.withColumn("store_id_from_sale", substring(col("sale_id"), 1, 4))
df = df.withColumn("year_month_from_sale", substring(col("sale_id"), 5, 4))

# Vérifier après extraction
print("\n✅ Vérification après extraction des IDs :")
df.select("sale_id", "store_id_from_sale", "year_month_from_sale").show(5, truncate=False)

# Extraire l'année et le mois corrects à partir de transaction_date
df = df.withColumn("year_month_from_date", expr("date_format(transaction_date, 'yyyyMM')"))



# 🎯 Transformer le format YYYYMM en YYMM
df = df.withColumn("year_month_yy", expr("substring(year_month_from_date, 3, 4)"))  # Prend YYMM


# ✅ Vérification après extraction de l'année/mois
print("\n✅ Vérification après extraction de year_month_from_date :")
df.select("transaction_date", "year_month_from_date", "year_month_yy").show(5, truncate=False)


# Correction du sale_id si store_id incorrect ou année/mois incorrects
store_ids = ["PA01", "PA02", "PA03", "BO01", "BO02", "MO01", "LY01", "LY02", "MA01", "LI01", "RE01", "ST01", "CL01"]



# 🏪 Correction FORCÉE des store_id mal formés
df = df.withColumn("store_id_from_sale", substring(col("sale_id"), 1, 4))  # Prendre toujours les 4 premiers caractères

df = df.withColumn("store_id_corrected",
    when(col("store_id_from_sale").isin(store_ids), col("store_id_from_sale"))  # Si déjà correct, on garde
    .when(col("store_id_from_sale").startswith("XXMO"), "MO01")
    .when(col("store_id_from_sale").startswith("XXCL"), "CL01")
    .when(col("store_id_from_sale").startswith("XXLI"), "LI01")
    .when(col("store_id_from_sale").startswith("XXRE"), "RE01")
    .when(col("store_id_from_sale").startswith("XXST"), "ST01")
    .when(col("store_id_from_sale").startswith("XXPA"), "PA01")  # PA02 et PA03 → PA01
    .when(col("store_id_from_sale").startswith("XXBO"), "BO01")  # BO02 → BO01
    .when(col("store_id_from_sale").startswith("XXLY"), "LY01")  # LY02 → LY01
    .otherwise("UNKNOWN")  # Si encore inconnu, on met "UNKNOWN"
)


# ✅ Vérification après correction des store_id
print("\n✅ Vérification après correction FORCÉE des store_id :")
df.select("sale_id", "store_id_from_sale", "store_id_corrected").show(10, truncate=False)




# 🎯 Correction du `sale_id` avec `YYMM`
df = df.withColumn("sale_id",
    when(col("store_id_corrected") == "UNKNOWN", col("sale_id"))  # Si inconnu, ne change rien
    .otherwise(concat_ws("", col("store_id_corrected"), col("year_month_yy"), lpad(substring(col("sale_id"), -5, 5), 5, "0")))
)


# ✅ Vérification après correction du sale_id
print("\n✅ Vérification après correction finale du sale_id :")
df.select("sale_id", "store_id_from_sale", "store_id_corrected", "year_month_yy").show(10, truncate=False)



# Supprimer les colonnes temporaires
df = df.drop("store_id_from_sale", "year_month_from_sale", "year_month_from_date")

# Vérifier après suppression des colonnes temporaires
print("\n✅ Vérification après suppression des colonnes temporaires :")
df.show(5, truncate=False)

# Vérifier le nombre total de lignes après transformation
print(f"\n📏 Nombre total de lignes après transformation : {df.count()}")


# Vérifier les sale_id avec une longueur différente de 13
df.filter(length(col("sale_id")) != 13).select("sale_id").show(20, truncate=False)

# Compter combien de sale_id ont une longueur incorrecte
count_incorrect = df.filter(length(col("sale_id")) != 13).count()
print(f"\n📏 Nombre total de sale_id incorrects (≠ 13 caractères) : {count_incorrect}")

#############################################################################################
######################### Modification price ################################################
#############################################################################################

# 🏷️ Trouver un prix existant pour chaque `product_name`
df_price_lookup = df.groupBy("product_name").agg(first("price", ignorenulls=True).alias("ref_price"))

# 🏷️ Joindre ce prix de référence sur le DataFrame principal
df = df.join(df_price_lookup, on="product_name", how="left")

# 🏷️ Remplacer `price` si NULL, 0 ou non numérique
df = df.withColumn("price",
    round(
        when(col("price").isNull(), when(col("ref_price").isNull(), 1.00).otherwise(col("ref_price")))  # NULL → Prix homonyme ou 1.00
        .when(trim(col("price")) == "", when(col("ref_price").isNull(), 1.00).otherwise(col("ref_price")))  # Vide → Prix homonyme ou 1.00
        .when(col("price") == "X", when(col("ref_price").isNull(), 1.00).otherwise(col("ref_price")))  # "X" → Prix homonyme ou 1.00
        .when(col("price").cast("double").isNull(), when(col("ref_price").isNull(), 1.00).otherwise(col("ref_price")))  # Non numérique → Prix homonyme ou 1.00
        .otherwise(col("price").cast("double")),  # Sinon, garder le prix actuel
        2  # Arrondi à 2 décimales
    )
)

# 🧹 Supprimer la colonne temporaire `ref_price`
df = df.drop("ref_price")

# ✅ Vérification après transformation
print("\n✅ Vérification de la colonne price :")
df.select("product_name", "price").show(10, truncate=False)


# 🏷️ Nettoyer et convertir price
df = df.withColumn("price",
    round(
        when(col("price").isNull(), 0.00)  # Remplace NULL par 0.00
        .when(trim(col("price")) == "", 0.00)  # Remplace les valeurs vides " " par 0.00
        .when(col("price") == "X", 0.00)  # Remplace "X" par 0.00
        .otherwise(col("price").cast("double")),  # Convertit les autres valeurs en nombre
        2  # Arrondi à 2 décimales
    )
)

# ✅ Vérification après transformation
print("\n✅ Vérification de la colonne price :")
df.select("price").show(10, truncate=False)

# 🏷️ Convertir `price` en type float (double en Spark)
df = df.withColumn("price", col("price").cast("double"))

# ✅ Vérification après transformation
print("\n✅ Vérification finale de la colonne price :")
df.select("product_name", "price").show(10, truncate=False)

# 🏷️ Vérification du schéma
df.printSchema()

#############################################################################################
######################### Modification type colonne #########################################
#############################################################################################

# 🏷️ Convertir les types des colonnes
df = df.withColumn("price", col("price").cast("double"))  # Garde price en float
df = df.withColumn("quantity", col("quantity").cast("int"))  # Convertir en int (moins de mémoire)
df = df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))  # Date propre
df = df.withColumn("year_month_yy", col("year_month_yy").cast("string"))  # Garde en string
df = df.withColumn("store_id_corrected", col("store_id_corrected").cast("string"))  # Garde en string
df = df.withColumn("sale_id", col("sale_id").cast("string"))  # Garde en string
df = df.withColumn("email", col("email").cast("string"))  # Garde en string
df = df.withColumn("product_name", col("product_name").cast("string"))  # Garde en string

# ✅ Vérification après transformation
print("\n✅ Vérification finale du schéma des colonnes :")
df.printSchema()


# 🏷️ Remplacer l'ancienne colonne transaction_date par la version propre
df = df.drop("transaction_date").withColumnRenamed("transaction_date_clean", "transaction_date")

# ✅ Vérification après remplacement
print("\n✅ Vérification finale de transaction_date :")
df.select("transaction_date").show(10, truncate=False)

# 🏷️ Vérifier le schéma final
df.printSchema()


#############################################################################################
######################### Export CSV propre #################################################
#############################################################################################

df.write.mode("overwrite").csv("data/cegid/cleaned_cegid_sales.csv", header=True)


