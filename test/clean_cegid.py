from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, expr, concat_ws, col, when, lpad, length, round, trim, first, to_date, isnan,lower, regexp_replace, row_number
from pyspark.sql import Window

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

# Vérifier les doublons
print("\n🔍 Vérification des doublons AVANT transformation :")
df.groupBy("sale_id").count().filter(col("count") > 1).show(20, truncate=False)


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
#df = df.withColumn("email",
#    when(col("email").isNull(), "non_renseignée")  # NULL → "non_renseigné"
#    .when(trim(col("email")) == "", "non_renseignée")  # " " (vide) → "non_renseigné"
#    .otherwise(col("email"))  # Sinon, on garde l'email existant
#)
# 🏷️ Nettoyer la colonne email
df = df.withColumn("email",
    trim(lower(col("email")))  # Convertir en minuscule et supprimer les espaces autour
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

df = df.withColumn("store_id",
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
df.select("sale_id", "store_id_from_sale", "store_id").show(10, truncate=False)

print("\n🔍 Vérification des lignes en double sur sale_id AVANT transformation :")
df.filter(col("sale_id").isin(["MO01240100002", "BO02240800001", "MO01240800001"])).show(truncate=False)


print("\n🔍 Vérification des valeurs qui vont être modifiées :")
df.filter((~col("store_id_from_sale").isin(store_ids)) |
          (col("year_month_from_sale") != col("year_month_yy"))
         ).select("sale_id", "store_id_from_sale", "year_month_from_sale", "year_month_yy").show(20, truncate=False)


# 🏷️ Ne modifier `sale_id` QUE si le `store_id` ou `year_month` est incorrect
df = df.withColumn("sale_id",
    when(
        (~col("store_id_from_sale").isin(store_ids)) |  # Si `store_id` invalide
        ((col("year_month_from_sale") != col("year_month_yy")) & col("year_month_from_sale").isNotNull()),  # Vérification stricte sur l'année/mois
        concat_ws("", col("store_id"), col("year_month_yy"), substring(col("sale_id"), 9, 5))
    ).otherwise(col("sale_id"))
)

# ✅ Vérification après correction finale du sale_id
print("\n✅ Vérification après correction finale du sale_id :")
df.select("sale_id", "store_id_from_sale", "store_id", "year_month_yy").show(10, truncate=False)

print("\n✅ Vérification des doublons APRÈS transformation :")
df.groupBy("sale_id").count().filter(col("count") > 1).show(20, truncate=False)

#############################################################################################
######################### Correction des doublons de sales_id ################################
#############################################################################################

# Créer une fenêtre pour numéroter les doublons
window_spec = Window.partitionBy("sale_id").orderBy("transaction_date")
df = df.withColumn("row_num", row_number().over(window_spec))

# Fonction pour incrémenter le dernier numéro sans créer de nouveaux doublons
def increment_last_number(df):
    # Identifier les sales_id existants pour éviter de créer de nouveaux doublons
    existing_sales_ids = set(row.sale_id for row in df.select("sale_id").distinct().collect())

    processed_df = df
    for sale_id in df.filter(col("row_num") > 1).select("sale_id").distinct().collect():
        base_id = sale_id.sale_id[:-2]  # Prendre tout sauf les 2 derniers chiffres

        # Trouver le prochain numéro disponible
        i = 1
        while base_id + str(i).zfill(2) in existing_sales_ids:
            i += 1

        # Mettre à jour le sale_id
        processed_df = processed_df.withColumn(
            "sale_id",
            when(
                (col("sale_id") == sale_id.sale_id) & (col("row_num") > 1),
                expr(f"'{base_id}' || lpad('{i}', 2, '0')")
            ).otherwise(col("sale_id"))
        )

        # Ajouter le nouveau sale_id à l'ensemble des existants
        existing_sales_ids.add(base_id + str(i).zfill(2))

    return processed_df

# Appliquer l'incrémentation
df = increment_last_number(df)

# Supprimer la colonne temporaire
df = df.drop("row_num")

# Vérifier qu'il n'y a plus de doublons
print("\n✅ Vérification finale des doublons après correction :")
df.groupBy("sale_id").count().filter(col("count") > 1).show()

#############################################################################################
######################### Modification type colonne #########################################
#############################################################################################


# 🏷️ Supprimer les colonnes temporaires
df = df.drop("store_id_from_sale", "year_month_from_sale", "year_month_from_date", "store_id")
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

# 🏷️ Calculer `price_unitaire` correct pour chaque `product_name` en excluant les valeurs invalides
df_price_lookup = df.filter(
    (col("price").isNotNull()) & (~isnan(col("price"))) & (trim(col("price")) != "") & (col("price") != "X") & (col("price").cast("double").isNotNull())
).withColumn("price_unitaire", (col("price") / col("quantity")).cast("double"))

# 🏷️ Agréger le premier `price_unitaire` valide pour chaque `product_name`
df_price_lookup = df_price_lookup.groupBy("product_name").agg(first("price_unitaire", ignorenulls=True).alias("ref_price_unitaire"))

# 🏷️ Joindre ce `price_unitaire` de référence sur le DataFrame principal
df = df.join(df_price_lookup, on="product_name", how="left")

# 🏷️ Remplacer les valeurs invalides de `price` par `ref_price_unitaire * quantity`
df = df.withColumn("price",
    when(
        (col("price").isNull()) | (trim(col("price")) == "") | (col("price") == "X") | (col("price").cast("double").isNull()),
        round(col("ref_price_unitaire") * col("quantity"), 2)  # Remplacement par `price_unitaire * quantity`
    )
    .otherwise(col("price").cast("double"))  # Sinon, garder la valeur existante
)

# 🏷️ Supprimer la colonne temporaire `ref_price_unitaire`
df = df.drop("ref_price_unitaire")

# ✅ Vérification après transformation
print("\n✅ Vérification de la colonne price après correction des valeurs invalides :")
df.select("product_name", "quantity", "price").show(10, truncate=False)


#############################################################################################
######################### Modification type colonne #########################################
#############################################################################################

# 🏷️ Convertir les types des colonnes
df = df.withColumn("price", col("price").cast("double"))  # Garde price en float
df = df.withColumn("quantity", col("quantity").cast("int"))  # Convertir en int (moins de mémoire)
df = df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))  # Date propre
df = df.withColumn("year_month_yy", col("year_month_yy").cast("string"))  # Garde en string
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

# 🏷️ Supprimer la colonne year_month_yy à la fin
df = df.drop("year_month_yy")

# ✅ Vérification après suppression
print("\n✅ Vérification du schéma après suppression de year_month_yy :")
df.printSchema()



#############################################################################################
######################### Export CSV propre #################################################
#############################################################################################

df.write.mode("overwrite").csv("data/cegid/cleaned_cegid_sales.csv", header=True)


