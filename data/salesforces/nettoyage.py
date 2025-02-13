import os
import shutil
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace

# Obtenir le dossier où se trouve `nettoyage.py`
script_dir = os.path.dirname(os.path.abspath(__file__))

# Définir les chemins relatifs au script
input_folder = os.path.join(script_dir)  # Tous les fichiers CSV sont dans le même dossier
output_file = os.path.join(script_dir, "sfcc_cleaned.csv")  # Fichier nettoyé dans le même dossier

def clean_sfcc_data(input_folder, output_file):
    """
    Nettoie et fusionne tous les fichiers CSV SFCC d'un dossier en un seul fichier propre en utilisant Spark.
    - Supprime les tabulations et espaces en trop
    - Conserve les accents (UTF-8 avec BOM)
    - Assure une fin de ligne CRLF (\r\n) sans double saut de ligne
    """

    # Vérifier si le dossier d'entrée existe
    if not os.path.exists(input_folder):
        print(f"❌ Erreur : Le dossier '{input_folder}' n'existe pas. Vérifiez le chemin.")
        return
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("SFCC Data Cleaning")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()
    
    all_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".csv")]
    df_list = []
    
    for file in all_files:
        try:
            df = spark.read.option("header", "true")\
                           .option("encoding", "UTF-8")\
                           .option("multiline", "true")\
                           .option("escape", "\"")\
                           .csv(file)
            df_list.append(df)
        except Exception as e:
            print(f"⚠️ Erreur lors de la lecture du fichier {file}: {e}")
    
    if df_list:
        merged_df = df_list[0]
        for df in df_list[1:]:
            merged_df = merged_df.union(df)
        
        # Nettoyer les colonnes : enlever les espaces et renommer en minuscules
        merged_df = merged_df.select([col(c).alias(c.strip().lower().replace(" ", "_")) for c in merged_df.columns])
        
        # Supprimer les doublons
        merged_df = merged_df.dropDuplicates()
        
        # Nettoyer les valeurs de colonnes (suppression tabulations et espaces en trop)
        for column in merged_df.columns:
            merged_df = merged_df.withColumn(column, trim(regexp_replace(col(column), r"[\t\r\n]+", " ")))

        # Convertir les types
        if 'transaction_date' in merged_df.columns:
            merged_df = merged_df.withColumn("transaction_date", col("transaction_date").cast("date"))
        if 'price' in merged_df.columns:
            merged_df = merged_df.withColumn("price", col("price").cast("double"))
        if 'quantity' in merged_df.columns:
            merged_df = merged_df.withColumn("quantity", col("quantity").cast("int"))


        # Sauvegarde des données nettoyées dans un fichier temporaire Spark
        temp_output_folder = os.path.join(input_folder, "temp_output")
        merged_df.coalesce(1).write.option("header", "true")\
                                   .option("encoding", "UTF-8")\
                                   .mode("overwrite")\
                                   .csv(temp_output_folder)

        print(f"✅ Données nettoyées enregistrées temporairement sous : {temp_output_folder}")

        # Trouver le fichier CSV généré par Spark et le renommer en `sfcc_cleaned.csv`
        convert_to_utf8_bom_crlf(temp_output_folder, output_file)

        # Supprimer le dossier temporaire après conversion
        shutil.rmtree(temp_output_folder)
    else:
        print("❌ Aucun fichier CSV valide trouvé dans le dossier.")

def convert_to_utf8_bom_crlf(temp_output_folder, output_file):
    """
    Convertit le fichier CSV généré par Spark en UTF-8 avec BOM et CRLF sans double saut de ligne.
    """
    # Trouver le fichier CSV généré dans le dossier temporaire
    csv_file = None
    for file in os.listdir(temp_output_folder):
        if file.endswith(".csv"):
            csv_file = os.path.join(temp_output_folder, file)
            break

    if not csv_file:
        print("❌ Erreur : Aucun fichier CSV généré par Spark.")
        return

    # Lire et réécrire le fichier avec UTF-8 BOM et CRLF
    with open(csv_file, "r", encoding="utf-8") as infile, \
         open(output_file, "w", encoding="utf-8-sig", newline="") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_ALL, lineterminator="\r\n")  # CRLF
        
        for row in reader:
            writer.writerow(row)

    print(f"✅ Fichier final enregistré sous : {output_file}")

# Exécuter le nettoyage avec les chemins relatifs au fichier `nettoyage.py`
clean_sfcc_data(input_folder, output_file)
