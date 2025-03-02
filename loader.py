# loader.py
import logging
import mysql.connector
from pyspark.sql.functions import when, col, lit

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataLoader:
    """
    Classe pour charger les DataFrames dans MySQL via JDBC, en respectant le schéma en étoile.
    Les données sont ajoutées en mode append (ajout sans écrasement) et
    les vérifications des clés étrangères sont temporairement désactivées.

    Pour la table Fact_Sales, la colonne "Type" est ajoutée :
      - "Online" si FK_Store_ID est null
      - "Store" sinon.
    """
    def __init__(self, jdbc_url, user, password, database, driver="com.mysql.cj.jdbc.Driver"):
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password
        self.database = database
        self.driver = driver


    def load_dim_client(self, df_dim_client, mode="append"):
        self._load(df_dim_client, "Dim_Client", mode)

    def load_dim_product(self, df_dim_product, mode="append"):
        self._load(df_dim_product, "Dim_Product", mode)

    def load_dim_store(self, df_dim_store, mode="append"):
        self._load(df_dim_store, "Dim_Store", mode)

    def load_fact_sales(self, df_fact_sales, mode="append"):
        self._load(df_fact_sales, "Fact_Sales", mode)

    def _load(self, df, table_name, mode):
        """
        Réalise l'écriture JDBC dans MySQL.
        Pour Fact_Sales, on ajoute la colonne "Type" en fonction de FK_Store_ID.
        """
        if table_name == "Fact_Sales":
            # On s'assure de ne pas avoir de colonnes superflues (par ex. FK_Channel_ID)
            if "FK_Channel_ID" in df.columns:
                df = df.drop("FK_Channel_ID")
            # Ajout de la colonne Type
            df = df.withColumn("Type", when(col("FK_Store_ID").isNull(), lit("Online")).otherwise(lit("Store")))

        props = {
            "user": self.user,
            "password": self.password,
            "driver": self.driver
        }
        # Désactivation temporaire des contraintes FK
        try:
            conn = mysql.connector.connect(
                host=self._get_host_from_jdbc(),
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=0;", conn, "Foreign key checks disabled.")
        except Exception as e:
            logger.error(f"Erreur lors de la désactivation des contraintes FK: {str(e)}")
            conn = None

        try:
            df.write.jdbc(url=self.jdbc_url, table=table_name, mode=mode, properties=props)
            logger.info(f"Données chargées dans la table {table_name} avec succès.")
        except Exception as e:
            logger.error(f"Erreur lors du chargement dans la table {table_name} : {str(e)}")
        finally:
            if conn is not None:
                try:
                    self._execute_sql(cursor, "SET FOREIGN_KEY_CHECKS=1;", conn, "Foreign key checks enabled.")
                    cursor.close()
                    conn.close()
                except Exception as ex:
                    logger.error(f"Erreur lors de la réactivation des contraintes FK: {str(ex)}")

    def _execute_sql(self, cursor, sql_statement, conn, success_message):
        cursor.execute(sql_statement)
        conn.commit()
        logger.info(success_message)

    def _get_host_from_jdbc(self):
        try:
            url_without_prefix = self.jdbc_url.split("://")[1]
            host_port = url_without_prefix.split("/")[0]
            return host_port.split(":")[0]
        except Exception:
            return "localhost"
