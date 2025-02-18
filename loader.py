# loader.py
import logging
import mysql.connector

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataLoader:
    """
    Classe pour charger les DataFrames dans MySQL via JDBC, en respectant le schéma en étoile.
    Elle désactive temporairement les vérifications de clés étrangères pour permettre
    l'utilisation du mode "overwrite" sur les tables référencées par des clés étrangères.
    """

    def __init__(self, jdbc_url, user, password, database, driver="com.mysql.cj.jdbc.Driver"):
        self.jdbc_url = jdbc_url
        self.user = user
        self.password = password
        self.database = database
        self.driver = driver

    def load_dim_date(self, df_dim_date, mode="append"):
        self._load(df_dim_date, "Dim_Date", mode)

    def load_dim_client(self, df_dim_client, mode="append"):
        self._load(df_dim_client, "Dim_Client", mode)

    def load_dim_product(self, df_dim_product, mode="append"):
        self._load(df_dim_product, "Dim_Product", mode)

    def load_dim_channel(self, df_dim_channel, mode="append"):
        self._load(df_dim_channel, "Dim_Channel", mode)

    def load_dim_store(self, df_dim_store, mode="append"):
        self._load(df_dim_store, "Dim_Store", mode)

    def load_fact_sales(self, df_fact_sales, mode="append"):
        self._load(df_fact_sales, "Fact_Sales", mode)

    def _load(self, df, table_name, mode):
        """
        Méthode interne qui réalise l'écriture JDBC dans MySQL.
        Avant l'écriture, on désactive les vérifications de clés étrangères pour éviter
        les problèmes liés aux contraintes FK (ex: DROP TABLE sur une table référencée).
        """
        props = {
            "user": self.user,
            "password": self.password,
            "driver": self.driver
        }
        # Désactivation des vérifications FK via une connexion MySQL
        try:
            conn = mysql.connector.connect(
                host=self._get_host_from_jdbc(),
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            self._extracted_from__load_21(
                cursor,
                "SET FOREIGN_KEY_CHECKS=0;",
                conn,
                "Foreign key checks disabled.",
            )
        except Exception as e:
            logger.error(f"Erreur lors de la désactivation des contraintes FK: {str(e)}")
            conn = None

        try:
            df.write.jdbc(url=self.jdbc_url, table=table_name, mode=mode, properties=props)
            logger.info(f"Données chargées dans la table {table_name} avec succès.")
        except Exception as e:
            logger.error(f"Erreur lors du chargement dans la table {table_name} : {str(e)}")
        finally:
            # Réactivation des vérifications FK
            if conn is not None:
                try:
                    self._extracted_from__load_21(
                        cursor,
                        "SET FOREIGN_KEY_CHECKS=1;",
                        conn,
                        "Foreign key checks enabled.",
                    )
                    cursor.close()
                    conn.close()
                except Exception as ex:
                    logger.error(f"Erreur lors de la réactivation des contraintes FK: {str(ex)}")

    # TODO Rename this here and in `_load`
    def _extracted_from__load_21(self, cursor, arg1, conn, arg3):
        cursor.execute(arg1)
        conn.commit()
        logger.info(arg3)

    def _get_host_from_jdbc(self):
        """
        Extrait l'hôte de l'URL JDBC. Par exemple, pour "jdbc:mysql://localhost:3306/finegourmet",
        renvoie "localhost".
        """
        try:
            # Supposons le format jdbc:mysql://host:port/database
            url_without_prefix = self.jdbc_url.split("://")[1]
            host_port = url_without_prefix.split("/")[0]
            return host_port.split(":")[0]
        except Exception:
            return "localhost"
