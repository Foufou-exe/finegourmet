-- Suppression des tables existantes (dans l'ordre des dépendances)
DROP TABLE IF EXISTS Fact_Ventes;

DROP TABLE IF EXISTS Dim_Boutique;

DROP TABLE IF EXISTS Dim_Canal;

DROP TABLE IF EXISTS Dim_Produit;

DROP TABLE IF EXISTS Dim_Client;

DROP TABLE IF EXISTS Dim_Date;

-- Création de la dimension Date
CREATE TABLE
    Dim_Date (
        Date_ID INT PRIMARY KEY, -- Identifiant unique pour la date
        Date DATE NOT NULL, -- Date complète (ex: '2024-01-05')
        Jour INT, -- Jour du mois
        Mois INT, -- Mois
        Annee INT, -- Année
    );

-- Création de la dimension Client
CREATE TABLE
    Dim_Client (
        Client_ID INT PRIMARY KEY, -- Identifiant unique du client
        Email VARCHAR(255), -- Adresse email (peut être NULL pour les ventes physiques)
        Nom VARCHAR(255), -- Nom du client (optionnel)
        Prenom VARCHAR(255) -- Prénom du client (optionnel)
    );

-- Création de la dimension Produit
CREATE TABLE
    Dim_Produit (
        Product_ID INT PRIMARY KEY, -- Identifiant unique du produit
        Nom VARCHAR(255) NOT NULL, -- Nom du produit
        Categorie VARCHAR(255), -- Catégorie ou type de produit
    );

-- Création de la dimension Canal
CREATE TABLE
    Dim_Canal (
        Canal_ID INT PRIMARY KEY, -- Identifiant unique du canal de vente
        Type VARCHAR(50) NOT NULL -- Type de canal (ex : 'Online' ou 'Physique')
    );

-- Création de la dimension Boutique
CREATE TABLE
    Dim_Boutique (
        Boutique_ID INT PRIMARY KEY, -- Identifiant unique de la boutique
        Nom VARCHAR(255) NOT NULL, -- Nom de la boutique
        Localisation VARCHAR(255) -- Localisation ou adresse de la boutique
    );

-- Création de la table de faits Ventes
CREATE TABLE
    Fact_Ventes (
        Sale_ID VARCHAR(50) PRIMARY KEY, -- Identifiant unique de la vente (ex: numéro de ticket)
        Quantity INT NOT NULL, -- Quantité vendue
        Price DECIMAL(10, 2) NOT NULL, -- Prix unitaire ou montant total de la vente
        FK_Date_ID INT NOT NULL, -- Référence vers Dim_Date
        FK_Client_ID INT, -- Référence vers Dim_Client (peut être NULL pour une vente physique sans email)
        FK_Product_ID INT NOT NULL, -- Référence vers Dim_Produit
        FK_Canal_ID INT NOT NULL, -- Référence vers Dim_Canal
        FK_Boutique_ID INT, -- Référence vers Dim_Boutique (pour les ventes physiques)
        FOREIGN KEY (FK_Date_ID) REFERENCES Dim_Date (Date_ID),
        FOREIGN KEY (FK_Client_ID) REFERENCES Dim_Client (Client_ID),
        FOREIGN KEY (FK_Product_ID) REFERENCES Dim_Produit (Product_ID),
        FOREIGN KEY (FK_Canal_ID) REFERENCES Dim_Canal (Canal_ID),
        FOREIGN KEY (FK_Boutique_ID) REFERENCES Dim_Boutique (Boutique_ID)
    );

-- Insertion de valeurs initiales dans Dim_Canal
INSERT INTO
    Dim_Canal (Canal_ID, Type)
VALUES
    (1, 'Online'),
    (2, 'Physique');

-- Fin du script