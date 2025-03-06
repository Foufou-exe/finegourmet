<div align="center">

![logo finegourmet](./docs/images/logo_finegourmet.png)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub last commit](https://img.shields.io/github/last-commit/Foufou-exe/finegourmet)]()
[![GitHub pull requests](https://img.shields.io/github/issues-pr/Foufou-exe/finegourmet)]()
[![GitHub forks](https://img.shields.io/github/forks/Foufou-exe/finegourmet)]()
[![GitHub stars](https://img.shields.io/github/stars/Foufou-exe/finegourmet)]()
[![GitHub watchers](https://img.shields.io/github/watchers/Foufou-exe/finegourmet)]()

</div>

<div align="center">

[FR](./README.fr.md) | [EN](./README.md)

</div>

# FineGourmet

FineGourmet est un projet d'ETL (Extract, Transform, Load) conçu pour traiter et analyser des données de vente provenant de diverses sources, notamment Salesforce Commerce Cloud (SFCC) et CEGID. En utilisant Apache Spark pour le traitement des données et MySQL pour le stockage, FineGourmet permet une gestion efficace des données tout en garantissant leur qualité.

## Contexte et Présentation de l'Entreprise

FineGourmet est une entreprise fictive spécialisée dans la vente de produits d'épicerie fine. Elle opère à travers deux canaux de vente :

- **En ligne** : via un site web e-commerce géré avec Salesforce Commerce Cloud (SFCC).
- **Boutiques physiques** : un réseau de 13 boutiques réparties dans différentes villes de France, utilisant le système de caisse CEGID.

### Problématique

FineGourmet souhaite consolider et analyser les données de vente provenant de ces deux sources distinctes pour optimiser ses performances commerciales.

### Données Fournies

- Fichiers de ventes SFCC : `202401_sfcc_sales.csv`, `202402_sfcc_sales.csv`, ..., `202412_sfcc_sales.csv`
- Fichier de ventes CEGID : `2024_cegid_sales.json`
- Fichier de référence des produits : `2025_product_reference.csv`
- Fichier des boutiques : `2025_boutiques.csv`

## Objectifs Généraux de l'Étude de Cas

Les étudiants doivent :

1. Comprendre et explorer les données.
2. Identifier les indicateurs clés (KPI) pour répondre aux questions de FineGourmet :
   - Quelles sont les tendances globales des ventes ?
   - Quels sont les produits les plus performants ?
   - Qui sont nos clients les plus fidèles ?
3. Réaliser des analyses descriptives et de cohérence des données.
4. Créer des visualisations pertinentes et produire des tableaux de bord clairs.
5. Formuler des recommandations basées sur les résultats de l'analyse (optionnel).

## Table des matières

- [FineGourmet](#finegourmet)
  - [Contexte et Présentation de l'Entreprise](#contexte-et-présentation-de-lentreprise)
    - [Problématique](#problématique)
    - [Données Fournies](#données-fournies)
  - [Objectifs Généraux de l'Étude de Cas](#objectifs-généraux-de-létude-de-cas)
  - [Table des matières](#table-des-matières)
  - [Fonctionnalités](#fonctionnalités)
  - [Technologies utilisées](#technologies-utilisées)
  - [Installation](#installation)
  - [Utilisation](#utilisation)
  - [Contribuer](#contribuer)
  - [Auteurs](#auteurs)
  - [Licence](#licence)

## Fonctionnalités

- **Extraction des données** : Charge les données de vente à partir de fichiers CSV et JSON.
- **Transformation des données** : Applique des transformations pour adapter les données à un schéma en étoile, incluant la normalisation et la gestion des doublons.
- **Chargement des données** : Charge les données transformées dans une base de données MySQL.
- **Gestion des données manquantes** : Identifie et traite les données manquantes et les doublons.
- **Journalisation** : Fournit des logs détaillés pour le suivi des opérations ETL.

## Technologies utilisées

- **Python** : Langage de programmation principal.
- **Apache Spark** : Framework pour le traitement des données massives.
- **MySQL** : Système de gestion de base de données pour le stockage des données.
- **Pandas** : Bibliothèque pour la manipulation des données (si nécessaire).
- **Logging** : Pour la journalisation des opérations.

## Installation

1. **Clonez le dépôt** :

   ```bash
   git clone https://github.com/votre-utilisateur/finegourmet.git
   cd finegourmet
   ```

2. **Installez les dépendances** :

   ```bash
   pip install -r requirements.txt
   ```

3. **Configurez votre base de données MySQL** :

   - Assurez-vous que MySQL est installé et en cours d'exécution.
   - Mettez à jour les informations de connexion dans le fichier `main.py`.

4. **Initialisez la base de données MySQL avec Docker** :
   ```docker
   docker compose up -d
   ```

5. **Vérifiez que la base de données est accessible** :

   Voir via PhpMyAdmin ou votre outil de gestion de base de données préféré. 
   (DEFAULT : http://localhost:8080)

## Utilisation

1. **Exécutez le script principal** :

   ```bash
   python main.py
   ```

2. **Vérifiez les logs** :
   - Consultez les logs générés pour suivre le processus d'extraction, de transformation et de chargement.

## Contribuer

Les contributions sont les bienvenues ! Si vous souhaitez améliorer le projet, veuillez soumettre une demande de tirage (pull request) avec vos modifications.

## Auteurs

- [@Foufou-exe](https://github.com/Foufou-exe)
- [@ml704457](https://github.com/ml704457)
- [@Keods30](https://github.com/Keods30)
- [@JonathanDS30](https://github.com/JonathanDS30)

## Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.
