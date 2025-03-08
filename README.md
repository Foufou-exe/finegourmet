<div align="center">

![finegourmet logo](./docs/images/logo_finegourmet.png)

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

FineGourmet is an ETL (Extract, Transform, Load) project designed to process and analyse sales data from a variety of sources, including Salesforce Commerce Cloud (SFCC) and CEGID. Using Apache Spark for data processing and MySQL for storage, FineGourmet enables efficient data management while guaranteeing data quality.

## Background and Company Presentation

FineGourmet is a fictitious company specialising in the sale of delicatessen products. It operates through two sales channels:

- **Online**: via an e-commerce website managed with Salesforce Commerce Cloud (SFCC).
- **Physical stores**: a network of 13 shops in various French towns, using the CEGID POS system.

### Challenge

FineGourmet wanted to consolidate and analyse sales data from these two separate sources in order to optimise its sales performance.

### Data supplied

- SFCC sales files: `202401_sfcc_sales.csv`, `202402_sfcc_sales.csv`, ..., `202412_sfcc_sales.csv`.
- CEGID sales file: `2024_cegid_sales.json`, ...
- Product reference file: `2025_product_reference.csv`, ...
- Shop file: `2025_shop.csv`

## General objectives of the case study

Students will:

1. Understand and explore the data.
2. Identify key performance indicators (KPIs) to answer FineGourmet's questions:
   - What are the global sales trends?
   - Which products are performing best?
   - Who are our most loyal customers?
3. Carry out descriptive and consistency analyses of data.
4. Create relevant visualisations and produce clear dashboards.
5. Formulate recommendations based on the results of the analysis (optional).

## Table of contents

- FineGourmet](#finegourmet)
  - Context and presentation of the company](#contexte-et-présentation-de-l'entreprise)
    - Problematic](#problematic)
    - Data provided](#data-provided)
  - General objectives of the case study](#objectifs-généraux-de-létude-de-cas)
  - Table of contents](#table-des-matières)
  - Features](#features)
  - [Technologies used](#technologies-utilisées)
  - [Installation](#installation)
  - [Use](#use)
  - [Contribute](#contribute)
  - [Authors](#authors)
  - [Licence](#licence)

## Features

- **Data Extraction**: Loads sales data from CSV and JSON files.
- **Data transformation**: Applies transformations to adapt data to a star schema, including normalisation and duplicate management.
- **Load data**: Loads transformed data into a MySQL database.
- **Missing data management**: Identifies and processes missing data and duplicates.
- **Logging**: Provides detailed logs for monitoring ETL operations.

## Technologies used

- **Python**: Main programming language.
- **Apache Spark**: Framework for processing massive data.
- **MySQL**: Database management system for data storage.
- **Pandas**: Library for data manipulation (if required).
- **Logging**: For logging operations.

## Installation

1. **Clone the repository** :

   ```bash
   git clone https://github.com/votre-utilisateur/finegourmet.git
   cd finegourmet
   ```

2. **Install the dependencies** :

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure your MySQL database** :

   - Make sure MySQL is installed and running.
   - Update the connection information in the `main.py` file.

4. **Initialise the MySQL database with Docker** :
   ```docker
   docker compose up -d
   ```

5. **Check that the database is accessible** :

   See via PhpMyAdmin or your preferred database management tool.
   (DEFAULT: http://localhost:8080)

## Use

1. **Run the main script** :

   ```bash
   python main.py
   ```

2. **Check the logs**:
   - Check the generated logs to follow the extraction, transformation and loading process.

## Contribute

Contributions are welcome! If you would like to improve the project, please submit a pull request with your changes.

## Authors

- @Foufou-exe](https://github.com/Foufou-exe)
- [@ml704457](https://github.com/ml704457)
- [@Keods30](https://github.com/Keods30)
- [@JonathanDS30](https://github.com/JonathanDS30)

## License

This project is licensed under the MIT license. See the [LICENSE](LICENSE) file for more details.
