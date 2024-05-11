# Projet de Base de Données Géographiques

Ce projet contient du code Python pour créer une base de données géographiques à partir de fichiers CSV contenant des données sur les régions, les départements et les communes.

## Utilisation

1. Exécutez le script Python `Create&import.py` pour créer la base de données et importer les données à partir des fichiers CSV.
2. Assurez-vous d'avoir une base de données PostgreSQL en cours d'exécution avec les informations d'identification appropriées définies dans le script.
3. Consultez le fichier README pour plus d'instructions sur l'exécution du code.

## Structure des Fichiers CSV

- `regions.csv` : Contient les données sur les régions, y compris le code de région, le chef-lieu et le nom de région.
- `departements.csv` : Contient les données sur les départements, y compris le code de département, le code de région, le chef-lieu et le nom de département.
- `communes.csv` : Contient les données sur les communes, y compris le code de commune, le code de région, le code de département, le nom de commune et la population et annee.

## Base de Données

La base de données contient des tables pour les régions, les départements et les communes, ainsi que des vues pour afficher la population des départements et des régions pour différentes années.

