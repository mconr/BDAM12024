import psycopg2
import pandas as pd
from psycopg2 import sql
from io import StringIO

# Configuration de la connexion
DB_CONFIG = {
    'host': 'localhost',
    'database': 'inseedb',
    'user': 'postgres',
    'password': 'admin'
}

def create_tables(conn):
    """Crée les tables selon le schéma relationnel"""
    with conn.cursor() as cur:
        # Table REGION
        cur.execute("""
        CREATE TABLE IF NOT EXISTS region (
            reg_id VARCHAR(2) PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        );
        """)
        
        # Table DEPARTEMENT
        cur.execute("""
        CREATE TABLE IF NOT EXISTS departement (
            dep_id VARCHAR(3) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            reg_id VARCHAR(2) NOT NULL REFERENCES region(reg_id)
        );
        """)
        
        # Table COMMUNE
        cur.execute("""
        CREATE TABLE IF NOT EXISTS commune (
            com_id SERIAL PRIMARY KEY,
            code_insee VARCHAR(5) NOT NULL UNIQUE,
            name VARCHAR(100) NOT NULL,
            dep_id VARCHAR(3) NOT NULL REFERENCES departement(dep_id)
        );
        """)
        
        # Table CHEFLIEUREGION
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chef_lieu_region (
            reg_id VARCHAR(2) PRIMARY KEY REFERENCES region(reg_id),
            com_id INTEGER NOT NULL REFERENCES commune(com_id)
        );
        """)
        
        # Table CHEFLIEUDEPARTEMENT
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chef_lieu_departement (
            dep_id VARCHAR(3) PRIMARY KEY REFERENCES departement(dep_id),
            com_id INTEGER NOT NULL REFERENCES commune(com_id)
        );
        """)
        
        # Table TYPESTATISTIQUE
        cur.execute("""
        CREATE TABLE IF NOT EXISTS type_statistique (
            id SERIAL PRIMARY KEY,
            nom VARCHAR(50) NOT NULL UNIQUE,
            description TEXT
        );
        """)
        
        # Table STATISTIQUE
        cur.execute("""
        CREATE TABLE IF NOT EXISTS statistique (
            id SERIAL PRIMARY KEY,
            com_id INTEGER NOT NULL REFERENCES commune(com_id),
            type_id INTEGER NOT NULL REFERENCES type_statistique(id),
            annee INTEGER ,
            annee_fin INTEGER,
            valeur NUMERIC,
            CONSTRAINT valid_years CHECK (annee_fin IS NULL OR annee_fin >= annee),
            UNIQUE (com_id, type_id, annee)
        );
        """)
        
        # Création des index
        cur.execute("CREATE INDEX IF NOT EXISTS idx_statistique_com_id ON statistique(com_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_statistique_type_id ON statistique(type_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_statistique_annee ON statistique(annee);")
        
        conn.commit()

def import_regions(conn):
    """Importe les données des régions depuis v_region_2024.csv"""
    try:
        df = pd.read_csv('v_region_2024.csv')
        
        # Vérification des colonnes disponibles
        print("Colonnes dans v_region_2024.csv:", df.columns.tolist())
        
        # Les colonnes attendues sont: REG, CHEFLIEU, TNCC, NCC, NCCENR, LIBELLE
        with conn.cursor() as cur:
            # Préparation pour COPY
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, 
                     columns=['REG', 'LIBELLE'])  # On prend REG et LIBELLE
            
            output.seek(0)
            
            # Import
            cur.copy_expert(
                "COPY region (reg_id, name) FROM STDIN",
                output
            )
            conn.commit()
            print(f"Importation réussie: {len(df)} régions importées")
            
    except Exception as e:
        print(f"Erreur lors de l'importation des régions: {str(e)}")
        conn.rollback()
        raise

def import_departements(conn):
    """Importe les données des départements"""
    df = pd.read_csv('v_departement_2024.csv')
    
    with conn.cursor() as cur:
        # Préparation pour COPY
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False, 
                 columns=['DEP', 'LIBELLE', 'REG'])
        output.seek(0)
        
        # Import
        cur.copy_expert(
            "COPY departement (dep_id, name, reg_id) FROM STDIN",
            output
        )
        conn.commit()

def import_communes(conn):
    """Importe les données des communes"""
    df = pd.read_csv('v_commune_2024.csv')
    df = df[df['TYPECOM'] == 'COM']  # Seulement les communes principales
    
    with conn.cursor() as cur:
        # Préparation pour COPY
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False, 
                 columns=['COM', 'LIBELLE', 'DEP'])
        output.seek(0)
        
        # Import
        cur.copy_expert(
            "COPY commune (code_insee, name, dep_id) FROM STDIN",
            output
        )
        conn.commit()

def import_chefs_lieux(conn):
    """Importe les chefs-lieux de région et département"""
    try:
        # Chefs-lieux de région
        df_reg = pd.read_csv('v_region_2024.csv')
        print("Colonnes pour chefs-lieux région:", df_reg.columns.tolist())
        
        with conn.cursor() as cur:
            # Vider les tables avant import (optionnel)
            cur.execute("TRUNCATE chef_lieu_region, chef_lieu_departement;")
            
            # Régions
            for _, row in df_reg.iterrows():
                cur.execute(
                    "INSERT INTO chef_lieu_region (reg_id, com_id) "
                    "VALUES (%s, (SELECT com_id FROM commune WHERE code_insee = %s))",
                    (row['REG'], row['CHEFLIEU'])
                )
            
            # Départements
            df_dep = pd.read_csv('v_departement_2024.csv')
            
            for _, row in df_dep.iterrows():
                cur.execute(
                    "INSERT INTO chef_lieu_departement (dep_id, com_id) "
                    "VALUES (%s, (SELECT com_id FROM commune WHERE code_insee = %s))",
                    (row['DEP'], row['CHEFLIEU'])
                )
            
            conn.commit()
            print("Importation chefs-lieux réussie")
            
    except Exception as e:
        print(f"Erreur chefs-lieux: {str(e)}")
        conn.rollback()
        raise


def importer_types_statistiques(conn):
    """Importe tous les types de statistiques nécessaires"""
    types_stats = [
        # Population
        ('P21_POP', 'Population en 2021'),
        ('P15_POP', 'Population en 2015'),
        ('P10_POP', 'Population en 2010'),
        ('D99_POP', 'Population en 1999'),
        ('D90_POP', 'Population en 1990'),
        ('D82_POP', 'Population en 1982'),
        ('D75_POP', 'Population en 1975'),
        ('D68_POP', 'Population en 1968'),
        
        # Superficie
        ('SUPERF', 'Superficie en km²'),
        
        # Logements
        ('P21_LOG', 'Logements en 2021'),
        ('P15_LOG', 'Logements en 2015'),
        ('P10_LOG', 'Logements en 2010'),
        ('D99_LOG', 'Logements en 1999'),
        ('D90_LOG', 'Logements en 1990'),
        ('D82_LOG', 'Logements en 1982'),
        ('D75_LOG', 'Logements en 1975'),
        ('D68_LOG', 'Logements en 1968'),
        
        # Naissances et décès
        ('NAIS1520', 'Naissances 2015-2020'),
        ('NAIS1014', 'Naissances 2010-2014'),
        ('NAIS9909', 'Naissances 1999-2009'),
        ('NAIS9099', 'Naissances 1990-1999'),
        ('NAIS8290', 'Naissances 1982-1990'),
        ('NAIS7582', 'Naissances 1975-1982'),
        ('NAIS6875', 'Naissances 1968-1975'),
        
        ('DECE1520', 'Décès 2015-2020'),
        ('DECE1014', 'Décès 2010-2014'),
        ('DECE9909', 'Décès 1999-2009'),
        ('DECE9099', 'Décès 1990-1999'),
        ('DECE8290', 'Décès 1982-1990'),
        ('DECE7582', 'Décès 1975-1982'),
        ('DECE6875', 'Décès 1968-1975')
    ]
    
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO type_statistique (nom, description) VALUES (%s, %s) ON CONFLICT (nom) DO NOTHING",
            types_stats
        )
        conn.commit()
        print(f"{len(types_stats)} types de statistiques ajoutés")


def importer_statistiques_communes(conn, fichier_csv):
    """Importe toutes les statistiques depuis le fichier INSEE de manière optimisée"""
    try:
        # 1. Chargement et préparation des données
        df = pd.read_csv(fichier_csv, sep=';', dtype={'CODGEO': str})
        df['CODGEO'] = df['CODGEO'].str.zfill(5)  # Formatage des codes INSEE sur 5 chiffres

        # 2. Définition des mappings complets avec les années associées
        mappings = {
            # Population (avec années déduites)
            'P21_POP': ('P21_POP', 2021),
            'P15_POP': ('P15_POP', 2015),
            'P10_POP': ('P10_POP', 2010),
            'D99_POP': ('D99_POP', 1999),
            'D90_POP': ('D90_POP', 1990),
            'D82_POP': ('D82_POP', 1982),
            'D75_POP': ('D75_POP', 1975),
            'D68_POP': ('D68_POP', 1968),
            
            # Superficie (pas d'année)
            'SUPERF': ('SUPERF', None),
            
            # Logements
            'P21_LOG': ('P21_LOG', 2021),
            'P15_LOG': ('P15_LOG', 2015),
            'P10_LOG': ('P10_LOG', 2010),
            'D99_LOG': ('D99_LOG', 1999),
            'D90_LOG': ('D90_LOG', 1990),
            'D82_LOG': ('D82_LOG', 1982),
            'D75_LOG': ('D75_LOG', 1975),
            'D68_LOG': ('D68_LOG', 1968),
            
            # Naissances (périodes sans année spécifique)
            'NAIS1520': ('NAIS1520', None),
            'NAIS1014': ('NAIS1014', None),
            'NAIS9909': ('NAIS9909', None),
            'NAIS9099': ('NAIS9099', None),
            'NAIS8290': ('NAIS8290', None),
            'NAIS7582': ('NAIS7582', None),
            'NAIS6875': ('NAIS6875', None),
            
            # Décès (périodes sans année spécifique)
            'DECE1520': ('DECE1520', None),
            'DECE1014': ('DECE1014', None),
            'DECE9909': ('DECE9909', None),
            'DECE9099': ('DECE9099', None),
            'DECE8290': ('DECE8290', None),
            'DECE7582': ('DECE7582', None),
            'DECE6875': ('DECE6875', None),
            
        }

        with conn.cursor() as cur:
            # 3. Récupération des IDs des types
            cur.execute("SELECT id, nom FROM type_statistique")
            type_ids = {nom: id for id, nom in cur.fetchall()}

            # 4. Préparation de la requête d'insertion
            insert_query = """
            INSERT INTO statistique (com_id, type_id, annee, valeur)
            SELECT c.com_id, %s, %s, %s
            FROM commune c 
            WHERE c.code_insee = %s
            ON CONFLICT (com_id, type_id, annee) DO NOTHING
            """

            # 5. Parcours des données
            total = 0
            for csv_col, (stat_name, year) in mappings.items():
                if csv_col not in df.columns:
                    print(f"Colonne {csv_col} non trouvée - ignorée")
                    continue

                type_id = type_ids.get(stat_name)
                if not type_id:
                    print(f"Type {stat_name} non trouvé - ignoré")
                    continue

                # 6. Filtrage des données valides
                valid_rows = df[['CODGEO', csv_col]].dropna()
                valid_rows = valid_rows[valid_rows[csv_col] != '']

                # 7. Insertion des données
                for _, row in valid_rows.iterrows():
                    try:
                        valeur = float(row[csv_col])
                        cur.execute(
                            insert_query,
                            (type_id, year, valeur, row['CODGEO'])
                        )
                        total += 1
                    except ValueError as e:
                        print(f"Valeur invalide pour {row['CODGEO']} {stat_name}: {row[csv_col]}")
                    except Exception as e:
                        print(f"Erreur sur {row['CODGEO']} {stat_name}: {str(e)}")
                        conn.rollback()
                        raise

                conn.commit()
                print(f"{csv_col} ({stat_name}) importé: {len(valid_rows)} lignes")

            print(f"\nImportation terminée: {total} enregistrements au total")

    except Exception as e:
        conn.rollback()
        print(f"\nERREUR IMPORTATION: {str(e)}")
        raise
    

def insert_batch(cur, data_batch):
    """Insère un lot de données de manière optimisée"""
    args = ','.join(cur.mogrify("(%s,%s,%s,%s)", row).decode('utf-8') for row in data_batch)
    cur.execute(f"""
        INSERT INTO statistique (com_id, type_id, annee, valeur)
        VALUES (
            (SELECT com_id FROM commune WHERE code_insee = tmp.code_insee),
            tmp.type_id, tmp.annee, tmp.valeur
        )
        FROM (VALUES {args}) AS tmp(code_insee, type_id, annee, valeur)
        ON CONFLICT (com_id, type_id, annee) DO NOTHING
    """)
    
def verify_import(conn):
    """Vérifie que les données ont bien été importées"""
    with conn.cursor() as cur:
        print("\nVÉRIFICATION IMPORTATION:")
        
        # Compter les régions
        cur.execute("SELECT COUNT(*) FROM region")
        print(f"- Régions importées: {cur.fetchone()[0]}")
        
        # Compter les départements
        cur.execute("SELECT COUNT(*) FROM departement")
        print(f"- Départements importés: {cur.fetchone()[0]}")
        
        # Compter les communes
        cur.execute("SELECT COUNT(*) FROM commune")
        print(f"- Communes importées: {cur.fetchone()[0]}")
        
        # Vérifier chefs-lieux régions
        cur.execute("""
            SELECT r.reg_id, r.name, c.name 
            FROM region r
            JOIN chef_lieu_region clr ON r.reg_id = clr.reg_id
            JOIN commune c ON clr.com_id = c.com_id
            LIMIT 5
        """)
        print("\nExemple chefs-lieux région:")
        for reg_id, reg_name, com_name in cur.fetchall():
            print(f"{reg_id} {reg_name}: {com_name}")
            

def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_tables(conn)
        
        # Ordre important pour les contraintes de clé étrangère
        import_regions(conn)
        import_departements(conn)
        import_communes(conn)
        import_chefs_lieux(conn)
        importer_types_statistiques(conn)
        importer_statistiques_communes(conn, "base-cc-serie-historique-2021.csv")
        verify_import(conn)
        print("Importation terminée avec succès")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()