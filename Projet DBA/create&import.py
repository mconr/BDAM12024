import psycopg2
import csv

def create_tables():
    # Connexion à la base de données
    conn = psycopg2.connect(" dbname='bdam12025' user='postgres' password='admin' ")
    cur = conn.cursor()

    # Création de la table regions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS regions (
            reg INTEGER PRIMARY KEY,
            cheflieu VARCHAR(5),
            ncc VARCHAR(200)
        )
    """)

    # Création de la table departements
    cur.execute("""
        CREATE TABLE IF NOT EXISTS departements (
            dep VARCHAR(3) PRIMARY KEY,
            reg INTEGER REFERENCES regions(reg),
            cheflieu VARCHAR(5),
            ncc  VARCHAR(200)
            
        )
    """)

    # Création de la table communes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS communes (
            com VARCHAR(5) PRIMARY KEY,
            reg INTEGER REFERENCES regions(reg),
            dep VARCHAR(3) REFERENCES departements(dep),
            ncc VARCHAR(200),
            annee VARCHAR(10) DEFAULT 2020,
            population INTEGER
        
        )
    """)
    
    
    # Supprimer les triggers pour faciliter les tests !
    
    cur.execute("""
        DROP TRIGGER IF EXISTS update_population_trigger ON communes;
        DROP TRIGGER IF EXISTS prevent_modification_regions ON regions;
        DROP TRIGGER IF EXISTS prevent_modification_departements ON departements;
        
    """)

    # Création de la vue pour afficher la population des régions pour différentes années
    cur.execute("""
        CREATE OR REPLACE VIEW population_regions AS
        SELECT r.reg, r.ncc AS nom_region, c.annee, SUM(c.population) AS population
        FROM regions r
        JOIN departements d ON r.reg = d.reg
        JOIN communes c ON d.dep = c.dep
        GROUP BY r.reg, r.ncc, c.annee;
    """)

    # Création de la procédure stockée pour calculer et mettre à jour la population des départements et des régions
    cur.execute("""
        CREATE OR REPLACE PROCEDURE calculer_population()
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- Mettre à jour la population des départements
            UPDATE departements d
            SET population = (
                SELECT SUM(population)
                FROM communes
                WHERE dep = d.dep
            );

            -- Mettre à jour la population des régions
            UPDATE regions r
            SET population = (
                SELECT SUM(population)
                FROM departements d
                WHERE r.reg = d.reg
            );
        END;
        $$;
    """)

    # Déclencheur pour mettre à jour automatiquement la population des départements et des régions
    cur.execute("""
        CREATE OR REPLACE FUNCTION update_population()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM calculer_population();
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER update_population_trigger
        AFTER INSERT OR UPDATE OR DELETE ON communes
        FOR EACH STATEMENT
        EXECUTE FUNCTION update_population();
    """)

    # Déclencheurs pour empêcher la modification des tables REGIONS et DEPARTEMENTS
    cur.execute("""
        CREATE OR REPLACE FUNCTION prevent_modification()
        RETURNS TRIGGER AS $$
        BEGIN
            RAISE EXCEPTION 'La modification des tables REGIONS et DEPARTEMENTS est interdite';
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER prevent_modification_regions
        BEFORE INSERT OR UPDATE OR DELETE ON regions
        FOR EACH STATEMENT
        EXECUTE FUNCTION prevent_modification();

        CREATE TRIGGER prevent_modification_departements
        BEFORE INSERT OR UPDATE OR DELETE ON departements
        FOR EACH STATEMENT
        EXECUTE FUNCTION prevent_modification();
    """)


    # Validation des changements dans la base de données
    conn.commit()

    cur.close()
    conn.close()





def import_data():
    # Connexion à la base de données
    conn = psycopg2.connect("dbname='bdam12025' user='postgres' password='admin'")

    cur = conn.cursor()

    # Désactiver les déclencheurs
    cur.execute("ALTER TABLE regions DISABLE TRIGGER ALL;")
    cur.execute("ALTER TABLE departements DISABLE TRIGGER ALL;")
    cur.execute("ALTER TABLE communes DISABLE TRIGGER ALL;")
    
    # Import des données des régions depuis le fichier CSV
    with open('regions.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            cur.execute("""
                INSERT INTO regions (reg, ncc, cheflieu)
                VALUES (%s, %s, %s)
            """, (row[0], row[3], row[1]))

    # Import des données des départements depuis le fichier CSV
    with open('departements.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            cur.execute("""
                INSERT INTO departements (dep, reg, ncc, cheflieu)
                VALUES (%s, %s, %s, %s)
            """, (row[0], row[1], row[4], row[2]))

    # Import des données des communes depuis le fichier CSV
    with open('communes.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            cur.execute("""
                INSERT INTO communes (com, reg, dep, ncc)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (com) DO NOTHING
            """, (row[1], row[2], row[3], row[7]))
        
    # Import de la population depuis le fichier CSV
    with open('population.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            cur.execute("""
                UPDATE communes
                SET population = %s
                WHERE com = %s
            """, (row[1], row[0]))

    # Réactiver les déclencheurs
    cur.execute("ALTER TABLE regions ENABLE TRIGGER ALL;")
    cur.execute("ALTER TABLE departements ENABLE TRIGGER ALL;")
    cur.execute("ALTER TABLE communes ENABLE TRIGGER ALL;")

    # Validation des changements dans la base de données
    conn.commit()
    cur.close()
    conn.close()



create_tables()
import_data()
