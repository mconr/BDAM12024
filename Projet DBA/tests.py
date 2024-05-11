import psycopg2

def connect_to_database():
    # Connexion à la base de données
    conn = psycopg2.connect("dbname='bdam12025' user='postgres' password='admin'")
    return conn

def list_departements_by_region(conn, reg):
    cur = conn.cursor()

    # Requête : liste des départements d'une région donnée
    cur.execute("""
        SELECT nom_departement
        FROM departements
        WHERE reg = %(reg)s
    """, {'reg': reg})
    departements = cur.fetchall()

    cur.close()
    return departements

def list_communes_above_population(conn, population, dep):
    cur = conn.cursor()

    # Requête : liste des communes de plus de X habitants d'un département donné
    cur.execute("""
        SELECT ncc
        FROM communes
        WHERE population > %(population)s AND dep = %(dep)s
    """, {'population': population, 'dep': dep})
    communes = cur.fetchall()

    cur.close()
    return communes

def get_most_least_populated_region(conn):
    cur = conn.cursor()

    # Requête : région la plus peuplée
    cur.execute("""
        SELECT ncc
        FROM regions
        WHERE population = (
            SELECT MAX(population) FROM regions
        )
    """)
    region_plus_peuplee = cur.fetchone()

    # Requête : région la moins peuplée
    cur.execute("""
        SELECT ncc
        FROM regions
        WHERE population = (
            SELECT MIN(population) FROM regions
        )
    """)
    region_moins_peuplee = cur.fetchone()

    cur.close()
    return region_plus_peuplee, region_moins_peuplee

def main():
    conn = connect_to_database()

    code_region = "01"
    departements = list_departements_by_region(conn, code_region)
    print("Liste des départements de la région :", departements)

    population_threshold = 10000
    code_departement = "01"
    communes = list_communes_above_population(conn, population_threshold, code_departement)
    print("Liste des communes de plus de", population_threshold, "habitants dans le département :", communes)

    region_plus_peuplee, region_moins_peuplee = get_most_least_populated_region(conn)
    print("Région la plus peuplée :", region_plus_peuplee)
    print("Région la moins peuplée :", region_moins_peuplee)


    conn.close()

if __name__ == "__main__":
    main()
