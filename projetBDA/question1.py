import psycopg2
from tabulate import tabulate
import pandas as pd
from io import StringIO

DB_CONFIG = {
    'host': 'localhost',
    'database': 'inseedb',
    'user': 'postgres',
    'password': 'admin'
}

def get_departments_in_region(region_name):
    """Liste des départements d'une région donnée"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.dep_id, d.name, c.name as chef_lieu
                FROM departement d
                JOIN region r ON d.reg_id = r.reg_id
                JOIN chef_lieu_departement cld ON d.dep_id = cld.dep_id
                JOIN commune c ON cld.com_id = c.com_id
                WHERE r.name = %s
                ORDER BY d.name
            """, (region_name,))
            
            results = cur.fetchall()
            print(f"\nDépartements de la région {region_name}:")
            print(tabulate(results, headers=['Code', 'Département', 'Chef-lieu'], tablefmt='pretty'))
            
    finally:
        conn.close()

def get_communes_above_population(department_code, min_population):
    """Communes de plus de X habitants dans un département"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.name, s.valeur as population
                FROM commune c
                JOIN statistique s ON c.com_id = s.com_id
                JOIN type_statistique ts ON s.type_id = ts.id
                WHERE c.dep_id = %s 
                AND ts.nom = 'P21_POP'
                AND s.valeur > %s
                ORDER BY s.valeur DESC
            """, (department_code, min_population))
            
            results = cur.fetchall()
            print(f"\nCommunes de plus de {min_population} habitants dans le département {department_code}:")
            print(tabulate(results, headers=['Commune', 'Population'], tablefmt='pretty'))
            
    finally:
        conn.close()

def population_growth_rate(start_year, end_year):
    """Taux de croissance démographique par région"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH pop_start AS (
                    SELECT r.reg_id, r.name, SUM(s.valeur) as population
                    FROM region r
                    JOIN departement d ON r.reg_id = d.reg_id
                    JOIN commune c ON d.dep_id = c.dep_id
                    JOIN statistique s ON c.com_id = s.com_id
                    JOIN type_statistique ts ON s.type_id = ts.id
                    WHERE ts.nom = %s
                    GROUP BY r.reg_id, r.name
                ),
                pop_end AS (
                    SELECT r.reg_id, SUM(s.valeur) as population
                    FROM region r
                    JOIN departement d ON r.reg_id = d.reg_id
                    JOIN commune c ON d.dep_id = c.dep_id
                    JOIN statistique s ON c.com_id = s.com_id
                    JOIN type_statistique ts ON s.type_id = ts.id
                    WHERE ts.nom = %s
                    GROUP BY r.reg_id
                )
                SELECT 
                    ps.name as region,
                    ps.population as pop_start,
                    pe.population as pop_end,
                    ROUND((pe.population - ps.population) * 100.0 / ps.population, 2) as growth_rate
                FROM pop_start ps
                JOIN pop_end pe ON ps.reg_id = pe.reg_id
                ORDER BY growth_rate DESC
            """, (f'P{start_year}_POP', f'P{end_year}_POP'))
            
            results = cur.fetchall()
            print(f"\nTaux de croissance démographique entre {start_year} et {end_year} par région:")
            print(tabulate(results, headers=['Région', f'Pop {start_year}', f'Pop {end_year}', 'Taux (%)'], tablefmt='pretty'))
            
    finally:
        conn.close()
        
def explorer_donnees(conn):
    """Requêtes analytiques de base avec affichage amélioré"""
    from tabulate import tabulate

    requetes = {
        # 1. Top 5 des communes les plus peuplées (en 2021)
        "Top 5 des communes les plus peuplées (2021)": {
            "requete": """
                SELECT c.name as commune, d.name as departement, s.valeur as population 
                FROM commune c
                JOIN departement d ON c.dep_id = d.dep_id
                JOIN statistique s ON c.com_id = s.com_id
                JOIN type_statistique ts ON s.type_id = ts.id
                WHERE ts.nom = 'P21_POP'
                ORDER BY s.valeur DESC 
                LIMIT 5
            """,
            "headers": ["Commune", "Département", "Population"]
        },
        
        # 2. Population par région (en 2021)
        "Population par région (2021)": {
            "requete": """
                SELECT r.name as region, SUM(s.valeur) as population
                FROM region r
                JOIN departement d ON r.reg_id = d.reg_id
                JOIN commune c ON d.dep_id = c.dep_id
                JOIN statistique s ON c.com_id = s.com_id
                JOIN type_statistique ts ON s.type_id = ts.id
                WHERE ts.nom = 'P21_POP'
                GROUP BY r.reg_id, r.name
                ORDER BY population DESC
            """,
            "headers": ["Région", "Population"]
        },
        
        # 3. Évolution démographique 2015-2021
        "Evolution démographique 2015-2021": {
            "requete": """
                WITH pop_2015 AS (
                    SELECT c.com_id, c.name, s.valeur
                    FROM commune c
                    JOIN statistique s ON c.com_id = s.com_id
                    JOIN type_statistique ts ON s.type_id = ts.id
                    WHERE ts.nom = 'P15_POP'
                ),
                pop_2021 AS (
                    SELECT c.com_id, s.valeur
                    FROM commune c
                    JOIN statistique s ON c.com_id = s.com_id
                    JOIN type_statistique ts ON s.type_id = ts.id
                    WHERE ts.nom = 'P21_POP'
                )
                SELECT 
                    p15.name as commune,
                    d.name as departement,
                    p15.valeur as pop_2015,
                    p21.valeur as pop_2021,
                    ROUND((p21.valeur - p15.valeur) * 100.0 / p15.valeur, 2) as evolution_pct
                FROM pop_2015 p15
                JOIN pop_2021 p21 ON p15.com_id = p21.com_id
                JOIN commune c ON p15.com_id = c.com_id
                JOIN departement d ON c.dep_id = d.dep_id
                WHERE p15.valeur > 0  
                ORDER BY evolution_pct DESC
                LIMIT 5
            """,
            "headers": ["Commune", "Département", "Pop 2015", "Pop 2021", "Évolution (%)"]
        },
        
        # 4. Densité de population par département
        "Densité de population par département": {
            "requete": """
                SELECT 
                    d.name as departement,
                    ROUND(SUM(pop.valeur) / NULLIF(SUM(surf.valeur), 0), 2) as densite
                FROM departement d
                JOIN commune c ON d.dep_id = c.dep_id
                JOIN statistique pop ON c.com_id = pop.com_id
                JOIN type_statistique tpop ON pop.type_id = tpop.id
                JOIN statistique surf ON c.com_id = surf.com_id
                JOIN type_statistique tsurf ON surf.type_id = tsurf.id
                WHERE tpop.nom = 'P21_POP' AND tsurf.nom = 'SUPERF'
                GROUP BY d.dep_id, d.name
                ORDER BY densite DESC
                LIMIT 5
            """,
            "headers": ["Département", "Densité (hab/km²)"]
        }
    }

    with conn.cursor() as cur:
        for titre, data in requetes.items():
            print(f"\n\033[1m=== {titre} ===\033[0m")  # Texte en gras
            cur.execute(data["requete"])
            results = cur.fetchall()
            
            # Affichage avec tabulate
            print(tabulate(results, headers=data["headers"], tablefmt="pretty"))
            
            # Ajout d'une ligne vide entre les résultats
            print()
                      
# Exemples d'utilisation
if __name__ == "__main__":
    get_departments_in_region("Occitanie")
    get_communes_above_population("90", 1000)  # Paris
    population_growth_rate(15, 21)  # 2015-2021
    
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        explorer_donnees(conn)
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if conn:
            conn.close()