-- question6.sql - Analyse des performances avec EXPLAIN

-- PARTIE 1: COMPARAISON DES ALGORITHMES DE JOINTURE
-- ------------------------------------------------

-- Nettoyage des index existants
DROP INDEX IF EXISTS idx_departement_reg_id;
DROP INDEX IF EXISTS idx_commune_dep_id;
DROP INDEX IF EXISTS idx_statistique_com_id;
DROP INDEX IF EXISTS idx_statistique_type_id;
DROP INDEX IF EXISTS idx_type_statistique_nom;
DROP INDEX IF EXISTS idx_commune_name;
DROP INDEX IF EXISTS idx_departement_name;
DROP INDEX IF EXISTS idx_stat_pop_valeur;

-- Exemple 1: Jointure entre petite table (region) et grande table (commune via departement)
-- Sans index spécifiques
EXPLAIN ANALYZE
SELECT r.name AS region_name, COUNT(c.com_id) AS nb_communes
FROM region r
JOIN departement d ON r.reg_id = d.reg_id
JOIN commune c ON d.dep_id = c.dep_id
GROUP BY r.name;

-- Création des index pour optimiser cette requête
CREATE INDEX idx_departement_reg_id ON departement(reg_id);
CREATE INDEX idx_commune_dep_id ON commune(dep_id);

-- Répétition de la requête avec les index
EXPLAIN ANALYZE
SELECT r.name AS region_name, COUNT(c.com_id) AS nb_communes
FROM region r
JOIN departement d ON r.reg_id = d.reg_id
JOIN commune c ON d.dep_id = c.dep_id
GROUP BY r.name;

-- Exemple 2: Jointure entre deux grandes tables (communes et statistiques)
EXPLAIN ANALYZE
SELECT c.name, s.valeur
FROM commune c
JOIN statistique s ON c.com_id = s.com_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom = 'P21_POP'
LIMIT 100;

-- Création des index pour optimiser cette requête
CREATE INDEX idx_statistique_com_id ON statistique(com_id);
CREATE INDEX idx_statistique_type_id ON statistique(type_id);
CREATE INDEX idx_type_statistique_nom ON type_statistique(nom);

-- Répétition de la requête avec les index
EXPLAIN ANALYZE
SELECT c.name, s.valeur
FROM commune c
JOIN statistique s ON c.com_id = s.com_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom = 'P21_POP'
LIMIT 100;

-- Exemple 3: Jointure par fusion (merge join) avec des tables triées
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.name AS commune, d.name AS departement
FROM commune c
JOIN departement d ON c.dep_id = d.dep_id
ORDER BY c.name, d.name
LIMIT 100;

-- Création des index nécessaires pour un tri efficace
CREATE INDEX idx_commune_name ON commune(name);
CREATE INDEX idx_departement_name ON departement(name);

-- Répétition de la requête avec les index de tri
EXPLAIN (ANALYZE, BUFFERS)
SELECT c.name AS commune, d.name AS departement
FROM commune c
JOIN departement d ON c.dep_id = d.dep_id
ORDER BY c.name, d.name
LIMIT 100;

-- PARTIE 2: IMPACT DES INDEX SUR LES FILTRES
-- ------------------------------------------

-- Requête avec filtre sur population sans index
EXPLAIN ANALYZE
SELECT c.name, c.dep_id, s.valeur AS population
FROM commune c
JOIN statistique s ON c.com_id = s.com_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom = 'P21_POP' AND s.valeur < 5000
ORDER BY s.valeur DESC;

-- Création d'un index partiel pour optimiser ce filtre
-- D'abord on récupère l'ID du type de statistique
DO $$
DECLARE
    type_id_val INTEGER;
BEGIN
    SELECT id INTO type_id_val FROM type_statistique WHERE nom = 'P21_POP';
    EXECUTE format('CREATE INDEX idx_stat_pop_valeur ON statistique(valeur) WHERE type_id = %s', type_id_val);
END $$;

-- Répétition de la requête avec l'index
EXPLAIN ANALYZE
SELECT c.name, c.dep_id, s.valeur AS population
FROM commune c
JOIN statistique s ON c.com_id = s.com_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom = 'P21_POP' AND s.valeur < 5000
ORDER BY s.valeur DESC;

-- PARTIE 3: SÉLECTIONS AVANT JOINTURES
-- -----------------------------------

-- Vérification des sélections avant jointures
EXPLAIN ANALYZE
SELECT c.name, d.name, s.valeur
FROM commune c
JOIN departement d ON c.dep_id = d.dep_id
JOIN statistique s ON c.com_id = s.com_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom = 'P21_POP' 
  AND d.name LIKE 'Paris%'
  AND s.valeur > 10000;

-- PARTIE 4: REQUÊTE COMPLEXE AVEC PLAN D'EXÉCUTION LONG
-- ----------------------------------------------------

-- Requête d'analyse démographique complexe
EXPLAIN ANALYZE
WITH pop_actuelle AS (
    SELECT 
        c.com_id,
        c.name AS commune_nom,
        c.dep_id,
        s.valeur AS population_actuelle
    FROM commune c
    JOIN statistique s ON c.com_id = s.com_id
    JOIN type_statistique ts ON s.type_id = ts.id
    WHERE ts.nom = 'P21_POP'
),
pop_precedente AS (
    SELECT 
        c.com_id,
        s.valeur AS population_precedente
    FROM commune c
    JOIN statistique s ON c.com_id = s.com_id
    JOIN type_statistique ts ON s.type_id = ts.id
    WHERE ts.nom = 'P15_POP'
),
evolution_commune AS (
    SELECT 
        pa.com_id,
        pa.commune_nom,
        pa.dep_id,
        pa.population_actuelle,
        pp.population_precedente,
        CASE 
            WHEN pp.population_precedente > 0 
            THEN ROUND(((pa.population_actuelle - pp.population_precedente) * 100.0 / pp.population_precedente)::numeric, 2)
            ELSE NULL
        END AS evolution_pct
    FROM pop_actuelle pa
    LEFT JOIN pop_precedente pp ON pa.com_id = pp.com_id
),
stats_departement AS (
    SELECT 
        d.dep_id,
        d.name AS departement_nom,
        d.reg_id,
        COUNT(ec.com_id) AS nb_communes,
        SUM(ec.population_actuelle) AS population_totale,
        ROUND(AVG(ec.population_actuelle)::numeric, 2) AS population_moyenne,
        ROUND(AVG(ec.evolution_pct)::numeric, 2) AS evolution_moyenne
    FROM departement d
    JOIN evolution_commune ec ON d.dep_id = ec.dep_id
    GROUP BY d.dep_id, d.name, d.reg_id
),
stats_region AS (
    SELECT 
        r.reg_id,
        r.name AS region_nom,
        COUNT(DISTINCT sd.dep_id) AS nb_departements,
        SUM(sd.population_totale) AS population_totale,
        ROUND(AVG(sd.evolution_moyenne)::numeric, 2) AS evolution_moyenne
    FROM region r
    JOIN stats_departement sd ON r.reg_id = sd.reg_id
    GROUP BY r.reg_id, r.name
),
communes_classees AS (
    SELECT 
        ec.*,
        sd.departement_nom,
        sr.region_nom,
        RANK() OVER (PARTITION BY ec.dep_id ORDER BY ec.population_actuelle DESC) AS rang_departement,
        RANK() OVER (PARTITION BY sr.reg_id ORDER BY ec.population_actuelle DESC) AS rang_region
    FROM evolution_commune ec
    JOIN stats_departement sd ON ec.dep_id = sd.dep_id
    JOIN stats_region sr ON sd.reg_id = sr.reg_id
)
SELECT 
    cc.commune_nom,
    cc.departement_nom,
    cc.region_nom,
    cc.population_actuelle,
    cc.population_precedente,
    cc.evolution_pct,
    cc.rang_departement,
    cc.rang_region,
    sd.population_totale AS population_departement,
    sr.population_totale AS population_region
FROM communes_classees cc
JOIN stats_departement sd ON cc.dep_id = sd.dep_id
JOIN stats_region sr ON sd.reg_id = sr.reg_id
WHERE cc.rang_departement <= 3
ORDER BY sr.region_nom, sd.departement_nom, cc.rang_departement;