-- Nouvelle version de la vue population_departements
CREATE OR REPLACE VIEW population_departements AS
SELECT 
    d.dep_id as code_departement,
    d.name as departement,
    CASE 
        WHEN ts.nom = 'D68_POP' THEN 1968
        WHEN ts.nom = 'D75_POP' THEN 1975
        WHEN ts.nom = 'D82_POP' THEN 1982
        WHEN ts.nom = 'D90_POP' THEN 1990
        WHEN ts.nom = 'D99_POP' THEN 1999
        WHEN ts.nom = 'P10_POP' THEN 2010
        WHEN ts.nom = 'P15_POP' THEN 2015
        WHEN ts.nom = 'P21_POP' THEN 2021
        ELSE NULL
    END as annee,
    SUM(s.valeur) as population
FROM departement d
JOIN commune c ON d.dep_id = c.dep_id
JOIN statistique s ON c.com_id = s.com_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom LIKE '%_POP'
GROUP BY d.dep_id, d.name, ts.nom
ORDER BY d.name, annee;

-- Nouvelle version de la vue population_regions
CREATE OR REPLACE VIEW population_regions AS
SELECT 
    r.reg_id as code_region,
    r.name as region,
    CASE 
        WHEN ts.nom = 'D68_POP' THEN 1968
        WHEN ts.nom = 'D75_POP' THEN 1975
        WHEN ts.nom = 'D82_POP' THEN 1982
        WHEN ts.nom = 'D90_POP' THEN 1990
        WHEN ts.nom = 'D99_POP' THEN 1999
        WHEN ts.nom = 'P10_POP' THEN 2010
        WHEN ts.nom = 'P15_POP' THEN 2015
        WHEN ts.nom = 'P21_POP' THEN 2021
        ELSE NULL
    END as annee,
    SUM(s.valeur) as population,
    COUNT(DISTINCT c.com_id) as nombre_communes,
    ROUND(AVG(s.valeur), 2) as population_moyenne_commune
FROM region r
JOIN departement d ON r.reg_id = d.reg_id
JOIN commune c ON d.dep_id = c.dep_id
JOIN statistique s ON c.com_id = s.com_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom LIKE '%_POP'
GROUP BY r.reg_id, r.name, ts.nom
ORDER BY r.name, annee;