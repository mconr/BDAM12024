-- Vue pour les départements (version sécurisée)
CREATE OR REPLACE VIEW stats_population_departement AS
SELECT 
    d.dep_id,
    d.name,
    p.annee,
    p.population,
    (SELECT COUNT(*) FROM commune WHERE dep_id = d.dep_id) as nb_communes
FROM departement d
LEFT JOIN (
    SELECT s.com_id, 
           SUBSTRING(ts.nom FROM 'P([0-9]+)_POP')::INTEGER as annee,
           s.valeur as population
    FROM statistique s
    JOIN type_statistique ts ON s.type_id = ts.id
    WHERE ts.nom LIKE '%_POP'
) p ON d.dep_id = SUBSTRING(p.com_id::TEXT FROM 1 FOR 2)  -- Adaptation selon votre schéma
GROUP BY d.dep_id, d.name, p.annee, p.population;

-- Vue pour les régions
CREATE OR REPLACE VIEW stats_population_region AS
SELECT 
    r.reg_id,
    r.name,
    p.annee,
    SUM(p.population) as population,
    COUNT(DISTINCT d.dep_id) as nb_departements
FROM region r
JOIN departement d ON r.reg_id = d.reg_id
JOIN stats_population_departement p ON d.dep_id = p.dep_id
GROUP BY r.reg_id, r.name, p.annee;