-- Population des départements
CREATE OR REPLACE VIEW vue_population_departement AS
SELECT 
    d.dep_id,
    d.name as departement,
    r.name as region,
    s.annee,
    SUM(s.valeur) as population
FROM statistique s
JOIN commune c ON s.com_id = c.com_id
JOIN departement d ON c.dep_id = d.dep_id
JOIN region r ON d.reg_id = r.reg_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom = 'Population'
GROUP BY d.dep_id, d.name, r.name, s.annee
ORDER BY d.name, s.annee;

-- Population des régions
CREATE OR REPLACE VIEW vue_population_region AS
SELECT 
    r.reg_id,
    r.name as region,
    s.annee,
    SUM(s.valeur) as population
FROM statistique s
JOIN commune c ON s.com_id = c.com_id
JOIN departement d ON c.dep_id = d.dep_id
JOIN region r ON d.reg_id = r.reg_id
JOIN type_statistique ts ON s.type_id = ts.id
WHERE ts.nom = 'Population'
GROUP BY r.reg_id, r.name, s.annee
ORDER BY r.name, s.annee;