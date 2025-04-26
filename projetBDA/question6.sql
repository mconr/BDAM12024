-- Requête complexe sans index
EXPLAIN ANALYZE
SELECT r.name, COUNT(c.com_id), SUM(pc.population)
FROM region r
JOIN departement d ON r.reg_id = d.reg_id
JOIN commune c ON d.dep_id = c.dep_id
JOIN (
    SELECT s.com_id, s.valeur as population
    FROM statistique s
    JOIN type_statistique ts ON s.type_id = ts.id
    WHERE ts.nom = 'P21_POP'
) pc ON c.com_id = pc.com_id
GROUP BY r.name;

-- Création d'index stratégiques
CREATE INDEX idx_statistique_type_valeur ON statistique(type_id, valeur);
CREATE INDEX idx_commune_departement ON commune(dep_id);

-- Ré-exécution avec index
EXPLAIN ANALYZE
SELECT r.name, COUNT(c.com_id), SUM(pc.population)
FROM region r
JOIN departement d ON r.reg_id = d.reg_id
JOIN commune c ON d.dep_id = c.dep_id
JOIN (
    SELECT s.com_id, s.valeur as population
    FROM statistique s
    JOIN type_statistique ts ON s.type_id = ts.id
    WHERE ts.nom = 'P21_POP'
) pc ON c.com_id = pc.com_id
GROUP BY r.name;