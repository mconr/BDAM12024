CREATE OR REPLACE PROCEDURE calculer_population_derniere_annee()
LANGUAGE plpgsql AS $$
BEGIN
    -- Mise à jour des communes
    UPDATE commune c SET
        population_derniere_annee = derniere.valeur,
        annee_derniere_pop = derniere.annee
    FROM (
        SELECT s.com_id, 
               SUBSTRING(ts.nom FROM 'P([0-9]+)_POP')::INTEGER as annee,
               s.valeur
        FROM statistique s
        JOIN type_statistique ts ON s.type_id = ts.id
        WHERE ts.nom LIKE '%_POP'
        ORDER BY s.com_id, SUBSTRING(ts.nom FROM 'P([0-9]+)_POP')::INTEGER DESC
    ) derniere
    WHERE c.com_id = derniere.com_id;
    
    -- Calcul des départements
    INSERT INTO population_departement (dep_id, annee, population)
    SELECT 
        c.dep_id,
        MAX(c.annee_derniere_pop),
        SUM(c.population_derniere_annee)
    FROM commune c
    GROUP BY c.dep_id
    ON CONFLICT (dep_id, annee) DO UPDATE SET
        population = EXCLUDED.population;
    
    -- Calcul des régions
    INSERT INTO population_region (reg_id, annee, population)
    SELECT 
        d.reg_id,
        MAX(pd.annee),
        SUM(pd.population)
    FROM population_departement pd
    JOIN departement d ON pd.dep_id = d.dep_id
    GROUP BY d.reg_id
    ON CONFLICT (reg_id, annee) DO UPDATE SET
        population = EXCLUDED.population;
END;
$$;