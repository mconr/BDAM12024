CREATE OR REPLACE PROCEDURE calculer_populations_aggregees(p_annee INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
    v_annee_text TEXT := 'P' || p_annee || '_POP';
BEGIN
    -- 1. Mise à jour des données communales
    UPDATE commune c SET
        derniere_population = s.valeur,
        annee_derniere_pop = p_annee
    FROM statistique s
    JOIN type_statistique ts ON s.type_id = ts.id
    WHERE s.com_id = c.com_id
    AND ts.nom = v_annee_text
    AND s.valeur IS NOT NULL;
    
    RAISE NOTICE 'Mise à jour des communes terminée';
    
    -- 2. Calcul des populations départementales (uniquement pour les départements complets)
    WITH dep_complets AS (
        SELECT c.dep_id
        FROM commune c
        LEFT JOIN statistique s ON c.com_id = s.com_id
        LEFT JOIN type_statistique ts ON s.type_id = ts.id AND ts.nom = v_annee_text
        GROUP BY c.dep_id
        HAVING COUNT(*) = COUNT(s.valeur)  -- Toutes les communes ont des données
    )
    INSERT INTO population_departement (dep_id, annee, population)
    SELECT 
        c.dep_id,
        p_annee,
        SUM(c.derniere_population)
    FROM commune c
    JOIN dep_complets dc ON c.dep_id = dc.dep_id
    WHERE c.annee_derniere_pop = p_annee
    AND c.derniere_population IS NOT NULL
    GROUP BY c.dep_id
    ON CONFLICT (dep_id, annee) DO UPDATE SET
        population = EXCLUDED.population,
        date_maj = CURRENT_TIMESTAMP;
    
    RAISE NOTICE 'Calcul départemental terminé';
    
    -- 3. Calcul des populations régionales (uniquement pour les régions complètes)
    WITH reg_complets AS (
        SELECT d.reg_id
        FROM departement d
        LEFT JOIN population_departement pd ON d.dep_id = pd.dep_id AND pd.annee = p_annee
        GROUP BY d.reg_id
        HAVING COUNT(*) = COUNT(pd.population)  -- Tous les départements ont des données
    )
    INSERT INTO population_region (reg_id, annee, population)
    SELECT 
        d.reg_id,
        p_annee,
        SUM(pd.population)
    FROM departement d
    JOIN population_departement pd ON d.dep_id = pd.dep_id
    JOIN reg_complets rc ON d.reg_id = rc.reg_id
    WHERE pd.annee = p_annee
    GROUP BY d.reg_id
    ON CONFLICT (reg_id, annee) DO UPDATE SET
        population = EXCLUDED.population,
        date_maj = CURRENT_TIMESTAMP;
    
    RAISE NOTICE 'Calcul régional terminé';
END;
$$;