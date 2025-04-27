-- Protection des tables
CREATE OR REPLACE FUNCTION bloquer_modifications()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Modifications directes non autorisées sur %', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_protect_region
BEFORE INSERT OR UPDATE OR DELETE ON region
FOR EACH STATEMENT EXECUTE FUNCTION bloquer_modifications();

CREATE TRIGGER tr_protect_departement
BEFORE INSERT OR UPDATE OR DELETE ON departement
FOR EACH STATEMENT EXECUTE FUNCTION bloquer_modifications();

-- Mise à jour automatique
CREATE OR REPLACE FUNCTION after_stat_update()
RETURNS TRIGGER AS $$
DECLARE
    code_annee TEXT;
BEGIN
    -- Détection d'une nouvelle année
    SELECT SUBSTRING(ts.nom FROM 'P([0-9]+)_POP') INTO code_annee
    FROM type_statistique ts WHERE ts.id = NEW.type_id;
    
    IF code_annee IS NOT NULL THEN
        CALL calculer_population_derniere_annee();
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_after_stat_update
AFTER INSERT OR UPDATE ON statistique
FOR EACH ROW EXECUTE FUNCTION after_stat_update();