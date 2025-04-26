-- Fonction pour bloquer les modifications
CREATE OR REPLACE FUNCTION bloquer_modification_region_departement()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Les modifications directes sur les régions et départements sont interdites';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger pour la table region
CREATE TRIGGER tr_bloquer_region
BEFORE INSERT OR UPDATE OR DELETE ON region
FOR EACH STATEMENT EXECUTE FUNCTION bloquer_modification_region_departement();

-- Trigger pour la table departement
CREATE TRIGGER tr_bloquer_departement
BEFORE INSERT OR UPDATE OR DELETE ON departement
FOR EACH STATEMENT EXECUTE FUNCTION bloquer_modification_region_departement();

-- Trigger pour mettre à jour les populations agrégées
CREATE OR REPLACE FUNCTION after_update_statistique()
RETURNS TRIGGER AS $$
BEGIN
    -- Ne mettre à jour que si c'est une statistique de population
    IF (SELECT nom FROM type_statistique WHERE id = NEW.type_id) = 'Population' THEN
        PERFORM calculer_population_agregee(NEW.annee);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_after_update_statistique
AFTER INSERT OR UPDATE ON statistique
FOR EACH ROW EXECUTE FUNCTION after_update_statistique();