-- Ajout des colonnes de cache
ALTER TABLE commune ADD COLUMN derniere_population INTEGER;
ALTER TABLE commune ADD COLUMN annee_derniere_pop INTEGER;

-- Tables de population
CREATE TABLE IF NOT EXISTS population_commune (
    com_id INTEGER REFERENCES commune(com_id),
    annee INTEGER,
    population INTEGER NOT NULL,
    PRIMARY KEY (com_id, annee)
);

CREATE TABLE IF NOT EXISTS population_departement (
    dep_id VARCHAR(3) REFERENCES departement(dep_id),
    annee INTEGER,
    population INTEGER NOT NULL,
    date_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dep_id, annee)
);

CREATE TABLE IF NOT EXISTS population_region (
    reg_id VARCHAR(2) REFERENCES region(reg_id),
    annee INTEGER,
    population INTEGER NOT NULL,
    date_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (reg_id, annee)
);