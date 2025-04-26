-- Table REGION
CREATE TABLE region (
    reg_id VARCHAR(2) PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- Table DEPARTEMENT
CREATE TABLE departement (
    dep_id VARCHAR(3) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    reg_id VARCHAR(2) NOT NULL REFERENCES region(reg_id)
);

-- Table COMMUNE
CREATE TABLE commune (
    com_id SERIAL PRIMARY KEY,
    code_insee VARCHAR(5) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    dep_id VARCHAR(3) NOT NULL REFERENCES departement(dep_id)
);

-- Table CHEFLIEUREGION
CREATE TABLE chef_lieu_region (
    reg_id VARCHAR(2) PRIMARY KEY REFERENCES region(reg_id),
    com_id INTEGER NOT NULL REFERENCES commune(com_id)
);

-- Table CHEFLIEUDEPARTEMENT
CREATE TABLE chef_lieu_departement (
    dep_id VARCHAR(3) PRIMARY KEY REFERENCES departement(dep_id),
    com_id INTEGER NOT NULL REFERENCES commune(com_id)
);

-- Table TYPESTATISTIQUE
CREATE TABLE type_statistique (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(50) NOT NULL UNIQUE,
    description TEXT
);

-- Table STATISTIQUE
CREATE TABLE statistique (
    id SERIAL PRIMARY KEY,
    com_id INTEGER NOT NULL REFERENCES commune(com_id),
    type_id INTEGER NOT NULL REFERENCES type_statistique(id),
    annee INTEGER NOT NULL,
    annee_fin INTEGER,
    valeur NUMERIC,
    CONSTRAINT valid_years CHECK (annee_fin IS NULL OR annee_fin >= annee),
    UNIQUE (com_id, type_id, annee)
);

-- Index pour améliorer les performances
CREATE INDEX idx_statistique_com_id ON statistique(com_id);
CREATE INDEX idx_statistique_type_id ON statistique(type_id);
CREATE INDEX idx_statistique_annee ON statistique(annee);