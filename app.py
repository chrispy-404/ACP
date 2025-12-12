-- Tabellen erstellen (falls sie noch nicht existieren)

CREATE TABLE IF NOT EXISTS mitarbeiter (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    personalnummer VARCHAR(50),
    telefon VARCHAR(50),
    aktiv TINYINT DEFAULT 1
);

CREATE TABLE IF NOT EXISTS standorte (
    id INT AUTO_INCREMENT PRIMARY KEY,
    objekt_name VARCHAR(100) NOT NULL,
    slot_name VARCHAR(50) NOT NULL,
    ansprechpartner VARCHAR(100),
    telefon VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS einsaetze (
    id INT AUTO_INCREMENT PRIMARY KEY,
    datum DATE NOT NULL,
    objekt_name VARCHAR(100),
    slot_name VARCHAR(50),
    mitarbeiter_name VARCHAR(100),
    start_zeit DECIMAL(5,2),
    end_zeit DECIMAL(5,2),
    pause DECIMAL(5,2),
    stunden_gesamt DECIMAL(5,2)
);

-- Ein paar Testdaten anlegen, damit du gleich was siehst
INSERT INTO mitarbeiter (name) VALUES ('Max Mustermann'), ('Erika Musterfrau'), ('Ali Yilmaz');
INSERT INTO standorte (objekt_name, slot_name) VALUES ('Haupttor', 'Schichtleiter'), ('Haupttor', 'Pförtner'), ('Empfang', 'Rezeption');
