-- Crear base de datos
CREATE DATABASE IF NOT EXISTS workshop1
CHARACTER SET utf8mb4
COLLATE utf8mb4_general_ci;

USE workshop1;

-- ==============================
-- DIMENSION: CANDIDATE
-- ==============================

DROP TABLE IF EXISTS dim_candidate;

CREATE TABLE dim_candidate (
  candidate_key INT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(45) NOT NULL,
  last_name VARCHAR(45) NOT NULL,
  email VARCHAR(250) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- ==============================
-- DIMENSION: COUNTRY
-- ==============================

DROP TABLE IF EXISTS dim_country;

CREATE TABLE dim_country (
  country_key INT AUTO_INCREMENT PRIMARY KEY,
  country VARCHAR(150) NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- DIMENSION: DATE
-- ==============================

DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
  date_key INT PRIMARY KEY,
  month INT NOT NULL,
  day INT NOT NULL,
  year INT NOT NULL,
  full_date DATE NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- DIMENSION: SENIORITY
-- ==============================

DROP TABLE IF EXISTS dim_seniority;

CREATE TABLE dim_seniority (
  seniority_key INT AUTO_INCREMENT PRIMARY KEY,
  seniority VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- DIMENSION: TECHNOLOGY
-- ==============================

DROP TABLE IF EXISTS dim_technology;

CREATE TABLE dim_technology (
  technology_key INT AUTO_INCREMENT PRIMARY KEY,
  technology VARCHAR(45) NOT NULL
) ENGINE=InnoDB;

-- ==============================
-- FACT TABLE: APPLICATION
-- ==============================

DROP TABLE IF EXISTS fact_application;

CREATE TABLE fact_application (
  application_key INT AUTO_INCREMENT PRIMARY KEY,
  code_challenge_score INT NOT NULL,
  technical_interview_score INT NOT NULL,
  yoe INT NOT NULL,
  hired_flag TINYINT(1) NOT NULL DEFAULT 0,
  country_key INT NOT NULL,
  candidate_key INT NOT NULL,
  technology_key INT NOT NULL,
  seniority_key INT NOT NULL,
  date_key INT NOT NULL,

  INDEX (country_key),
  INDEX (candidate_key),
  INDEX (technology_key),
  INDEX (seniority_key),
  INDEX (date_key),

  CONSTRAINT fk_fact_country
    FOREIGN KEY (country_key)
    REFERENCES dim_country(country_key),

  CONSTRAINT fk_fact_candidate
    FOREIGN KEY (candidate_key)
    REFERENCES dim_candidate(candidate_key),

  CONSTRAINT fk_fact_technology
    FOREIGN KEY (technology_key)
    REFERENCES dim_technology(technology_key),

  CONSTRAINT fk_fact_seniority
    FOREIGN KEY (seniority_key)
    REFERENCES dim_seniority(seniority_key),

  CONSTRAINT fk_fact_date
    FOREIGN KEY (date_key)
    REFERENCES dim_date(date_key)

) ENGINE=InnoDB;
