CREATE DATABASE  IF NOT EXISTS `incautaciones_dw` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `incautaciones_dw`;
-- MySQL dump 10.13  Distrib 8.0.43, for Win64 (x86_64)
--
-- Host: localhost    Database: incautaciones_dw
-- ------------------------------------------------------
-- Server version	8.0.43

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `dim_autoridad`
--

DROP TABLE IF EXISTS `dim_autoridad`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_autoridad` (
  `autoridad_key` int NOT NULL,
  `autoridad_que_incauto` varchar(255) NOT NULL,
  PRIMARY KEY (`autoridad_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_especie`
--

DROP TABLE IF EXISTS `dim_especie`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_especie` (
  `especie_key` int NOT NULL,
  `tipo_especie` varchar(100) NOT NULL,
  `nombre_comun` varchar(255) NOT NULL,
  `nombre_cientifico` varchar(255) NOT NULL,
  PRIMARY KEY (`especie_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_tiempo`
--

DROP TABLE IF EXISTS `dim_tiempo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_tiempo` (
  `tiempo_key` int NOT NULL,
  `anio` int NOT NULL,
  PRIMARY KEY (`tiempo_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dim_ubicacion`
--

DROP TABLE IF EXISTS `dim_ubicacion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_ubicacion` (
  `ubicacion_key` int NOT NULL,
  `departamento` varchar(100) NOT NULL,
  `municipio` varchar(100) NOT NULL,
  `lugar_decomiso` varchar(255) NOT NULL,
  PRIMARY KEY (`ubicacion_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `fact_incautaciones`
--

DROP TABLE IF EXISTS `fact_incautaciones`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `fact_incautaciones` (
  `id` int NOT NULL AUTO_INCREMENT,
  `tiempo_key` int NOT NULL,
  `ubicacion_key` int NOT NULL,
  `especie_key` int NOT NULL,
  `autoridad_key` int NOT NULL,
  `situacion` varchar(100) NOT NULL,
  `cantidad` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `tiempo_key` (`tiempo_key`),
  KEY `ubicacion_key` (`ubicacion_key`),
  KEY `especie_key` (`especie_key`),
  KEY `autoridad_key` (`autoridad_key`),
  CONSTRAINT `fact_incautaciones_ibfk_1` FOREIGN KEY (`tiempo_key`) REFERENCES `dim_tiempo` (`tiempo_key`),
  CONSTRAINT `fact_incautaciones_ibfk_2` FOREIGN KEY (`ubicacion_key`) REFERENCES `dim_ubicacion` (`ubicacion_key`),
  CONSTRAINT `fact_incautaciones_ibfk_3` FOREIGN KEY (`especie_key`) REFERENCES `dim_especie` (`especie_key`),
  CONSTRAINT `fact_incautaciones_ibfk_4` FOREIGN KEY (`autoridad_key`) REFERENCES `dim_autoridad` (`autoridad_key`)
) ENGINE=InnoDB AUTO_INCREMENT=32768 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2026-03-03 16:39:57
