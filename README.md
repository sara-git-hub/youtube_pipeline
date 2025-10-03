# YouTube Pipeline

[![CI](https://github.com/<votre-utilisateur>/youtube_pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/<votre-utilisateur>/youtube_pipeline/actions/workflows/ci.yml)

Pipeline ETL pour extraire, transformer et charger des données YouTube dans PostgreSQL, avec validation de qualité via Soda Core et orchestration via Apache Airflow.

---

## Table des matières

- [Description](#description)  
- [Architecture](#architecture)  
- [Prérequis](#prérequis)   

---

## Description

Cette pipeline effectue :

1. Extraction des données YouTube (vidéos, chaînes, playlists) via l'API YouTube.  
2. Transformation et nettoyage des données.  
3. Chargement dans PostgreSQL (`staging` et `core`).  
4. Vérification de la qualité des données avec **Soda Core**.  
5. Orchestration et scheduling des DAGs via **Airflow**.

---

## Architecture

YouTube API → Extractor → JSON → PostgreSQL Staging → Transform → PostgreSQL Core
↘ Soda Core (Data Quality Checks)
Airflow DAGs orchestrent le workflow complet

DAGs inclus :

- `youtube_data_extraction` : extraction et sauvegarde des vidéos.  
- `youtube_update_database` : mise à jour de la base `core`.  
- `data_quality` : vérification des données via Soda Core.

---

## Prérequis

- Python 3.11  
- PostgreSQL  
- Apache Airflow 2.x  
- Clé API YouTube  
- Soda Core pour la qualité des données  

---

