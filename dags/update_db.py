from datetime import datetime, timedelta
import logging
import json
import os
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 18),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='youtube_update_database',
    default_args=default_args,
    description='Chargement des données YouTube en PostgreSQL avec architecture staging/core',
    schedule=None,
    catchup=False,
    tags=['youtube', 'database', 'etl'],
)
def youtube_update_database_dag():

    @task
    def get_latest_json_file():
        """Trouve le dernier fichier JSON généré"""
        try:
            data_path = Variable.get("youtube_data_path", "dags/data")
            if not os.path.exists(data_path):
                raise AirflowException(f"Le dossier {data_path} n'existe pas")
            json_files = [f for f in os.listdir(data_path) if f.endswith('.json')]
            if not json_files:
                raise AirflowException("Aucun fichier JSON trouvé")
            latest_file = max(json_files)
            file_path = os.path.join(data_path, latest_file)
            logging.info(f"📁 Fichier JSON sélectionné: {file_path}")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logging.info(f"📊 Données chargées: {len(data['videos'])} vidéos")
            return data
        except Exception as e:
            logging.error(f"Erreur lecture JSON: {str(e)}")
            raise AirflowException(f"Erreur lecture JSON: {str(e)}")

    @task
    def create_staging_schema():
        """Crée le schéma staging et les tables si nécessaire"""
        try:
            hook = PostgresHook(postgres_conn_id='postgres_youtube')
            hook.run("CREATE SCHEMA IF NOT EXISTS staging;")
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS staging.youtube_videos_raw (
                extraction_id SERIAL PRIMARY KEY,
                channel_handle VARCHAR(100),
                extraction_date TIMESTAMP,
                total_videos INTEGER,
                video_id VARCHAR(20) NOT NULL,
                title TEXT,
                duration VARCHAR(20),
                duration_readable VARCHAR(20),
                like_count BIGINT,
                view_count BIGINT,
                comment_count BIGINT,
                published_at TIMESTAMP,
                raw_data JSONB,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_video_id_raw ON staging.youtube_videos_raw(video_id);
            CREATE INDEX IF NOT EXISTS idx_published_at_raw ON staging.youtube_videos_raw(published_at);
            """
            hook.run(create_table_sql)
            logging.info("✅ Schéma staging créé/validé")
            return "staging_schema_created"
        except Exception as e:
            logging.error(f"Erreur création schéma staging: {str(e)}")
            raise

    @task
    def load_to_staging(json_data):
        """Charge les données brutes en table staging"""
        try:
            hook = PostgresHook(postgres_conn_id='postgres_youtube')
            records = []
            for video in json_data['videos']:
                like_count = int(video.get('like_count', 0))
                view_count = int(video.get('view_count', 0))
                comment_count = int(video.get('comment_count', 0))
                record = (
                    json_data['channel_handle'],
                    datetime.fromisoformat(json_data['extraction_date']),
                    json_data['total_videos'],
                    video['video_id'],
                    video.get('title'),
                    video.get('duration'),
                    video.get('duration_readable'),
                    like_count,
                    view_count,
                    comment_count,
                    datetime.fromisoformat(video['published_at'].replace('Z', '+00:00')),
                    json.dumps(video)
                )
                records.append(record)
            hook.insert_rows(
                table='staging.youtube_videos_raw',
                rows=records,
                target_fields=[
                    'channel_handle', 'extraction_date', 'total_videos',
                    'video_id', 'title', 'duration', 'duration_readable',
                    'like_count', 'view_count', 'comment_count', 'published_at', 'raw_data'
                ]
            )
            logging.info(f"✅ {len(records)} vidéos chargées en staging")
            return f"loaded_{len(records)}_videos"
        except Exception as e:
            logging.error(f"Erreur chargement staging: {str(e)}")
            raise

    @task
    def create_core_schema():
        """Crée le schéma core et les tables transformées"""
        try:
            hook = PostgresHook(postgres_conn_id='postgres_youtube')
            hook.run("CREATE SCHEMA IF NOT EXISTS core;")
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS core.youtube_videos (
                video_id VARCHAR(20) PRIMARY KEY,
                title TEXT NOT NULL,
                duration_seconds INTEGER,
                duration_formatted VARCHAR(20),
                like_count BIGINT,
                view_count BIGINT,
                comment_count BIGINT,
                published_at TIMESTAMP,
                channel_handle VARCHAR(100),
                content_type VARCHAR(50),
                engagement_ratio NUMERIC(10,4),
                is_short BOOLEAN,
                extraction_date TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS core.youtube_videos_history (
                history_id SERIAL PRIMARY KEY,
                video_id VARCHAR(20),
                like_count BIGINT,
                view_count BIGINT,
                comment_count BIGINT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (video_id) REFERENCES core.youtube_videos(video_id)
            );
            CREATE INDEX IF NOT EXISTS idx_video_published ON core.youtube_videos(published_at);
            CREATE INDEX IF NOT EXISTS idx_video_engagement ON core.youtube_videos(engagement_ratio);
            CREATE INDEX IF NOT EXISTS idx_history_video_date ON core.youtube_videos_history(video_id, recorded_at);
            """
            hook.run(create_table_sql)
            logging.info("✅ Schéma core créé/validé")
            return "core_schema_created"
        except Exception as e:
            logging.error(f"Erreur création schéma core: {str(e)}")
            raise

    @task
    def transform_to_core():
        """Transforme et charge les données en schéma core avec gestion des doublons"""
        try:
            hook = PostgresHook(postgres_conn_id='postgres_youtube')
            transform_sql = """
            WITH latest_extraction AS (
                SELECT MAX(extraction_date) as max_date
                FROM staging.youtube_videos_raw
            ),
            ranked_data AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY loaded_at DESC) as rn
                FROM staging.youtube_videos_raw
                WHERE extraction_date = (SELECT max_date FROM latest_extraction)
            ),
            transformed_data AS (
                SELECT 
                    video_id,
                    title,
                    -- Conversion duration_readable en secondes
                    CASE
                        WHEN duration_readable LIKE '%:%:%' THEN
                            (split_part(duration_readable, ':', 1)::int * 3600) +
                            (split_part(duration_readable, ':', 2)::int * 60) +
                            (split_part(duration_readable, ':', 3)::int)
                        ELSE
                            (split_part(duration_readable, ':', 1)::int * 60) +
                            (split_part(duration_readable, ':', 2)::int)
                    END AS duration_seconds,
                    duration_readable AS duration_formatted,
                    COALESCE(like_count,0) AS like_count,
                    COALESCE(view_count,0) AS view_count,
                    COALESCE(comment_count,0) AS comment_count,
                    published_at,
                    channel_handle,
                    CASE 
                        WHEN (
                            CASE
                                WHEN duration_readable LIKE '%:%:%' THEN
                                    (split_part(duration_readable, ':', 1)::int * 3600) +
                                    (split_part(duration_readable, ':', 2)::int * 60) +
                                    (split_part(duration_readable, ':', 3)::int)
                                ELSE
                                    (split_part(duration_readable, ':', 1)::int * 60) +
                                    (split_part(duration_readable, ':', 2)::int)
                            END
                        ) <= 60 THEN 'Short'
                        WHEN (
                            CASE
                                WHEN duration_readable LIKE '%:%:%' THEN
                                    (split_part(duration_readable, ':', 1)::int * 3600) +
                                    (split_part(duration_readable, ':', 2)::int * 60) +
                                    (split_part(duration_readable, ':', 3)::int)
                                ELSE
                                    (split_part(duration_readable, ':', 1)::int * 60) +
                                    (split_part(duration_readable, ':', 2)::int)
                            END
                        ) <= 300 THEN 'Medium'
                        ELSE 'Long'
                    END AS content_type,
                    CASE 
                        WHEN COALESCE(view_count,0) > 0 THEN 
                            ((COALESCE(like_count,0) + COALESCE(comment_count,0))::NUMERIC / COALESCE(view_count,1))
                        ELSE 0::NUMERIC
                    END AS engagement_ratio,
                    CASE 
                        WHEN (
                            CASE
                                WHEN duration_readable LIKE '%:%:%' THEN
                                    (split_part(duration_readable, ':', 1)::int * 3600) +
                                    (split_part(duration_readable, ':', 2)::int * 60) +
                                    (split_part(duration_readable, ':', 3)::int)
                                ELSE
                                    (split_part(duration_readable, ':', 1)::int * 60) +
                                    (split_part(duration_readable, ':', 2)::int)
                            END
                        ) <= 60 THEN TRUE
                        ELSE FALSE
                    END AS is_short,
                    extraction_date
                FROM ranked_data
                WHERE rn = 1
            )
            INSERT INTO core.youtube_videos (
                video_id, title, duration_seconds, duration_formatted,
                like_count, view_count, comment_count, published_at,
                channel_handle, content_type, engagement_ratio, is_short, extraction_date
            )
            SELECT 
                video_id, 
                title, 
                duration_seconds, 
                duration_formatted,
                like_count, 
                view_count, 
                comment_count, 
                published_at,
                channel_handle, 
                content_type, 
                engagement_ratio, 
                is_short, 
                extraction_date
            FROM transformed_data
            ON CONFLICT (video_id) DO UPDATE SET
                title = EXCLUDED.title,
                duration_seconds = EXCLUDED.duration_seconds,
                duration_formatted = EXCLUDED.duration_formatted,
                like_count = EXCLUDED.like_count,
                view_count = EXCLUDED.view_count,
                comment_count = EXCLUDED.comment_count,
                content_type = EXCLUDED.content_type,
                engagement_ratio = EXCLUDED.engagement_ratio,
                is_short = EXCLUDED.is_short,
                updated_at = CURRENT_TIMESTAMP;
            """
            history_sql = """
            INSERT INTO core.youtube_videos_history (video_id, like_count, view_count, comment_count)
            SELECT v.video_id, v.like_count, v.view_count, v.comment_count
            FROM core.youtube_videos v
            WHERE v.extraction_date = (SELECT MAX(extraction_date) FROM staging.youtube_videos_raw)
            AND NOT EXISTS (
                SELECT 1 FROM core.youtube_videos_history h
                WHERE h.video_id = v.video_id AND h.recorded_at::date = CURRENT_DATE
            );
            """
            hook.run(transform_sql)
            hook.run(history_sql)
            records_processed = hook.get_first("SELECT COUNT(*) FROM core.youtube_videos")[0]
            history_records = hook.get_first("SELECT COUNT(*) FROM core.youtube_videos_history WHERE recorded_at::date = CURRENT_DATE")[0]
            logging.info(f"✅ Transformation terminée: {records_processed} vidéos dans core")
            logging.info(f"📈 Historique: {history_records} nouvelles entrées")
            return f"transformed_{records_processed}_videos"
        except Exception as e:
            logging.error(f"Erreur transformation: {str(e)}")
            raise

    # Workflow
    json_data = get_latest_json_file()
    staging_schema = create_staging_schema()
    staging_loaded = load_to_staging(json_data)
    core_schema = create_core_schema()
    transformation = transform_to_core()

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",  # ID de la DAG qualité
        wait_for_completion=False   
    )

    # Dépendances
    staging_loaded >> core_schema >> transformation >> trigger_quality

youtube_update_database_dag = youtube_update_database_dag()
