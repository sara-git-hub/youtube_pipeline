from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.exceptions import AirflowException
from include.youtube.extractor import YouTubeExtractor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

@dag(
    dag_id='youtube_data_extraction',
    default_args=default_args,
    description='Extraction des données YouTube MrBeast et sauvegarde en JSON',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['youtube', 'data-extraction', 'json'],
)
def youtube_data_extraction_dag():

    @task(
        retries=2,
        execution_timeout=timedelta(minutes=30),
        task_id='extract_youtube_data'
    )
    def extract_youtube_data():
        """Task d'extraction des données YouTube avec votre classe"""
        try:
            # Utilisation de la classe YouTubeExtractor
            extractor = YouTubeExtractor()
            
            # Configuration via Variables Airflow
            max_results = int(Variable.get("youtube_max_results", 200))
            
            logging.info(f"Extraction pour {extractor.channel_handle}")
            logging.info(f"Max results: {max_results}")
            
            # Appel de la méthode get_channel_videos
            videos_data = extractor.get_channel_videos(max_results=max_results)
            
            logging.info(f"✅ Extraction réussie: {len(videos_data)} vidéos")
            
            # Retourne les données pour la tâche suivante
            return videos_data
            
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction: {str(e)}")
            raise AirflowException(f"Extraction failed: {str(e)}")

    @task(
        execution_timeout=timedelta(minutes=10),
        task_id='save_json_data'
    )
    def save_json_data(videos_data):
        """Task de sauvegarde utilisant la fonction save_to_json"""
        try:
            if not videos_data:
                raise AirflowException("Aucune donnée à sauvegarder")
            
            # Utilisation de la classe YoutubeExtractor et méthode save_to_json
            extractor = YouTubeExtractor()
            
            # Chemin configurable dans Airflow Variables
            save_path = "dags/data"
            
            # Appel de VOTRE fonction save_to_json
            filename = extractor.save_to_json(videos_data, save_path)
            
            logging.info(f"💾 Données sauvegardées dans: {filename}")
            logging.info(f"📊 Nombre de vidéos: {len(videos_data)}")
            
            return filename
            
        except Exception as e:
            logging.error(f"Erreur lors de la sauvegarde: {str(e)}")
            raise AirflowException(f"Sauvegarde failed: {str(e)}")

    @task(
        task_id='log_extraction_summary'
    )
    def log_extraction_summary(json_filename):
        """Task de log du résumé"""
        logging.info(f"✅ Extraction terminée avec succès")
        logging.info(f"💾 Fichier JSON généré: {json_filename}")
        return f"Fichier sauvegardé: {json_filename}"

    # Workflow
    videos_data = extract_youtube_data()
    json_filename = save_json_data(videos_data)
    log_summary = log_extraction_summary(json_filename)

# Instanciation du DAG
youtube_data_extraction_dag = youtube_data_extraction_dag()