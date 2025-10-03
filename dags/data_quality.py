import logging
import subprocess
import os
from airflow.decorators import dag, task
from datetime import datetime
from airflow.models import Variable

logger = logging.getLogger(__name__)

# Configuration Soda
SODA_PATH = "/usr/local/airflow/include/quality"
DATASOURCE = "postgres_youtube"

@task
def run_soda_quality_checks():
    """
    Exécute les checks de qualité Soda sur toutes les tables
    (staging et core en une seule fois)
    """
    try:
        env = os.environ.copy()
        env['POSTGRES_CONN_HOST'] = Variable.get("POSTGRES_CONN_HOST")
        env['POSTGRES_CONN_PORT'] = Variable.get("POSTGRES_CONN_PORT")
        env['ELT_DATABASE_USERNAME'] = Variable.get("ELT_DATABASE_USERNAME")
        env['ELT_DATABASE_PASSWORD'] = Variable.get("ELT_DATABASE_PASSWORD")
        env['ELT_DATABASE_NAME'] = Variable.get("ELT_DATABASE_NAME")

        cmd = [
            'soda', 'scan',
            '-d', DATASOURCE,
            '-c', f'{SODA_PATH}/configuration.yml',
            f'{SODA_PATH}/soda_checks.yml'
        ]

        logger.info(f"Executing Soda command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            cwd='/usr/local/airflow'
        )

        if result.stdout:
            logger.info(f"Soda output:\n{result.stdout}")
        if result.stderr:
            logger.error(f"Soda errors:\n{result.stderr}")

        if result.returncode != 0:
            raise Exception(f"Soda scan failed with return code {result.returncode}")

        logger.info("✅ Soda validation successful for all tables")
        return True

    except Exception as e:
        logger.error(f"❌ Error running Soda validation: {e}")
        raise e


# === DAG avec décorateur ===
@dag(
    dag_id="data_quality",
    start_date=datetime(2025, 9, 30),
    schedule=None,
    catchup=False,
    tags=["elt", "soda", "quality"],
    description="Validation de la qualité des données avec Soda Core"
)
def data_quality_dag():
    """
    DAG de validation de la qualité des données
    Valide les tables staging et core en une seule exécution
    """
    run_soda_quality_checks()


# Instanciation de la DAG
dag = data_quality_dag()