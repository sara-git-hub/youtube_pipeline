import pytest
from soda.scan import Scan
import os
from airflow.models import Variable

@pytest.fixture
def scan():
    # Charger les variables directement depuis Airflow
    env_vars = {
        "POSTGRES_CONN_HOST": Variable.get("POSTGRES_CONN_HOST"),
        "POSTGRES_CONN_PORT": Variable.get("POSTGRES_CONN_PORT"),
        "ELT_DATABASE_USERNAME": Variable.get("ELT_DATABASE_USERNAME"),
        "ELT_DATABASE_PASSWORD": Variable.get("ELT_DATABASE_PASSWORD"),
        "ELT_DATABASE_NAME": Variable.get("ELT_DATABASE_NAME"),
    }
    
    os.environ.update(env_vars)  # Met à jour les variables pour Soda Scan

    base_path = "/usr/local/airflow/include/quality"
    scan = Scan()
    scan.set_data_source_name("postgres_youtube")
    scan.add_configuration_yaml_file(os.path.join(base_path, "configuration.yml"))
    scan.add_sodacl_yaml_file(os.path.join(base_path, "soda_checks.yml"))
    return scan


def test_soda_scan_runs(scan):
    """Vérifie que le scan s'exécute sans erreur système"""
    exit_code = scan.execute()
    # 0 = succès, 3 = échec de check (mais pas crash technique)
    assert exit_code in (0, 3), f"Le scan a échoué avec exit_code={exit_code}"


def test_soda_checks_pass(scan):
    """Vérifie que tous les checks passent"""
    exit_code = scan.execute()
    assert exit_code == 0, f"❌ Des checks ont échoué, exit_code={exit_code}"


@pytest.mark.parametrize("table", ["staging.youtube_videos_raw", "core.youtube_videos", "core.youtube_videos_history"])
def test_table_checks(scan, table):
    """
    Vérifie que les checks passent pour une table spécifique.
    On relance un scan ciblé sur la table en filtrant le YAML.
    """
    scan_filtered = Scan()
    base_path = "/usr/local/airflow/include/quality"
    scan_filtered.set_data_source_name("postgres_youtube")
    scan_filtered.add_configuration_yaml_file(os.path.join(base_path, "configuration.yml"))
    # Filtre uniquement les checks pour la table en question
    scan_filtered.add_sodacl_yaml_str(f"""
    checks for {table}:
      - row_count > 0
    """)
    exit_code = scan_filtered.execute()
    assert exit_code == 0, f"❌ Des checks ont échoué pour {table}, exit_code={exit_code}"
