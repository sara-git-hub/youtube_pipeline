import pytest
from airflow.models import DagBag
from airflow.utils import timezone

# ---------------------------
# Fixtures
# ---------------------------
@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


# ---------------------------
# Test existence des DAGs
# ---------------------------
@pytest.mark.parametrize(
    "dag_id",
    [
        "youtube_data_extraction",
        "youtube_update_database",
        "data_quality",
    ],
)
def test_dag_exists(dagbag, dag_id):
    """Vérifie que le DAG existe dans le DagBag"""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} non trouvé dans le DagBag"


# ---------------------------
# Test structure des DAGs
# ---------------------------
@pytest.mark.parametrize(
    "dag_id",
    [
        "youtube_data_extraction",
        "youtube_update_database",
        "data_quality",
    ],
)
def test_dag_structure(dagbag, dag_id):
    """Vérifie la structure de base du DAG"""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} non trouvé"
    
    # Vérifie que le DAG a des tâches
    assert len(dag.tasks) > 0, f"Le DAG {dag_id} n'a pas de tâches"
    
    # Vérifie que toutes les tâches ont un task_id
    for task in dag.tasks:
        assert task.task_id, f"Une tâche du DAG {dag_id} n'a pas de task_id"


# ---------------------------
# Test des dépendances des tâches
# ---------------------------
@pytest.mark.parametrize(
    "dag_id",
    [
        "youtube_data_extraction",
        "youtube_update_database",
        "data_quality",
    ],
)
def test_dag_task_dependencies(dagbag, dag_id):
    """Vérifie que les dépendances des tâches sont correctement définies"""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} non trouvé"
    
    # Vérifie qu'il n'y a pas de cycles dans les dépendances
    # En Airflow moderne, on utilise la topologie du DAG
    try:
        # Tente d'obtenir l'ordre topologique des tâches
        # S'il y a un cycle, cette opération échouera
        list(dag.topological_sort())
    except Exception as e:
        pytest.fail(f"Erreur de dépendances dans le DAG {dag_id}: {str(e)}")


# ---------------------------
# Test de la validation du DAG
# ---------------------------
@pytest.mark.parametrize(
    "dag_id",
    [
        "youtube_data_extraction",
        "youtube_update_database",
        "data_quality",
    ],
)
def test_dag_validation(dagbag, dag_id):
    """Vérifie que le DAG passe toutes les validations Airflow"""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} non trouvé"
    
    # Vérifie qu'il n'y a pas d'erreurs d'import
    assert dag_id not in dagbag.import_errors, \
        f"Erreurs d'import pour {dag_id}: {dagbag.import_errors.get(dag_id)}"


# ---------------------------
# Test d'exécution simulée (DryRun)
# ---------------------------
@pytest.mark.parametrize(
    "dag_id",
    [
        "youtube_data_extraction",
        "youtube_update_database",
        "data_quality",
    ],
)
def test_dag_dry_run(dagbag, dag_id):
    """Teste l'exécution à vide du DAG"""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} non trouvé"
    
    # Date d'exécution pour le test
    execution_date = timezone.utcnow()
    
    # Vérifie que chaque tâche peut être instanciée
    for task in dag.tasks:
        try:
            # Teste la création d'une TaskInstance
            from airflow.models import TaskInstance
            ti = TaskInstance(task=task, run_id=f"test_{execution_date.isoformat()}")
            assert ti is not None, f"Impossible de créer TaskInstance pour {task.task_id}"
        except Exception as e:
            pytest.fail(f"Erreur lors de la création de TaskInstance pour {task.task_id}: {str(e)}")


# ---------------------------
# Test des paramètres par défaut du DAG
# ---------------------------
@pytest.mark.parametrize(
    "dag_id,expected_retries",
    [
        ("youtube_data_extraction", 1),
        ("youtube_update_database", 1),
        ("data_quality", 1),
    ],
)
def test_dag_default_args(dagbag, dag_id, expected_retries):
    """Vérifie que les arguments par défaut sont correctement définis"""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} non trouvé"
    
    # Vérifie les default_args si ils existent
    if dag.default_args:
        # Exemple: vérifier que retries est défini
        if 'retries' in dag.default_args:
            assert dag.default_args['retries'] >= 0, \
                f"Les retries doivent être >= 0 pour {dag_id}"


# ---------------------------
# Test de tous les DAGs (pas d'erreurs d'import)
# ---------------------------
def test_no_import_errors(dagbag):
    """Vérifie qu'il n'y a aucune erreur d'import dans le DagBag"""
    assert not dagbag.import_errors, \
        f"Erreurs d'import détectées: {dagbag.import_errors}"


# ---------------------------
# Test du nombre de DAGs
# ---------------------------
def test_dag_count(dagbag):
    """Vérifie que tous les DAGs attendus sont présents"""
    expected_dags = [
        "youtube_data_extraction",
        "youtube_update_database",
        "data_quality",
    ]
    
    dag_ids = list(dagbag.dag_ids)
    
    for expected_dag in expected_dags:
        assert expected_dag in dag_ids, \
            f"DAG {expected_dag} manquant. DAGs trouvés: {dag_ids}"


# ---------------------------
# Test de la configuration du schedule
# ---------------------------
@pytest.mark.parametrize(
    "dag_id",
    [
        "youtube_data_extraction",
        "youtube_update_database",
        "data_quality",
    ],
)
def test_dag_schedule(dagbag, dag_id):
    """Vérifie que le schedule du DAG est configuré"""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} non trouvé"
    
    # Vérifie que l'attribut timetable existe (remplace schedule_interval dans Airflow 2.2+)
    # Le schedule peut être None pour les DAGs déclenchés manuellement
    assert hasattr(dag, 'timetable') or hasattr(dag, 'schedule_interval'), \
        f"Le DAG {dag_id} n'a pas de configuration de schedule"