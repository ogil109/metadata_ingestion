from pathlib import Path

from metadata_ingestion.config.models import Pipeline
from metadata_ingestion.connectors.api_connector import Api
from metadata_ingestion.dagster_manager import get_dagster_manager


def test_api_connector_jobs():
    """Tests the full Dagster job execution for the Api connector."""
    print("--- Testing API Connector Dagster Jobs ---")
    dagster_manager = get_dagster_manager()

    # Load pipeline configuration and get the sources
    pipeline = Pipeline.from_json(Path("pipelines/placeholder_api.json"))
    source_get = pipeline.sources[0]
    source_post = pipeline.sources[1]

    # Test GET request job
    Api(source_get)  # This will trigger the Dagster job

    # Test POST request job
    Api(source_post)  # This will trigger the Dagster job

    run_status = dagster_manager.get_run_status()

    # Check that two jobs were executed successfully
    assert len(run_status) >= 2, f"Expected at least 2 runs, but found {len(run_status)}"

    get_job_run = next(
        (run for run in run_status if run["job_name"] == "placeholder_API_GET_job"), None
    )
    post_job_run = next(
        (run for run in run_status if run["job_name"] == "placeholder_API_POST_job"), None
    )

    assert get_job_run, "GET job run not found"
    assert get_job_run["status"] == "SUCCESS", f"GET job failed with status {get_job_run['status']}"
    print("GET job test PASSED")

    assert post_job_run, "POST job run not found"
    assert post_job_run["status"] == "SUCCESS", (
        f"POST job failed with status {post_job_run['status']}"
    )
    print("POST job test PASSED")


if __name__ == "__main__":
    test_api_connector_jobs()
