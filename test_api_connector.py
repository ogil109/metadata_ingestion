from metadata_ingestion.config.models import Source
from metadata_ingestion.connectors.api_connector import Api
from metadata_ingestion.dagster_manager import get_dagster_manager


def test_api_connector_jobs():
    """Tests the full Dagster job execution for the Api connector."""
    print("--- Testing API Connector Dagster Jobs ---")
    dagster_manager = get_dagster_manager()

    # Test GET request job
    source_config_get = {
        "name": "test_api_get_job",
        "src_type": "api",
        "connection": {
            "endpoint": "https://jsonplaceholder.typicode.com/posts",
            "type": "GET",
            "args": {"userId": 1},
        },
    }
    source_get = Source(**source_config_get)
    Api(source_get)  # This will trigger the Dagster job

    # Test POST request job
    source_config_post = {
        "name": "test_api_post_job",
        "src_type": "api",
        "connection": {
            "endpoint": "https://jsonplaceholder.typicode.com/posts",
            "type": "POST",
            "body": {"title": "foo", "body": "bar", "userId": 1},
        },
    }
    source_post = Source(**source_config_post)
    Api(source_post)  # This will trigger the Dagster job

    run_status = dagster_manager.get_run_status()

    # Check that two jobs were executed successfully
    assert len(run_status) >= 2, f"Expected at least 2 runs, but found {len(run_status)}"

    get_job_run = next(
        (run for run in run_status if run["job_name"] == "test_api_get_job_job"), None
    )
    post_job_run = next(
        (run for run in run_status if run["job_name"] == "test_api_post_job_job"), None
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
