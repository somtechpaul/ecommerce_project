"""
==========================================================
Integration Tests
Full PEI Pipeline
==========================================================

Tests complete pipeline orchestration.

Ingestion
    ↓
Schema Validation
    ↓
Data Quality
    ↓
Enrichment
    ↓
Gold
"""

from types import SimpleNamespace

from pei_pipeline.main import (
    main
)


def test_full_pipeline(
    spark,
    monkeypatch
):
    """
    Verify complete PEI pipeline orchestration.
    """

    # ======================================================
    # Capture stage execution
    # ======================================================

    executed_stages = []
    captured_run_ids = []
    captured_start_times = []

    # ======================================================
    # Mock pipeline stages
    # ======================================================

    def mock_ingestion_pipeline(
        spark,
        dbutils,
        run_id,
        start_time
    ):

        executed_stages.append(
            "ingestion"
        )

        captured_run_ids.append(
            run_id
        )

        captured_start_times.append(
            start_time
        )

        return {
            "stage": "ingestion",
            "run_id": run_id,
            "status": "SUCCESS",
            "processed_files": 3,
            "failed_files": 0,
            "rows_read": 10,
            "rows_written": 10
        }

    def mock_schema_validation_pipeline(
        spark,
        run_id,
        start_time
    ):

        executed_stages.append(
            "schema_validation"
        )

        captured_run_ids.append(
            run_id
        )

        captured_start_times.append(
            start_time
        )

        return {
            "stage": "schema_validation",
            "run_id": run_id,
            "status": "SUCCESS",
            "processed_tables": 3,
            "failed_tables": 0,
            "rows_read": 10,
            "rows_written": 10
        }

    def mock_data_quality_pipeline(
        spark,
        run_id,
        start_time
    ):

        executed_stages.append(
            "data_quality"
        )

        captured_run_ids.append(
            run_id
        )

        captured_start_times.append(
            start_time
        )

        return {
            "stage": "data_quality",
            "run_id": run_id,
            "status": "SUCCESS",
            "processed_tables": 3,
            "failed_tables": 0,
            "rows_read": 10,
            "valid_rows_written": 10,
            "rejected_rows_written": 0
        }

    def mock_enrichment_pipeline(
        spark,
        run_id,
        start_time
    ):

        executed_stages.append(
            "enrichment"
        )

        captured_run_ids.append(
            run_id
        )

        captured_start_times.append(
            start_time
        )

        return {
            "stage": "enrichment",
            "run_id": run_id,
            "status": "SUCCESS",
            "processed_tables": 2,
            "failed_tables": 0,
            "rows_read": 6,
            "rows_written": 6
        }

    def mock_gold_pipeline(
        spark,
        run_id,
        start_time
    ):

        executed_stages.append(
            "gold"
        )

        captured_run_ids.append(
            run_id
        )

        captured_start_times.append(
            start_time
        )

        return {
            "stage": "gold",
            "run_id": run_id,
            "status": "SUCCESS",
            "processed_tables": 4,
            "failed_tables": 0,
            "rows_written": 12
        }

    # ======================================================
    # Patch functions where main() imports them
    # ======================================================

    monkeypatch.setattr(
        "pei_pipeline.pipelines.ingestion."
        "run_ingestion_pipeline",
        mock_ingestion_pipeline
    )

    monkeypatch.setattr(
        "pei_pipeline.pipelines.schema_validation."
        "run_schema_validation_pipeline",
        mock_schema_validation_pipeline
    )

    monkeypatch.setattr(
        "pei_pipeline.pipelines.data_quality."
        "run_data_quality_pipeline",
        mock_data_quality_pipeline
    )

    monkeypatch.setattr(
        "pei_pipeline.pipelines.enrichment."
        "run_enrichment_pipeline",
        mock_enrichment_pipeline
    )

    monkeypatch.setattr(
        "pei_pipeline.pipelines.gold."
        "run_gold_pipeline",
        mock_gold_pipeline
    )

    # ======================================================
    # Dummy dbutils
    # ======================================================

    dbutils = SimpleNamespace()

    # ======================================================
    # Execute complete pipeline
    # ======================================================

    result = main(
        spark=spark,
        dbutils=dbutils,
        stage="all"
    )

    # ======================================================
    # Validate overall result
    # ======================================================

    assert result["stage"] == "all"

    assert result["status"] == "SUCCESS"

    assert result["run_id"] is not None

    assert result["start_time"] is not None

    assert result["end_time"] is not None

    # ======================================================
    # Validate execution order
    # ======================================================

    assert executed_stages == [
        "ingestion",
        "schema_validation",
        "data_quality",
        "enrichment",
        "gold"
    ]

    # ======================================================
    # All stages must receive the same run ID
    # ======================================================

    assert len(captured_run_ids) == 5

    assert len(
        set(captured_run_ids)
    ) == 1

    assert captured_run_ids[0] == result["run_id"]

    # ======================================================
    # All stages must receive the same start time
    # ======================================================

    assert len(captured_start_times) == 5

    assert len(
        set(captured_start_times)
    ) == 1

    # ======================================================
    # Validate stage results
    # ======================================================

    stage_results = result["stage_results"]

    assert len(stage_results) == 5

    assert (
        stage_results["ingestion"]["status"]
        == "SUCCESS"
    )

    assert (
        stage_results[
            "schema_validation"
        ]["status"]
        == "SUCCESS"
    )

    assert (
        stage_results[
            "data_quality"
        ]["status"]
        == "SUCCESS"
    )

    assert (
        stage_results[
            "enrichment"
        ]["status"]
        == "SUCCESS"
    )

    assert (
        stage_results["gold"]["status"]
        == "SUCCESS"
    )


def test_full_pipeline_stops_when_stage_fails(
    spark,
    monkeypatch
):
    """
    Verify downstream stages do not execute after failure.
    """

    import pytest

    executed_stages = []

    def mock_ingestion_pipeline(
        spark,
        dbutils,
        run_id,
        start_time
    ):

        executed_stages.append(
            "ingestion"
        )

        return {
            "stage": "ingestion",
            "run_id": run_id,
            "status": "SUCCESS"
        }

    def mock_schema_validation_pipeline(
        spark,
        run_id,
        start_time
    ):

        executed_stages.append(
            "schema_validation"
        )

        return {
            "stage": "schema_validation",
            "run_id": run_id,
            "status": "FAILED"
        }

    def unexpected_data_quality_call(
        *args,
        **kwargs
    ):

        executed_stages.append(
            "data_quality"
        )

        raise AssertionError(
            "Data Quality should not execute."
        )

    monkeypatch.setattr(
        "pei_pipeline.pipelines.ingestion."
        "run_ingestion_pipeline",
        mock_ingestion_pipeline
    )

    monkeypatch.setattr(
        "pei_pipeline.pipelines.schema_validation."
        "run_schema_validation_pipeline",
        mock_schema_validation_pipeline
    )

    monkeypatch.setattr(
        "pei_pipeline.pipelines.data_quality."
        "run_data_quality_pipeline",
        unexpected_data_quality_call
    )

    dbutils = SimpleNamespace()

    with pytest.raises(
        RuntimeError,
        match="schema_validation"
    ):

        main(
            spark=spark,
            dbutils=dbutils,
            stage="all"
        )

    assert executed_stages == [
        "ingestion",
        "schema_validation"
    ]