"""
==========================================================
PEI Pipeline Entry Point
==========================================================

Supported stages
----------------
- ingestion
- schema_validation
- data_quality
- enrichment
- gold
- all
"""

from uuid import uuid4
from datetime import datetime


def main(
    spark,
    dbutils,
    stage
):
    """
    Execute a PEI pipeline stage.

    Parameters
    ----------
    spark
        Active Spark session.

    dbutils
        Databricks utilities object.

    stage
        Pipeline stage to execute.

    Returns
    -------
    dict
        Pipeline execution summary.
    """

    run_id = str(uuid4())
    start_time = datetime.now()

    normalized_stage = stage.strip().lower()

    # ======================================================
    # Ingestion
    # ======================================================

    if normalized_stage == "ingestion":

        from pei_pipeline.pipelines.ingestion import (
            run_ingestion_pipeline
        )

        return run_ingestion_pipeline(
            spark=spark,
            dbutils=dbutils,
            run_id=run_id,
            start_time=start_time
        )

    # ======================================================
    # Schema Validation
    # ======================================================

    if normalized_stage == "schema_validation":

        from pei_pipeline.pipelines.schema_validation import (
            run_schema_validation_pipeline
        )

        return run_schema_validation_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

    # ======================================================
    # Data Quality
    # ======================================================

    if normalized_stage == "data_quality":

        from pei_pipeline.pipelines.data_quality import (
            run_data_quality_pipeline
        )

        return run_data_quality_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

    # ======================================================
    # Enrichment
    # ======================================================

    if normalized_stage == "enrichment":

        from pei_pipeline.pipelines.enrichment import (
            run_enrichment_pipeline
        )

        return run_enrichment_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

    # ======================================================
    # Gold
    # ======================================================

    if normalized_stage == "gold":

        from pei_pipeline.pipelines.gold import (
            run_gold_pipeline
        )

        return run_gold_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

    # ======================================================
    # Complete Pipeline
    # ======================================================

    if normalized_stage == "all":

        from pei_pipeline.pipelines.ingestion import (
            run_ingestion_pipeline
        )

        from pei_pipeline.pipelines.schema_validation import (
            run_schema_validation_pipeline
        )

        from pei_pipeline.pipelines.data_quality import (
            run_data_quality_pipeline
        )

        from pei_pipeline.pipelines.enrichment import (
            run_enrichment_pipeline
        )

        from pei_pipeline.pipelines.gold import (
            run_gold_pipeline
        )

        stage_results = {}

        print()
        print("=" * 70)
        print("PEI FULL PIPELINE STARTED")
        print("=" * 70)
        print(f"Run ID     : {run_id}")
        print(f"Start Time : {start_time}")
        print("=" * 70)

        # --------------------------------------------------
        # Stage 1: Ingestion
        # --------------------------------------------------

        stage_results["ingestion"] = run_ingestion_pipeline(
            spark=spark,
            dbutils=dbutils,
            run_id=run_id,
            start_time=start_time
        )

        _validate_stage_result(
            stage_name="ingestion",
            result=stage_results["ingestion"]
        )

        # --------------------------------------------------
        # Stage 2: Schema Validation
        # --------------------------------------------------

        stage_results[
            "schema_validation"
        ] = run_schema_validation_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

        _validate_stage_result(
            stage_name="schema_validation",
            result=stage_results["schema_validation"]
        )

        # --------------------------------------------------
        # Stage 3: Data Quality
        # --------------------------------------------------

        stage_results[
            "data_quality"
        ] = run_data_quality_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

        _validate_stage_result(
            stage_name="data_quality",
            result=stage_results["data_quality"]
        )

        # --------------------------------------------------
        # Stage 4: Enrichment
        # --------------------------------------------------

        stage_results[
            "enrichment"
        ] = run_enrichment_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

        _validate_stage_result(
            stage_name="enrichment",
            result=stage_results["enrichment"]
        )

        # --------------------------------------------------
        # Stage 5: Gold
        # --------------------------------------------------

        stage_results["gold"] = run_gold_pipeline(
            spark=spark,
            run_id=run_id,
            start_time=start_time
        )

        _validate_stage_result(
            stage_name="gold",
            result=stage_results["gold"]
        )

        end_time = datetime.now()

        print()
        print("=" * 70)
        print("PEI FULL PIPELINE COMPLETED")
        print("=" * 70)
        print(f"Run ID   : {run_id}")
        print("Status   : SUCCESS")
        print(f"End Time : {end_time}")
        print("=" * 70)

        return {
            "stage": "all",
            "run_id": run_id,
            "status": "SUCCESS",
            "stage_results": stage_results,
            "start_time": start_time,
            "end_time": end_time
        }

    raise ValueError(
        f"Unsupported pipeline stage: {stage}. "
        "Supported stages are ingestion, schema_validation, "
        "data_quality, enrichment, gold and all."
    )


def _validate_stage_result(
    stage_name,
    result
):
    """
    Stop full-pipeline execution when a stage fails.
    """

    if result is None:

        raise RuntimeError(
            f"Pipeline stage '{stage_name}' returned no result."
        )

    stage_status = result.get(
        "status",
        "FAILED"
    )

    if stage_status != "SUCCESS":

        raise RuntimeError(
            f"Pipeline stage '{stage_name}' completed with "
            f"status '{stage_status}'."
        )