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

from datetime import datetime
from uuid import uuid4


# ==========================================================
# Main Entry Point
# ==========================================================

def main(
    spark,
    dbutils,
    stage,
    pipeline_run_id=None,
    attempt_id=None,
    attempt_number=1
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

    pipeline_run_id
        Identifier shared by all stages belonging to the
        same Databricks Workflow run.

    attempt_id
        Identifier for the current task retry or repair.

    attempt_number
        Current task execution count, including retries
        and repairs.

    Returns
    -------
    dict
        Pipeline execution summary.
    """

    # ======================================================
    # Validate Stage
    # ======================================================

    if not isinstance(stage, str) or not stage.strip():

        raise ValueError(
            "stage must be a non-empty string."
        )

    normalized_stage = stage.strip().lower()

    # ======================================================
    # Resolve Pipeline Run ID
    # ======================================================

    pipeline_run_id = _resolve_identifier(
        value=pipeline_run_id,
        fallback_prefix="interactive_run"
    )

    # ======================================================
    # Resolve Attempt ID
    # ======================================================

    attempt_id = _resolve_identifier(
        value=attempt_id,
        fallback_prefix="interactive_attempt"
    )

    # ======================================================
    # Resolve Attempt Number
    # ======================================================

    attempt_number = _resolve_attempt_number(
        attempt_number
    )

    pipeline_start_time = datetime.now()

    print()
    print("=" * 70)
    print("PEI PIPELINE EXECUTION CONTEXT")
    print("=" * 70)
    print(f"Requested Stage : {normalized_stage}")
    print(f"Pipeline Run ID : {pipeline_run_id}")
    print(f"Attempt ID      : {attempt_id}")
    print(f"Attempt Number  : {attempt_number}")
    print(f"Start Time      : {pipeline_start_time}")
    print("=" * 70)

    # ======================================================
    # Ingestion
    # ======================================================

    if normalized_stage == "ingestion":

        from pei_pipeline.pipelines.ingestion import (
            run_ingestion_pipeline
        )

        result = run_ingestion_pipeline(
            spark=spark,
            dbutils=dbutils,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="ingestion",
            result=result
        )

        return _add_execution_context(
            result=result,
            pipeline_run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number
        )

    # ======================================================
    # Schema Validation
    # ======================================================

    if normalized_stage == "schema_validation":

        from pei_pipeline.pipelines.schema_validation import (
            run_schema_validation_pipeline
        )

        result = run_schema_validation_pipeline(
            spark=spark,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="schema_validation",
            result=result
        )

        return _add_execution_context(
            result=result,
            pipeline_run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number
        )

    # ======================================================
    # Data Quality
    # ======================================================

    if normalized_stage == "data_quality":

        from pei_pipeline.pipelines.data_quality import (
            run_data_quality_pipeline
        )

        result = run_data_quality_pipeline(
            spark=spark,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="data_quality",
            result=result
        )

        return _add_execution_context(
            result=result,
            pipeline_run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number
        )

    # ======================================================
    # Enrichment
    # ======================================================

    if normalized_stage == "enrichment":

        from pei_pipeline.pipelines.enrichment import (
            run_enrichment_pipeline
        )

        result = run_enrichment_pipeline(
            spark=spark,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="enrichment",
            result=result
        )

        return _add_execution_context(
            result=result,
            pipeline_run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number
        )

    # ======================================================
    # Gold
    # ======================================================

    if normalized_stage == "gold":

        from pei_pipeline.pipelines.gold import (
            run_gold_pipeline
        )

        result = run_gold_pipeline(
            spark=spark,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="gold",
            result=result
        )

        return _add_execution_context(
            result=result,
            pipeline_run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number
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
        print(f"Pipeline Run ID : {pipeline_run_id}")
        print(f"Attempt ID      : {attempt_id}")
        print(f"Attempt Number  : {attempt_number}")
        print(f"Start Time      : {pipeline_start_time}")
        print("=" * 70)

        # --------------------------------------------------
        # Stage 1: Ingestion
        # --------------------------------------------------

        ingestion_result = run_ingestion_pipeline(
            spark=spark,
            dbutils=dbutils,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="ingestion",
            result=ingestion_result
        )

        stage_results["ingestion"] = (
            _add_execution_context(
                result=ingestion_result,
                pipeline_run_id=pipeline_run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number
            )
        )

        # --------------------------------------------------
        # Stage 2: Schema Validation
        # --------------------------------------------------

        schema_validation_result = (
            run_schema_validation_pipeline(
                spark=spark,
                run_id=pipeline_run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                start_time=datetime.now()
            )
        )

        _validate_stage_result(
            stage_name="schema_validation",
            result=schema_validation_result
        )

        stage_results["schema_validation"] = (
            _add_execution_context(
                result=schema_validation_result,
                pipeline_run_id=pipeline_run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number
            )
        )

        # --------------------------------------------------
        # Stage 3: Data Quality
        # --------------------------------------------------

        data_quality_result = run_data_quality_pipeline(
            spark=spark,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="data_quality",
            result=data_quality_result
        )

        stage_results["data_quality"] = (
            _add_execution_context(
                result=data_quality_result,
                pipeline_run_id=pipeline_run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number
            )
        )

        # --------------------------------------------------
        # Stage 4: Enrichment
        # --------------------------------------------------

        enrichment_result = run_enrichment_pipeline(
            spark=spark,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="enrichment",
            result=enrichment_result
        )

        stage_results["enrichment"] = (
            _add_execution_context(
                result=enrichment_result,
                pipeline_run_id=pipeline_run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number
            )
        )

        # --------------------------------------------------
        # Stage 5: Gold
        # --------------------------------------------------

        gold_result = run_gold_pipeline(
            spark=spark,
            run_id=pipeline_run_id,
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            start_time=datetime.now()
        )

        _validate_stage_result(
            stage_name="gold",
            result=gold_result
        )

        stage_results["gold"] = (
            _add_execution_context(
                result=gold_result,
                pipeline_run_id=pipeline_run_id,
                attempt_id=attempt_id,
                attempt_number=attempt_number
            )
        )

        pipeline_end_time = datetime.now()

        print()
        print("=" * 70)
        print("PEI FULL PIPELINE COMPLETED")
        print("=" * 70)
        print(f"Pipeline Run ID : {pipeline_run_id}")
        print(f"Attempt ID      : {attempt_id}")
        print(f"Attempt Number  : {attempt_number}")
        print("Status          : SUCCESS")
        print(f"End Time        : {pipeline_end_time}")
        print("=" * 70)

        return {
            "stage": "all",
            "run_id": pipeline_run_id,
            "attempt_id": attempt_id,
            "attempt_number": attempt_number,
            "status": "SUCCESS",
            "stage_results": stage_results,
            "start_time": pipeline_start_time,
            "end_time": pipeline_end_time
        }

    # ======================================================
    # Unsupported Stage
    # ======================================================

    raise ValueError(
        f"Unsupported pipeline stage: {stage}. "
        "Supported stages are ingestion, schema_validation, "
        "data_quality, enrichment, gold and all."
    )


# ==========================================================
# Validate Stage Result
# ==========================================================

def _validate_stage_result(
    stage_name,
    result
):
    """
    Stop execution when a pipeline stage does not complete
    successfully.

    FAILED and PARTIAL_SUCCESS both cause the Databricks
    task to fail so the orchestrator can retry or repair it.
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


# ==========================================================
# Add Execution Context to Result
# ==========================================================

def _add_execution_context(
    result,
    pipeline_run_id,
    attempt_id,
    attempt_number
):
    """
    Add common workflow execution identifiers to a stage
    result without modifying the original dictionary.
    """

    enriched_result = dict(result)

    enriched_result["run_id"] = pipeline_run_id
    enriched_result["attempt_id"] = attempt_id
    enriched_result["attempt_number"] = attempt_number

    return enriched_result


# ==========================================================
# Resolve Identifier
# ==========================================================

def _resolve_identifier(
    value,
    fallback_prefix
):
    """
    Use a Databricks-supplied identifier when available.
    Generate an interactive identifier otherwise.
    """

    if value is not None:

        normalized_value = str(value).strip()

        if normalized_value:
            return normalized_value

    return f"{fallback_prefix}_{uuid4()}"


# ==========================================================
# Resolve Attempt Number
# ==========================================================

def _resolve_attempt_number(
    attempt_number
):
    """
    Validate and return the current attempt number.
    """

    if attempt_number is None or str(attempt_number).strip() == "":

        return 1

    try:

        resolved_attempt_number = int(
            attempt_number
        )

    except (TypeError, ValueError) as exception:

        raise ValueError(
            "attempt_number must be an integer."
        ) from exception

    if resolved_attempt_number < 1:

        raise ValueError(
            "attempt_number must be greater than or equal to 1."
        )

    return resolved_attempt_number