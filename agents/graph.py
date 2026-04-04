"""
LangGraph Conversion Graph — wires all agents into a stateful pipeline.

Graph topology:
  extractor → router → [java_fast_path | llm_parser] → validator
                                                            ↓ valid
                                                        optimizer → done
                                                            ↓ invalid (retry)
                                                        repair_agent → validator (loop)
                                                            ↓ failed
                                                        mark_failed → done

State is accumulated across the entire file (all val blocks), producing one
output SQL entry per extracted operation.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langgraph.graph import StateGraph, END

from agents.config import Config, default_config
from agents.extractor_agent import ExtractorAgent, ExtractedOperation
from agents.java_bridge import JavaBridge, is_simple_chain
from agents.llm_parser_agent import LLMParserAgent
from agents.validator_agent import ValidatorAgent
from agents.optimizer_agent import OptimizerAgent
from agents.repair_agent import RepairAgent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State schema
# ---------------------------------------------------------------------------

class SingleOpState(TypedDict):
    """State for processing ONE extracted operation through the pipeline."""
    op: ExtractedOperation              # The operation being processed
    sql_candidate: Optional[str]        # Raw SQL from java/llm
    validated_sql: Optional[str]        # SQL after passing validation
    validation_errors: List[str]        # Current validation errors
    repair_attempt: int                 # Retry counter
    failed: bool                        # True if all repair attempts exhausted
    path_used: str                      # "java" | "llm" — for logging


class ConversionState(TypedDict):
    """Top-level state tracking results for ALL operations in the file."""
    file_path: str
    extracted_ops: List[ExtractedOperation]
    results: List[Dict[str, Any]]       # [{variable, sql, failed, path_used}, ...]
    current_index: int                  # Which op we are processing


# ---------------------------------------------------------------------------
# Node functions
# ---------------------------------------------------------------------------

def extractor_node(state: ConversionState, config: Config) -> ConversionState:
    """Phase 1: Extract all DataFrame operations from the file."""
    agent = ExtractorAgent()
    ops = agent.run(state["file_path"])
    logger.info("Extractor: found %d operations in '%s'", len(ops), state["file_path"])
    return {**state, "extracted_ops": ops, "current_index": 0, "results": []}


def _process_single_op(op: ExtractedOperation, cfg: Config) -> Dict[str, Any]:
    """
    Run a single operation through the full pipeline:
    router → java|llm → validator (→ repair loop) → optimizer
    """
    java_bridge = JavaBridge(cfg)
    llm_parser  = LLMParserAgent(cfg)
    validator   = ValidatorAgent()
    optimizer   = OptimizerAgent()
    repair      = RepairAgent(cfg)

    # --- Route ---
    path = "java" if is_simple_chain(op.chain) else "llm"
    logger.info("Processing '%s' via %s path", op.variable_name, path)

    # --- Generate initial SQL candidate ---
    if path == "java":
        sql = java_bridge.convert(op.chain)
        if sql is None:
            logger.warning(
                "'%s': Java path returned None — falling back to LLM path",
                op.variable_name,
            )
            path = "llm"
            sql = llm_parser.convert(op.chain, op.variable_name)
    else:
        sql = llm_parser.convert(op.chain, op.variable_name)

    if sql is None:
        return {
            "variable": op.variable_name,
            "sql": None,
            "failed": True,
            "path_used": path,
            "errors": ["Both Java and LLM paths returned no SQL."],
        }

    # --- Validate + Repair loop ---
    attempt = 0
    while True:
        result = validator.validate(sql)
        if result.is_valid:
            break

        attempt += 1
        if attempt > cfg.max_repair_retries:
            logger.error(
                "'%s': validation failed after %d repair attempts. Errors: %s",
                op.variable_name, cfg.max_repair_retries, result.errors,
            )
            return {
                "variable": op.variable_name,
                "sql": sql,  # best effort
                "failed": True,
                "path_used": path,
                "errors": result.errors,
            }

        logger.warning(
            "'%s': validation failed (attempt %d/%d). Errors: %s",
            op.variable_name, attempt, cfg.max_repair_retries, result.errors,
        )
        repaired = repair.repair(sql, result.errors, attempt=attempt)
        if repaired is None:
            return {
                "variable": op.variable_name,
                "sql": sql,
                "failed": True,
                "path_used": path,
                "errors": result.errors,
            }
        sql = repaired

    # --- Optimize ---
    final_sql = optimizer.optimize(sql, variable_name=op.variable_name)

    return {
        "variable": op.variable_name,
        "sql": final_sql,
        "failed": False,
        "path_used": path,
        "errors": [],
    }


def processing_node(state: ConversionState, config: Config) -> ConversionState:
    """Phase 2-N: Process all extracted operations sequentially."""
    results = []
    for op in state["extracted_ops"]:
        result = _process_single_op(op, config)
        results.append(result)
    return {**state, "results": results}


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_graph(config: Config = default_config) -> Any:
    """
    Build and compile the LangGraph StateGraph.

    Returns a compiled graph that can be invoked with:
        graph.invoke({"file_path": "path/to/file.scala", ...})
    """
    builder = StateGraph(ConversionState)

    # Wrap nodes with config via closure
    def _extractor(state):
        return extractor_node(state, config)

    def _processing(state):
        return processing_node(state, config)

    builder.add_node("extractor", _extractor)
    builder.add_node("processing", _processing)

    builder.set_entry_point("extractor")
    builder.add_edge("extractor", "processing")
    builder.add_edge("processing", END)

    return builder.compile()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class ConversionPipeline:
    """
    High-level interface for the full conversion pipeline.

    Usage:
        pipeline = ConversionPipeline(config)
        results = pipeline.run("path/to/SparkFile.scala")
        for r in results:
            print(r["variable"], r["sql"])
    """

    def __init__(self, config: Config = default_config):
        self.config = config
        self._graph = build_graph(config)

    def run(self, file_path: str | Path) -> List[Dict[str, Any]]:
        """
        Convert an entire Spark file and return a list of result dicts.

        Each dict has keys:
            variable (str): Scala val name
            sql      (str | None): Generated SQL (None if completely failed)
            failed   (bool): True if validation/repair could not produce valid SQL
            path_used(str): "java" or "llm"
            errors   (list[str]): Validation error messages (empty if succeeded)
        """
        initial_state: ConversionState = {
            "file_path": str(file_path),
            "extracted_ops": [],
            "results": [],
            "current_index": 0,
        }
        final_state = self._graph.invoke(initial_state)
        return final_state.get("results", [])
