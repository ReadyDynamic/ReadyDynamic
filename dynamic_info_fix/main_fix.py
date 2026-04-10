from __future__ import annotations

import argparse
import pickle
import time
from pathlib import Path
from typing import Iterable

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]

PREFIX_CONFIG = {
    "Hadoop": ["attempt_", "task_"],
    "Spark": ["rdd_", "broadcast_", "mesos-slave-", "mesos-master-"],
    "BGL": [],
}

SPECIAL_VAR_CONFIG = {
    "BGL": ["X-", "X+", "Y-", "Y+", "Z-", "Z+", "x+", "x-", "y+", "y-", "z+", "z-"],
}


def csv_list(value: str) -> list[str]:
    if not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def resolve_path(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def default_split_prefixes(dataset: str) -> list[str]:
    return list(PREFIX_CONFIG.get(dataset, []))


def default_special_vars(dataset: str) -> list[str]:
    return list(SPECIAL_VAR_CONFIG.get(dataset, []))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract and normalize dynamic variable information from structured log parsing results.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    io_group = parser.add_argument_group("Input / Output")
    io_group.add_argument(
        "--dataset",
        required=True,
        help="Dataset name, used for default input and output file naming.",
    )
    io_group.add_argument(
        "--structured-file",
        help="Path to the structured log CSV file. If omitted, a dataset-based default path is used.",
    )
    io_group.add_argument(
        "--template-file",
        help="Path to the template CSV file. If omitted, a dataset-based default path is used.",
    )
    io_group.add_argument(
        "--output-root",
        default="Output/dynamic_extract",
        help="Root directory for generated CSV and pickle artifacts.",
    )

    processing_group = parser.add_argument_group("Processing")
    processing_group.add_argument(
        "--sample-ratio",
        type=float,
        default=0.05,
        help="Sampling ratio passed to dynamic format extraction.",
    )
    processing_group.add_argument(
        "--split-prefixes",
        type=csv_list,
        help="Comma-separated prefixes used during format extraction. Defaults are dataset-specific.",
    )
    processing_group.add_argument(
        "--special-vars",
        type=csv_list,
        help="Comma-separated special dynamic value markers used during preprocessing. Defaults are dataset-specific.",
    )
    processing_group.add_argument(
        "--skip-format-extraction",
        action="store_true",
        help="Only generate grouped dynamic-variable data and skip the LLM-based format extraction stage.",
    )

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not 0 < args.sample_ratio <= 1:
        raise ValueError("--sample-ratio must be within (0, 1].")


def build_paths(args: argparse.Namespace) -> dict[str, Path]:
    dataset = args.dataset
    structured_file = (
        resolve_path(args.structured_file)
        if args.structured_file
        else REPO_ROOT / "data" / "Drain_result" / f"{dataset}_full.log_structured.csv"
    )
    template_file = (
        resolve_path(args.template_file)
        if args.template_file
        else REPO_ROOT / "data" / "Drain_result" / f"{dataset}_full.log_templates.csv"
    )

    output_root = resolve_path(args.output_root)
    return {
        "structured_file": structured_file,
        "template_file": template_file,
        "output_structured": output_root / "structured_file" / f"{dataset}_dynamic_structured.csv",
        "output_preprocessed": output_root / "preprocessed" / f"{dataset}_dynamic_preprocessed.csv",
        "output_grouped": output_root / "grouped" / f"{dataset}_dynamic_grouped.csv",
        "output_format": output_root / "format" / f"{dataset}_dynamic_format.csv",
        "pickle_grouped": output_root / "pickle" / f"{dataset}.pkl",
        "pickle_format": output_root / "format" / f"{dataset}_format.pkl",
    }


def print_stage_result(stage_name: str, start_time: float) -> None:
    elapsed = time.time() - start_time
    print(f"[done] {stage_name} ({elapsed:.2f}s)")


def print_runtime_summary(paths: dict[str, Path], split_prefixes: Iterable[str], special_vars: Iterable[str]) -> None:
    print("Dynamic info extraction configuration:")
    print(f"  structured file : {paths['structured_file']}")
    print(f"  template file   : {paths['template_file']}")
    print(f"  grouped pickle  : {paths['pickle_grouped']}")
    print(f"  format pickle   : {paths['pickle_format']}")
    print(f"  split prefixes  : {list(split_prefixes)}")
    print(f"  special vars    : {list(special_vars)}")


def run(args: argparse.Namespace) -> None:
    validate_args(args)
    paths = build_paths(args)
    from dynamic_info_combine import (
        generate_combined_dynamic_structured_df,
        generate_combined_dynamic_template,
        get_template_with_dynamic,
    )
    from dynamic_info_fix import (
        dynamic_based_df_generate_v2,
        find_same_value_group,
        preprocess,
        update_dynamic_based_df_with_group_info,
        value_format_generate_with_group,
    )

    for key in ("structured_file", "template_file"):
        if not paths[key].exists():
            raise FileNotFoundError(f"Required input file not found: {paths[key]}")

    split_prefixes = args.split_prefixes if args.split_prefixes is not None else default_split_prefixes(args.dataset)
    special_vars = args.special_vars if args.special_vars is not None else default_special_vars(args.dataset)

    for key, path in paths.items():
        if key not in {"structured_file", "template_file"}:
            ensure_parent(path)

    print_runtime_summary(paths, split_prefixes, special_vars)
    pipeline_start = time.time()

    template_df = pd.read_csv(paths["template_file"])
    structured_df = pd.read_csv(paths["structured_file"])

    template_list = template_df["EventTemplate"].tolist()
    new_old_template_dict, template_dynamic_dict = generate_combined_dynamic_template(template_list)
    new_structured_df, new_old_template_dict, template_dynamic_dict = generate_combined_dynamic_structured_df(
        structured_df,
        new_old_template_dict,
        template_dynamic_dict,
    )
    new_structured_df.to_csv(paths["output_structured"], index=False)
    print_stage_result("original data fix", pipeline_start)

    template_with_dynamic = get_template_with_dynamic(template_list)
    new_template_list_with_dynamic = [new_old_template_dict[template] for template in template_with_dynamic]
    dynamic_based_df = dynamic_based_df_generate_v2(
        new_structured_df,
        new_template_list_with_dynamic,
        extract_from_file=False,
    )
    print_stage_result("dynamic-based data generation", pipeline_start)

    dynamic_based_updated_df = preprocess(dynamic_based_df, special_vars, template_dynamic_dict)
    dynamic_based_updated_df["DynamicID"] = dynamic_based_updated_df.index
    dynamic_based_updated_df[
        ["DynamicID", "Template", "ParameterIndex", "Tokens", "TokenFormat", "ComponentFormat", "ValueList", "ValueType"]
    ].to_csv(paths["output_preprocessed"], index=False)
    print_stage_result("dynamic-based data preprocess", pipeline_start)

    same_value_group = find_same_value_group(dynamic_based_updated_df)
    dynamic_based_updated_df = update_dynamic_based_df_with_group_info(dynamic_based_updated_df, same_value_group)
    dynamic_based_updated_df.to_csv(paths["output_grouped"], index=False)
    with open(paths["pickle_grouped"], "wb") as handle:
        pickle.dump(dynamic_based_updated_df, handle)
    print_stage_result("related dynamic variable finding", pipeline_start)

    if args.skip_format_extraction:
        print("[skip] dynamic format extraction")
        return

    print("[start] dynamic format extraction")
    with open(paths["pickle_grouped"], "rb") as handle:
        dynamic_based_updated_df = pickle.load(handle)

    format_start = time.time()
    dynamic_format_df = value_format_generate_with_group(
        dynamic_based_updated_df,
        sample_ratio=args.sample_ratio,
        split_prefixes=split_prefixes,
    )
    dynamic_format_df.to_csv(paths["output_format"], index=False)

    with open(paths["pickle_format"], "wb") as handle:
        pickle.dump(dynamic_format_df, handle)

    print_stage_result("dynamic format extraction", format_start)
    print_stage_result("full pipeline", pipeline_start)


if __name__ == "__main__":
    run(parse_args())
