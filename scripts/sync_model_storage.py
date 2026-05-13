#!/usr/bin/env python3
"""Upload local rebound model artifacts to Supabase Storage."""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from dotenv import load_dotenv

from services.model_storage import upload_model_artifacts
from services.research_database import build_supabase

load_dotenv()
ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = ROOT / "models"


def _active_model_paths(sb) -> list[Path]:
    rows = (
        sb.table("ml_models")
        .select("model_path,feature_path,importance_path,is_active")
        .eq("is_active", True)
        .execute()
        .data
        or []
    )
    paths: list[Path] = []
    for row in rows:
        for key in ("model_path", "feature_path", "importance_path"):
            value = row.get(key)
            if value:
                paths.append(ROOT / str(value))
    return paths


def _all_model_paths() -> list[Path]:
    patterns = ("*.pkl", "*_features.json", "*_importance.csv")
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(MODEL_DIR.glob(pattern))
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync rebound model artifacts to Supabase Storage")
    parser.add_argument("--all", action="store_true", help="Upload all local model artifacts, not only active models")
    args = parser.parse_args()

    sb = build_supabase()
    paths = _all_model_paths() if args.all else _active_model_paths(sb)
    existing = sorted({p for p in paths if p.exists()})
    missing = sorted({p for p in paths if not p.exists()})
    for path in missing:
        print(f"[model_storage] missing local artifact: {path}")
    uploaded = upload_model_artifacts(sb, existing, root=ROOT)
    print(f"[model_storage] uploaded files={uploaded}")


if __name__ == "__main__":
    main()

