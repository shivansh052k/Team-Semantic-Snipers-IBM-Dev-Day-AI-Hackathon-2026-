import json
import re
from pathlib import Path

import pandas as pd


# ------------- CONFIG -------------
# Set this to the folder that contains your CSV files
# Example: DATA_DIR = Path("/Users/you/Downloads/my_datasets")
DATA_DIR = Path("./Dataset")

# Output folder for JSON files
OUT_DIR = Path("./cloudant_seed_json")

# File-specific parsing rules.
# Keys are the CSV filenames you have (case-sensitive). Adjust if your names differ.
CONFIG = {
    "Course_Catalog_Mock.csv": {
        "id_col": "course_id",
        "list_cols": ["skills", "skill_tags_normalized"],
        "date_cols": [],
        "drop_cols": [],
    },
    "Work_log_Mock.csv": {
        # We will generate event_id from LogID and set _id = event_id
        "id_col": None,
        "list_cols": ["Skill Tags (normalized)", "Technologies"],
        "date_cols": ["Date"],
        "drop_cols": ["Employee Name"],  # safety: drop names even if synthetic
        "transform": "work_log_to_work_events",
    },
    "Kudos_log.csv": {
        "id_col": "kudos_id",
        "list_cols": ["values_tags"],
        "date_cols": ["created_at", "approved_at"],
        "drop_cols": [],
    },
    "Growth_Recos_log.csv": {
        "id_col": "reco_id",  # IMPORTANT: your CSV uses reco_id
        "list_cols": ["recommended_course_ids", "skill_tags_input", "technologies_snapshot"],
        "date_cols": ["created_at"],
        "drop_cols": [],
    },
    "Pulse_aggregates.csv": {
        "id_col": "pulse_id",
        "list_cols": ["top_signals"],
        "date_cols": ["week_start"],
        "drop_cols": [],
        # recommended_actions is pipe-delimited in your mock
        "pipe_list_cols": ["recommended_actions"],
    },
}


# ------------- HELPERS -------------
def to_list_commas(val):
    """Convert comma-separated string or JSON string list to Python list."""
    if pd.isna(val):
        return []
    s = str(val).strip()
    if not s:
        return []

    # If already JSON-like list, try to parse
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

    return [x.strip() for x in s.split(",") if x.strip()]


def to_list_pipes(val):
    """Convert pipe-delimited string to list."""
    if pd.isna(val):
        return []
    s = str(val).strip()
    if not s:
        return []
    return [x.strip() for x in s.split("|") if x.strip()]


def to_iso_like(val):
    """Leave ISO timestamps alone; convert YYYY-MM-DD to YYYY-MM-DDT12:00:00Z."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s + "T12:00:00Z"
    return s


def safe_write_json(records, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def work_log_to_work_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Work_log_Mock.csv into the work_events schema:
    event_id/_id, timestamp, employee_id, manager_id, team_id, title, description, tags, etc.
    """
    # drop name column if present
    if "Employee Name" in df.columns:
        df = df.drop(columns=["Employee Name"])

    out = pd.DataFrame()
    # Create event_id from LogID
    out["event_id"] = df["LogID"].apply(lambda x: f"log_{int(x)}" if pd.notna(x) else None)
    out["_id"] = out["event_id"]

    out["timestamp"] = df["Date"].apply(to_iso_like)
    out["employee_id"] = df["Employee ID"].astype(str)
    out["manager_id"] = df["Manager ID"].astype(str)
    out["team_id"] = df["Team ID"].astype(str)
    out["title"] = df["Task Name"].astype(str)

    def build_desc(row):
        parts = []
        if pd.notna(row.get("Work Item Type")):
            parts.append(f"Type: {row['Work Item Type']}")
        if pd.notna(row.get("Comments")) and str(row.get("Comments")).strip():
            parts.append(f"Notes: {row['Comments']}")
        if pd.notna(row.get("Artifact Link")) and str(row.get("Artifact Link")).strip():
            parts.append(f"Link: {row['Artifact Link']}")
        return " | ".join(parts) if parts else None

    out["description"] = df.apply(build_desc, axis=1)

    # Build tags as union of Skill Tags (normalized) and Technologies
    skill_tags = df["Skill Tags (normalized)"].apply(to_list_commas)
    technologies = df["Technologies"].apply(to_list_commas)

    out["tags"] = [
        sorted(set((a or []) + (b or [])))
        for a, b in zip(skill_tags, technologies)
    ]

    # optional extra fields (helpful for demos)
    for col in ["Status", "Percent Complete", "Complexity"]:
        if col in df.columns:
            out[col.lower().replace(" ", "_")] = df[col].astype(str)

    for col in ["Estimated Hours", "Actual Hours", "Bugs Reported"]:
        if col in df.columns:
            out[col.lower().replace(" ", "_")] = pd.to_numeric(df[col], errors="coerce")

    return out


# ------------- MAIN -------------
def convert_csv(csv_path: Path):
    filename = csv_path.name
    if filename not in CONFIG:
        print(f"Skip (not in CONFIG): {filename}")
        return

    cfg = CONFIG[filename]
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]

    # Drop columns if requested
    for c in cfg.get("drop_cols", []):
        if c in df.columns:
            df = df.drop(columns=[c])

    # Transform if needed
    if cfg.get("transform") == "work_log_to_work_events":
        df = work_log_to_work_events(df)
    else:
        # Create _id from id_col
        id_col = cfg.get("id_col")
        if not id_col or id_col not in df.columns:
            raise ValueError(
                f"{filename}: id_col '{id_col}' not found. Columns: {list(df.columns)}"
            )
        df["_id"] = df[id_col].astype(str)

        # Comma-delimited list columns
        for col in cfg.get("list_cols", []):
            if col in df.columns:
                df[col] = df[col].apply(to_list_commas)

        # Pipe-delimited list columns
        for col in cfg.get("pipe_list_cols", []):
            if col in df.columns:
                df[col] = df[col].apply(to_list_pipes)

        # Date columns normalize
        for col in cfg.get("date_cols", []):
            if col in df.columns:
                df[col] = df[col].apply(to_iso_like)

    # Replace NaN with None
    records = df.where(pd.notnull(df), None).to_dict(orient="records")

    out_name = filename.replace(".csv", ".json")
    out_path = OUT_DIR / out_name
    safe_write_json(records, out_path)
    print(f"Wrote {out_path} with {len(records)} docs")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not DATA_DIR.exists():
        raise FileNotFoundError(f"DATA_DIR does not exist: {DATA_DIR.resolve()}")

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR.resolve()}")

    for csv_path in csv_files:
        convert_csv(csv_path)


if __name__ == "__main__":
    main()
