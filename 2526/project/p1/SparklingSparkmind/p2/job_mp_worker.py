
import pandas as pd

def process_job_chunk(args_tuple):
    # Unpack the chunk from the loop arguments
    chunk, chunk_idx = args_tuple

    # ── Step 2: Filter (Job features) ─────────────────────
    df_f = chunk[(chunk['salary_average'] > 0) & (chunk['salary_max'] > 0)].copy()
    if df_f.empty:
        return df_f

    # ── Step 3: Feature Engineering ────────────────────────
    df_f['salary_gap'] = df_f['salary_max'] - df_f['salary_min']
    df_f['gap_percentage'] = (df_f['salary_gap'] / df_f['salary_min']) * 100
    return df_f
