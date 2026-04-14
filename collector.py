"""Data collection entrypoint."""

from gex_engine import collect_all_symbols
from config import OPTIONS_FILE, SNAPSHOT_DB
from pro.snapshot_store import write_snapshot, write_metric


def run_collection():
    df_out = collect_all_symbols()
    df_out.to_csv(OPTIONS_FILE, index=False)
    write_snapshot(SNAPSHOT_DB, df_out)
    try:
        df_out["total_gex"] = df_out["call_gex"] + df_out["put_gex"]
        for symbol, df_symbol in df_out.groupby("symbol"):
            net_gex = float(df_symbol["total_gex"].sum())
            write_metric(
                SNAPSHOT_DB,
                symbol=str(symbol),
                net_gex=net_gex,
                row_count=len(df_symbol),
                unique_strikes=int(df_symbol["strike"].nunique()),
            )
    except Exception:
        pass
    return df_out


if __name__ == "__main__":
    result = run_collection()
    print(f"Saved {len(result)} rows to {OPTIONS_FILE}")
