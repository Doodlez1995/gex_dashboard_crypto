import json
import pandas as pd
import os
from config import DATA_DIR


def save_gex(df, coin):
    os.makedirs(DATA_DIR, exist_ok=True)

    payload = {
        "coin": coin,
        "updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": df.to_dict(orient="records")
    }

    with open(f"{DATA_DIR}/gex_{coin}.json", "w", encoding="utf-8") as f:
        json.dump(payload, f)


def load_gex(coin):
    with open(f"{DATA_DIR}/gex_{coin}.json", encoding="utf-8") as f:
        return json.load(f)


def gamma_flip(strikes, gex):
    cum = 0
    for s, g in sorted(zip(strikes, gex)):
        cum += g
        if cum >= 0:
            return s
    return None
