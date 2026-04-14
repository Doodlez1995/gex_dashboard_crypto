from typing import Dict

from pro.models import Profile
from pro.signals import concentration_score


def stability_component(cv: float) -> float:
    if cv <= 0.15:
        return 1.0
    if cv >= 0.7:
        return 0.0
    return max(0.0, 1.0 - (cv - 0.15) / 0.55)


def net_gex_component(net_gex: float) -> float:
    mag = min(abs(net_gex) / 1_000_000_000.0, 1.0)
    return mag


def flip_distance_component(profile: Profile) -> float:
    if profile.flip is None or profile.spot <= 0:
        return 0.5
    dist_pct = abs(profile.spot - profile.flip) / profile.spot
    return max(0.0, 1.0 - min(dist_pct / 0.03, 1.0))


def score_profile(profile: Profile, stability_cv: float, vol_regime: str, term_aligned: bool) -> Dict[str, float]:
    c1 = stability_component(stability_cv)
    c2 = net_gex_component(profile.net_gex)
    c3 = flip_distance_component(profile)
    c4 = concentration_score(profile)
    c5 = 1.0 if term_aligned else 0.5
    c6 = {"high": 0.7, "normal": 1.0, "low": 0.8}.get(vol_regime, 0.6)
    weighted = (0.22 * c1) + (0.2 * c2) + (0.18 * c3) + (0.16 * c4) + (0.12 * c5) + (0.12 * c6)
    score = int(round(weighted * 100))
    return {
        "score": float(score),
        "stability": c1,
        "gex_mag": c2,
        "flip_proximity": c3,
        "concentration": c4,
        "term_alignment": c5,
        "vol_regime": c6,
    }

