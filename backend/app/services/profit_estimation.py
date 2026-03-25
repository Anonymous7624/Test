"""
Basic profit estimation — replace with comps, fees, and shipping models later.
"""

from dataclasses import dataclass


@dataclass
class ProfitEstimate:
    estimated_resale: float
    estimated_profit: float
    profitable: bool


def estimate_profit(asking_price: float, category_slug: str) -> ProfitEstimate:
    # Mock multipliers: electronics slightly higher assumed margin (heuristic resale only).
    bump = {"electronics": 1.45, "vehicles": 1.15}.get(category_slug, 1.35)
    estimated_resale = round(asking_price * bump, 2)
    estimated_profit = round(estimated_resale - asking_price, 2)
    return ProfitEstimate(
        estimated_resale=estimated_resale,
        estimated_profit=estimated_profit,
        profitable=estimated_profit > 0,
    )
