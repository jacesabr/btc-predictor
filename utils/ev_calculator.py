"""
Expected Value Calculator and Kelly Criterion

Core decision-making logic: should we enter this market?
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class EVResult:
    """Result of an EV calculation."""
    expected_value: float      # EV per $1 wagered
    edge: float                # Model prob - implied prob
    implied_probability: float # What the market thinks
    model_probability: float   # What our model thinks
    kelly_fraction: float      # Optimal bet size as fraction of bankroll
    signal: str                # "STRONG_ENTER", "MARGINAL", "PASS"
    reasoning: str


def calculate_ev(
    model_probability: float,
    market_odds: float,
    max_kelly: float = 0.25,
    min_ev_threshold: float = 0.05,
    strong_ev_threshold: float = 0.15,
) -> EVResult:
    """
    Calculate expected value and optimal position size.
    
    Args:
        model_probability: Our model's estimated probability (0-1) of the predicted outcome
        market_odds: Decimal odds being offered (e.g., 2.0 means 1:1, 3.0 means 1:2 payout)
        max_kelly: Maximum Kelly fraction (cap to avoid over-betting)
        min_ev_threshold: Minimum EV to consider entering
        strong_ev_threshold: EV above this is a strong signal
    
    Returns:
        EVResult with all calculations
    """
    # Implied probability from market odds
    # If odds are 3.0 (1:2), market implies 1/3 = 33% chance
    implied_prob = 1.0 / (1.0 + market_odds) if market_odds > 0 else 1.0
    
    # Edge: how much better we think we are than the market
    edge = model_probability - implied_prob
    
    # Expected value per $1 bet
    # EV = P(win) * payout - P(lose) * stake
    ev = (model_probability * market_odds) - (1 - model_probability)
    
    # Kelly Criterion: optimal fraction of bankroll to bet
    # f* = (bp - q) / b where b=odds, p=win prob, q=lose prob
    if market_odds > 0 and edge > 0:
        kelly = edge / market_odds
        kelly = min(kelly, max_kelly)  # Cap it
    else:
        kelly = 0.0
    
    # Determine signal
    if ev >= strong_ev_threshold:
        signal = "STRONG_ENTER"
        reasoning = f"+EV of {ev:.3f} exceeds strong threshold. Edge: {edge*100:.1f}%. Kelly suggests {kelly*100:.1f}% of bankroll."
    elif ev >= min_ev_threshold:
        signal = "MARGINAL"
        reasoning = f"+EV of {ev:.3f} is positive but marginal. Edge: {edge*100:.1f}%. Proceed with caution."
    elif ev > 0:
        signal = "WEAK"
        reasoning = f"Slightly +EV ({ev:.3f}) but below threshold. Consider passing."
    else:
        signal = "PASS"
        reasoning = f"Negative EV ({ev:.3f}). Market odds don't justify entry. Edge: {edge*100:.1f}%."
    
    return EVResult(
        expected_value=ev,
        edge=edge,
        implied_probability=implied_prob,
        model_probability=model_probability,
        kelly_fraction=kelly,
        signal=signal,
        reasoning=reasoning,
    )


def optimal_entry_odds(model_probability: float) -> float:
    """
    Calculate the minimum odds needed for +EV entry given our model probability.
    
    If our model says 55% chance of UP, we need odds > 1/0.55 - 1 = 0.818
    i.e., better than 1:0.82 odds.
    
    At 1:3 odds with a 55% model, EV = 0.55 * 3 - 0.45 = 1.2 (massive +EV)
    """
    if model_probability <= 0 or model_probability >= 1:
        return float('inf')
    return (1 - model_probability) / model_probability


def required_accuracy_for_odds(market_odds: float) -> float:
    """
    What accuracy do we need to be +EV at these odds?
    
    At 1:1 (odds=1.0): need >50%
    At 1:2 (odds=2.0): need >33%  
    At 1:3 (odds=3.0): need >25%
    """
    return 1.0 / (1.0 + market_odds)
