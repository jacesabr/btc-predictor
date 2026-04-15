"""
Ensemble Prediction Combiner

Combines predictions from multiple strategies using weighted confidence voting.
Weights are dynamically adjusted based on rolling accuracy — indicators that
prove unreliable (<40% over 20+ samples) are nearly disabled; those that
consistently outperform get boosted.
"""

from typing import Dict, Optional
import numpy as np


# Accuracy thresholds for weight tiers
_DISABLED_THRESH  = 0.40   # < 40% → near-zero weight (nearly disabled)
_WEAK_THRESH      = 0.50   # 40–50% → reduced weight
_STRONG_THRESH    = 0.60   # 60–65% → boosted weight
_EXCELLENT_THRESH = 0.65   # > 65% → max boost


def accuracy_to_label(accuracy: float, total: int, min_samples: int = 10) -> str:
    """Human-readable reliability label for an indicator."""
    if total < min_samples:
        return "LEARNING"
    if accuracy < _DISABLED_THRESH:
        return "DISABLED"
    if accuracy < _WEAK_THRESH:
        return "WEAK"
    if accuracy < _STRONG_THRESH:
        return "MARGINAL"
    if accuracy < _EXCELLENT_THRESH:
        return "RELIABLE"
    return "EXCELLENT"


def accuracy_to_target_weight(accuracy: float, total: int, min_samples: int = 10) -> float:
    """
    Map historical accuracy → target weight.

    Tiers (requires min_samples before adjusting):
      < 40%       → 0.05  (nearly disabled — worse than random)
      40–50%      → 0.10–0.50  (linear scale, below-average)
      50–60%      → 0.50–1.20  (linear scale, average to good)
      60–65%      → 1.20–2.00  (boosted)
      > 65%       → 2.00–3.00  (strongly boosted, capped at 3.0)
    """
    if total < min_samples:
        return 1.0  # default until enough data

    if accuracy < _DISABLED_THRESH:
        return 0.05

    if accuracy < _WEAK_THRESH:
        # Linear: 0.40 → 0.10,  0.50 → 0.50
        t = (accuracy - _DISABLED_THRESH) / (_WEAK_THRESH - _DISABLED_THRESH)
        return round(0.10 + t * 0.40, 3)

    if accuracy < _STRONG_THRESH:
        # Linear: 0.50 → 0.50,  0.60 → 1.20
        t = (accuracy - _WEAK_THRESH) / (_STRONG_THRESH - _WEAK_THRESH)
        return round(0.50 + t * 0.70, 3)

    if accuracy < _EXCELLENT_THRESH:
        # Linear: 0.60 → 1.20,  0.65 → 2.00
        t = (accuracy - _STRONG_THRESH) / (_EXCELLENT_THRESH - _STRONG_THRESH)
        return round(1.20 + t * 0.80, 3)

    # > 65%: scale up to 3.0
    t = min(1.0, (accuracy - _EXCELLENT_THRESH) / 0.15)
    return round(min(3.0, 2.00 + t * 1.00), 3)


class EnsemblePredictor:
    """Weighted ensemble that dynamically adjusts strategy weights based on accuracy."""

    def __init__(self, initial_weights: Optional[Dict[str, float]] = None):
        self.weights = initial_weights or {}
        self.default_weight = 1.0

    def predict(self, strategy_predictions: Dict[str, Dict]) -> Dict:
        """
        Combine strategy predictions into a single ensemble prediction.

        Args:
            strategy_predictions: {strategy_name: {signal, confidence, reasoning}}

        Returns:
            {signal, confidence, up_probability, bullish_count, bearish_count,
             weighted_up_score, weighted_down_score}
        """
        if not strategy_predictions:
            return {"signal": "UP", "confidence": 0.5, "up_probability": 0.5,
                    "bullish_count": 0, "bearish_count": 0}

        up_score = 0.0
        down_score = 0.0

        for name, pred in strategy_predictions.items():
            w = self.weights.get(name, self.default_weight)
            conf = pred.get("confidence", 0.5)
            if pred["signal"] == "UP":
                up_score += conf * w
            else:
                down_score += conf * w

        total = up_score + down_score
        up_prob = up_score / total if total > 0 else 0.5

        return {
            "signal": "UP" if up_prob > 0.5 else "DOWN",
            "confidence": max(up_prob, 1 - up_prob),
            "up_probability": up_prob,
            "bullish_count": sum(1 for p in strategy_predictions.values() if p["signal"] == "UP"),
            "bearish_count": sum(1 for p in strategy_predictions.values() if p["signal"] == "DOWN"),
            "weighted_up_score": up_score,
            "weighted_down_score": down_score,
        }

    def update_weights(
        self,
        strategy_accuracies: Dict[str, float],
        min_samples: int = 10,
        learning_rate: float = 0.15,
        strategy_counts: Optional[Dict[str, int]] = None,
    ):
        """
        Adjust weights based on rolling accuracy with tiered logic.

        - Requires min_samples before changing a weight (avoids noise from small N).
        - Uses exponential smoothing so weights move gradually, not in jumps.
        - Indicators with <40% accuracy (20+ samples) converge toward ~0.05 (disabled).
        - Indicators with >65% accuracy get up to 3× weight.
        """
        for name, accuracy in strategy_accuracies.items():
            count = (strategy_counts or {}).get(name, min_samples)
            target = accuracy_to_target_weight(accuracy, count, min_samples)
            current = self.weights.get(name, self.default_weight)
            # Exponential smoothing toward target
            new_weight = current * (1 - learning_rate) + target * learning_rate
            self.weights[name] = round(new_weight, 3)

    def update_weights_from_full_stats(
        self,
        accuracy_stats: Dict[str, Dict],
        min_samples: int = 10,
        learning_rate: float = 0.15,
    ):
        """
        Like update_weights() but accepts the richer {name: {correct, total, accuracy}}
        format returned by storage.get_strategy_accuracy_full().
        """
        accuracies = {n: s["accuracy"] for n, s in accuracy_stats.items()}
        counts     = {n: s["total"]    for n, s in accuracy_stats.items()}
        self.update_weights(accuracies, min_samples, learning_rate, counts)

    def get_weights(self) -> Dict[str, float]:
        return dict(self.weights)
