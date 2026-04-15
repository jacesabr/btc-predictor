"""
MongoDB Storage for tick data and prediction results.
Replaces SQLite — all prediction and tick data persists in MongoDB.
"""

import json
import time
import logging
from typing import List, Optional, Dict, Tuple

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, uri: str = "mongodb://localhost:27017", db_name: str = "btc_predictor"):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        self.db = self.client[db_name]
        try:
            self.client.admin.command("ping")
            self._create_indexes()
            logger.info("MongoDB connected: %s / %s", uri, db_name)
        except Exception as exc:
            logger.warning("MongoDB ping failed at startup (will retry on first use): %s", exc)

    def _create_indexes(self):
        self.db.ticks.create_index([("timestamp", ASCENDING)])
        self.db.predictions.create_index([("window_start", ASCENDING)], unique=True)
        self.db.predictions.create_index([("window_start", DESCENDING)])
        self.db.deepseek_predictions.create_index([("window_start", ASCENDING)], unique=True)
        self.db.deepseek_predictions.create_index([("window_start", DESCENDING)])

    # -------------------------------------------------------------------------
    # Ticks
    # -------------------------------------------------------------------------

    def store_tick(self, timestamp: float, mid: float, bid: float, ask: float, spread: float):
        self.db.ticks.insert_one({
            "timestamp": timestamp,
            "mid_price": mid,
            "bid_price": bid,
            "ask_price": ask,
            "spread": spread,
            "created_at": time.time(),
        })

    def get_prices(self, n: Optional[int] = None, since: Optional[float] = None) -> List[float]:
        if since:
            docs = list(self.db.ticks.find(
                {"timestamp": {"$gte": since}}, {"mid_price": 1, "_id": 0}
            ).sort("timestamp", ASCENDING))
        elif n:
            docs = list(self.db.ticks.find(
                {}, {"mid_price": 1, "_id": 0}
            ).sort("timestamp", DESCENDING).limit(n))
            docs = list(reversed(docs))
        else:
            docs = list(self.db.ticks.find(
                {}, {"mid_price": 1, "_id": 0}
            ).sort("timestamp", ASCENDING))
        return [d["mid_price"] for d in docs]

    # -------------------------------------------------------------------------
    # Ensemble predictions
    # -------------------------------------------------------------------------

    def store_prediction(
        self,
        window_start: float,
        window_end: float,
        start_price: float,
        signal: str,
        confidence: float,
        strategy_votes: Dict,
        market_odds: Optional[float] = None,
        ev: Optional[float] = None,
    ):
        try:
            self.db.predictions.insert_one({
                "window_start": window_start,
                "window_end": window_end,
                "start_price": start_price,
                "signal": signal,
                "confidence": confidence,
                "strategy_votes": json.dumps(strategy_votes),
                "market_odds": market_odds,
                "ev": ev,
                "created_at": time.time(),
            })
        except DuplicateKeyError:
            pass  # already stored

    def resolve_prediction(self, window_start: float, end_price: float):
        doc = self.db.predictions.find_one(
            {"window_start": window_start}, {"signal": 1, "start_price": 1}
        )
        if not doc:
            return
        actual = "UP" if end_price >= doc["start_price"] else "DOWN"
        correct = 1 if actual == doc["signal"] else 0
        self.db.predictions.update_one(
            {"window_start": window_start},
            {"$set": {"end_price": end_price, "actual_direction": actual, "correct": correct}},
        )

    def get_rolling_accuracy(self, n: int = 12) -> Tuple[int, int, float]:
        """Return (total, correct, accuracy) for the last N resolved predictions."""
        docs = list(self.db.predictions.find(
            {"correct": {"$exists": True, "$ne": None}},
            {"correct": 1, "_id": 0},
        ).sort("window_start", DESCENDING).limit(n))
        if not docs:
            return 0, 0, 0.0
        total = len(docs)
        correct = sum(1 for d in docs if d["correct"])
        return total, correct, correct / total if total > 0 else 0.0

    def get_total_accuracy(self) -> Tuple[int, int, float]:
        """Return (total, correct, accuracy) across ALL resolved predictions."""
        docs = list(self.db.predictions.find(
            {"correct": {"$exists": True, "$ne": None}},
            {"correct": 1, "_id": 0},
        ))
        if not docs:
            return 0, 0, 0.0
        total = len(docs)
        correct = sum(1 for d in docs if d["correct"])
        return total, correct, correct / total if total > 0 else 0.0

    def get_strategy_rolling_accuracy(self, n: int = 20) -> Dict[str, float]:
        docs = list(self.db.predictions.find(
            {"actual_direction": {"$exists": True, "$ne": None}},
            {"strategy_votes": 1, "actual_direction": 1, "_id": 0},
        ).sort("window_start", DESCENDING).limit(n))

        if not docs:
            return {}

        accuracy: Dict[str, Dict] = {}
        for doc in docs:
            votes_raw = doc.get("strategy_votes", "{}")
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else votes_raw
            except Exception:
                votes = {}
            actual = doc["actual_direction"]
            for name, vote in votes.items():
                if name not in accuracy:
                    accuracy[name] = {"correct": 0, "total": 0}
                accuracy[name]["total"] += 1
                if vote.get("signal") == actual:
                    accuracy[name]["correct"] += 1

        return {
            name: s["correct"] / s["total"] if s["total"] > 0 else 0.5
            for name, s in accuracy.items()
        }

    def get_strategy_accuracy_full(self, n: int = 100) -> Dict[str, Dict]:
        """
        Return {name: {correct, total, accuracy}} for the last N resolved predictions.
        Larger window than get_strategy_rolling_accuracy for stable accuracy estimates.
        """
        docs = list(self.db.predictions.find(
            {"actual_direction": {"$exists": True, "$ne": None}},
            {"strategy_votes": 1, "actual_direction": 1, "_id": 0},
        ).sort("window_start", DESCENDING).limit(n))

        if not docs:
            return {}

        stats: Dict[str, Dict] = {}
        for doc in docs:
            votes_raw = doc.get("strategy_votes", "{}")
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else votes_raw
            except Exception:
                votes = {}
            actual = doc["actual_direction"]
            for name, vote in votes.items():
                if name not in stats:
                    stats[name] = {"correct": 0, "total": 0}
                stats[name]["total"] += 1
                if vote.get("signal") == actual:
                    stats[name]["correct"] += 1

        return {
            name: {
                "correct":  s["correct"],
                "total":    s["total"],
                "accuracy": s["correct"] / s["total"] if s["total"] > 0 else 0.5,
            }
            for name, s in stats.items()
        }

    def get_recent_predictions(self, n: int = 50) -> List[Dict]:
        docs = list(self.db.predictions.find(
            {"correct": {"$exists": True, "$ne": None}},
            {"_id": 0, "window_start": 1, "window_end": 1, "start_price": 1,
             "end_price": 1, "signal": 1, "confidence": 1, "actual_direction": 1,
             "correct": 1, "market_odds": 1, "ev": 1},
        ).sort("window_start", DESCENDING).limit(n))
        return docs

    # -------------------------------------------------------------------------
    # DeepSeek predictions
    # -------------------------------------------------------------------------

    def store_deepseek_prediction(
        self,
        window_start: float,
        window_end: float,
        start_price: float,
        signal: str,
        confidence: int,
        reasoning: str,
        raw_response: str,
        full_prompt: str,
        polymarket_url: str,
        strategy_snapshot: str,
        latency_ms: int,
        window_count: int,
        data_received: str = "",
        data_requests: str = "",
        indicators_snapshot: str = "",
        narrative: str = "",
        free_observation: str = "",
        chart_path: str = "",
        dashboard_signals_snapshot: str = "",
    ):
        self.db.deepseek_predictions.update_one(
            {"window_start": window_start},
            {"$set": {
                "window_start":       window_start,
                "window_end":         window_end,
                "start_price":        start_price,
                "signal":             signal,
                "confidence":         confidence,
                "reasoning":          reasoning,
                "raw_response":       raw_response,
                "full_prompt":        full_prompt,
                "polymarket_url":     polymarket_url,
                "strategy_snapshot":  strategy_snapshot,
                "latency_ms":         latency_ms,
                "window_count":       window_count,
                "data_received":      data_received,
                "data_requests":      data_requests,
                "indicators_snapshot": indicators_snapshot,
                "narrative":          narrative,
                "free_observation":   free_observation,
                "chart_path":         chart_path,
                "dashboard_signals_snapshot": dashboard_signals_snapshot,
                "created_at":         time.time(),
            }},
            upsert=True,
        )

    def resolve_deepseek_prediction(self, window_start: float, end_price: float):
        doc = self.db.deepseek_predictions.find_one(
            {"window_start": window_start}, {"signal": 1, "start_price": 1}
        )
        if not doc:
            return
        actual = "UP" if end_price >= doc["start_price"] else "DOWN"
        correct = 1 if actual == doc["signal"] else 0
        self.db.deepseek_predictions.update_one(
            {"window_start": window_start},
            {"$set": {"end_price": end_price, "actual_direction": actual, "correct": correct}},
        )

    def get_agree_accuracy(self) -> Dict:
        """
        Accuracy when ensemble signal == DeepSeek signal.
        Returns {total_agree, correct_agree, accuracy_agree}.
        """
        ds_docs = {
            d["window_start"]: d
            for d in self.db.deepseek_predictions.find(
                {"signal": {"$exists": True}, "actual_direction": {"$exists": True}},
                {"window_start": 1, "signal": 1, "_id": 0},
            )
        }
        if not ds_docs:
            return {"total_agree": 0, "correct_agree": 0, "accuracy_agree": 0.0}

        ens_docs = list(self.db.predictions.find(
            {"window_start": {"$in": list(ds_docs.keys())},
             "actual_direction": {"$exists": True}},
            {"window_start": 1, "signal": 1, "actual_direction": 1, "_id": 0},
        ))

        total = correct = 0
        for doc in ens_docs:
            ws = doc["window_start"]
            if ds_docs.get(ws, {}).get("signal") == doc["signal"]:
                total += 1
                if doc["actual_direction"] == doc["signal"]:
                    correct += 1

        return {
            "total_agree": total,
            "correct_agree": correct,
            "accuracy_agree": correct / total if total > 0 else 0.0,
        }

    def get_deepseek_accuracy(self) -> Dict:
        docs = list(self.db.deepseek_predictions.find(
            {"correct": {"$exists": True, "$ne": None}},
            {"correct": 1, "_id": 0},
        ))
        if not docs:
            return {"total": 0, "correct": 0, "accuracy": 0.0}
        total = len(docs)
        correct = sum(1 for d in docs if d["correct"])
        return {"total": total, "correct": correct, "accuracy": correct / total}

    def get_recent_deepseek_predictions(self, n: int = 50) -> List[Dict]:
        docs = list(self.db.deepseek_predictions.find(
            {}, {"_id": 0}
        ).sort("window_start", DESCENDING).limit(n))
        return docs

    def get_prediction_history_with_indicators(self, n: int = 25) -> List[Dict]:
        """
        Return last N resolved prediction windows joined with their indicator snapshots.
        Each record: {window_start, actual_direction, strategy_votes, indicators_snapshot}
        Used by the pattern analyst specialist to find similar historical setups.
        """
        pred_docs = list(self.db.predictions.find(
            {"actual_direction": {"$exists": True, "$ne": None}},
            {"window_start": 1, "actual_direction": 1, "strategy_votes": 1, "start_price": 1, "_id": 0},
        ).sort("window_start", DESCENDING).limit(n))

        if not pred_docs:
            return []

        # Build a lookup of indicators_snapshot keyed by window_start
        ws_keys = [d["window_start"] for d in pred_docs]
        ds_docs = {
            d["window_start"]: d
            for d in self.db.deepseek_predictions.find(
                {"window_start": {"$in": ws_keys}, "indicators_snapshot": {"$exists": True}},
                {"window_start": 1, "indicators_snapshot": 1, "_id": 0},
            )
        }

        records = []
        for doc in reversed(pred_docs):  # chronological order
            ws = doc["window_start"]
            votes_raw = doc.get("strategy_votes", "{}")
            try:
                votes = json.loads(votes_raw) if isinstance(votes_raw, str) else (votes_raw or {})
            except Exception:
                votes = {}

            ind_raw = ds_docs.get(ws, {}).get("indicators_snapshot", "{}")
            try:
                indicators = json.loads(ind_raw) if isinstance(ind_raw, str) else (ind_raw or {})
            except Exception:
                indicators = {}

            records.append({
                "window_start":     ws,
                "actual_direction": doc["actual_direction"],
                "start_price":      doc.get("start_price"),
                "strategy_votes":   votes,
                "indicators":       indicators,
            })

        return records

    def get_audit_records(self, n: int = 500) -> List[Dict]:
        docs = list(self.db.deepseek_predictions.find(
            {}, {"_id": 0}
        ).sort("window_start", DESCENDING).limit(n))

        for rec in docs:
            for field in ("indicators_snapshot", "strategy_snapshot", "dashboard_signals_snapshot"):
                raw = rec.get(field)
                if raw and isinstance(raw, str):
                    try:
                        rec[field] = json.loads(raw)
                    except Exception:
                        pass
        return docs

    def close(self):
        self.client.close()
