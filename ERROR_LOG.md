# Prediction Pipeline - Blocking Issues Log
**Date**: 2026-04-22  
**Status**: BLOCKED - Awaiting User Action

---

## 🔴 CRITICAL BLOCKERS

### 1. Cohere API Key - Still Trial Tier
**Issue**: New Cohere key `0nFYXrNoKfkDDKp93zHA0QU2teY7A1noHajwmJrX` is ALSO a Trial key  
**Error**: `Cohere embed HTTP 429: You are using a Trial key, limited to 1000 API calls / month`  
**Impact**: Historical analyst cannot fire → predictions pause every ~2 minutes  
**Blocking**: Yes - Prevents end-to-end prediction completion  
**Solution**: 
- Visit https://dashboard.cohere.com/api-keys
- Upgrade key to **Production tier** (not Trial)
- Ensure rate limits are sufficient (recommend 10k+/month)
- Update .env with production key

**Evidence in Logs**:
```
ERROR:engine:Cohere unavailable — pausing predictions: Cohere embed HTTP 429: {"id":"0e10cf96-1fdf-4aae-b1cd-652e4b1e5b32","message":"You are using a Trial key, which is limited to 1000 API calls / month...
```

---

## ⚠️ SECONDARY ISSUES (Non-blocking)

### 2. Polymarket API
**Issue**: No active BTC 5-min market found  
**Error**: Repeated warnings about window 1776855300, 1776855600  
**Impact**: Market odds not available for EV calculation  
**Status**: Non-blocking - predictions continue without market context

### 3. CoinGecko Rate Limit
**Issue**: HTTP 429 from CoinGecko API  
**Impact**: 19/20 dashboard signals instead of 20/20  
**Status**: Non-blocking - fallback working (OKX → CoinAPI)

### 4. OKX Network Unreachable
**Issue**: `Cannot connect to host www.okx.com:443 ssl:default [The specified network name is no longer available]`  
**Impact**: okx_funding fails, falls back to coinapi_vwap  
**Status**: Gracefully handled by fallback system ✓

---

## ✓ WHAT IS WORKING

- ✓ Dashboard signals fetch (19/20)
- ✓ CoinAPI integration (5 sources available)
- ✓ Unified specialist (9.8s completion)
- ✓ Binance expert (8.3s completion)
- ✓ CoinAPI fallback system active
- ✓ Specialist timeout fix in place

---

## SPECIALIST COMPLETION OBSERVED

**Bar #110 (11:00:00 UTC)**:
- Unified specialist: 9.8s ✓ | dow=UP fib=DOWN all=UP acc=UP har=UP | creative_edge: YES
- Binance expert: 8.3s ✓ | UP 85%
- Dashboard signals: 19/20 ✓
- Historical analyst: BLOCKED (Cohere HTTP 429)
- DeepSeek: BLOCKED (Cohere HTTP 429)

---

## ACTION REQUIRED

**User must upgrade Cohere API key to Production tier before predictions can complete.**

Without this, predictions will pause every ~2 minutes when Cohere rate limit is hit.

