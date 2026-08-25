#!/usr/bin/env python3
"""A federated tick-network node: this repo's own append-only chain, keyed to the
global tick spine at kody-w/dogg.

Every run reads the spine's current tick anchor, takes this node's themed snapshot of
keyless public APIs, and appends one frame referencing that tick. Different repos, run
by different people, each with their own outlook — all joinable on the tick key. To
start your own node: fork this repo, edit THEME/STREAM/SOURCES below, enable the
scheduled workflow. Frames verify with the reference implementation (tools/rapp.py,
from kody-w/rapp-1); CI re-verifies the whole chain on every push.
"""
import json, sys, pathlib, datetime, urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import rapp as R

ROOT = pathlib.Path(__file__).resolve().parent.parent
SPINE_HEAD = "https://raw.githubusercontent.com/kody-w/dogg/main/ticks/HEAD.json"
TIMEOUT = 8

# ---- edit these three for your node -------------------------------------------------
THEME = "markets"                     # also the data directory name
STREAM = "markets:@kody-w/dogg-markets"                   # your stream id (your repo, your name)
# SOURCES: name -> zero-arg callable returning a SMALL dict of facts.
# rapp/1 canonical hashing forbids floats: numeric facts ride as strings or ints.
# -------------------------------------------------------------------------------------

def utc():
    n = datetime.datetime.now(datetime.timezone.utc)
    return n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}Z"

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": f"tick-node-{THEME}"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode())

SOURCES = {
    "btc_usd": lambda: {"spot": str(get("https://api.coinbase.com/v2/prices/BTC-USD/spot")["data"]["amount"])},
    "eth_usd": lambda: {"spot": str(get("https://api.coinbase.com/v2/prices/ETH-USD/spot")["data"]["amount"])},
    "fx_usd": lambda: (lambda r: {k: f"{r[k]:.4f}" for k in ("EUR","GBP","JPY","CNY","CHF","INR","BRL","MXN")})(
        get("https://open.er-api.com/v6/latest/USD")["rates"]),
    "crypto_market": lambda: (lambda d: {"total_mcap_usd": str(int(d["total_market_cap"]["usd"])),
                                         "btc_dominance_pct": f"{d['market_cap_percentage']['btc']:.1f}"})(
        get("https://api.coingecko.com/api/v3/global")["data"]),
    "btc_fees": lambda: {"fastest_sat_vb": int(get("https://mempool.space/api/v1/fees/recommended")["fastestFee"])},
}

def load_chain(d):
    if not (d / "HEAD.json").exists():
        return []
    count = json.loads((d / "HEAD.json").read_text())["count"]
    return [json.loads((d / f"{i}.json").read_text()) for i in range(count)]

def main():
    spine = get(SPINE_HEAD)
    tick_n, tick_hash = spine["count"] - 1, spine["head_frame"]
    d = ROOT / THEME
    d.mkdir(exist_ok=True)
    chain = load_chain(d)
    head = chain[-1] if chain else None
    if head is not None and head["payload"].get("tick") == tick_n:
        print(f"{THEME}: tick {tick_n} already recorded — nothing to do")
        return
    data, failed = {}, []
    for name, fn in SOURCES.items():
        try:
            data[name] = fn()
        except Exception:
            failed.append(name)
    payload = {"tick": tick_n, "tick_frame": tick_hash, "spine": "kody-w/dogg",
               "fetched_utc": utc(), THEME: data, "sources_failed": failed}
    if head is None:
        payload["about"] = (f"A federated node of the global tick network: this repo's "
                            f"own {THEME} outlook, one frame per observed tick, keyed to "
                            "the spine's tick anchors so it joins every other node's "
                            "data on the same clock.")
    f = R.build_frame(f"{THEME}.snapshot", STREAM, (head["seq"] + 1) if head else 0,
                      utc(), payload, prev=(head["payload_hash"] if head else None))
    ok, step, why = R.verify_frame(f, head=head, stream_id_of_record=STREAM)
    if not ok:
        raise ValueError(f"refusing invalid frame: {step}: {why}")
    (d / f"{f['seq']}.json").write_text(json.dumps(f, indent=2, ensure_ascii=False) + "\n")
    (d / "HEAD.json").write_text(json.dumps({"count": f["seq"] + 1, "stream_id": STREAM,
        "head_frame": f["frame_hash"], "updated": utc()}, indent=2) + "\n")
    print(f"{THEME} frame {f['seq']} @ spine tick {tick_n}: {', '.join(data) or 'nothing'}"
          + (f" (failed: {', '.join(failed)})" if failed else ""))

if __name__ == "__main__":
    main()
