"""Measure prewarmed local extraction and a 30-session burst without live services."""

import argparse
import asyncio
import json
import math
import os
from collections import Counter
from time import monotonic

from realtime_voice.clients.asr_keywords import AsrKeywordExtractor


def percentiles(values):
    ordered = sorted(values)
    return {
        f"p{p}_ms": round(ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)] * 1000, 3)
        for p in (50, 95, 99)
    } if ordered else {}


async def main(budgets_ms, rounds, workers, concurrency):
    # Spawned workers re-import this script: do not load the gateway/Torch there.
    from realtime_voice.session.runtime import SessionRuntime
    from realtime_voice.session.state import SessionState, TurnContext, TurnStage

    extractor = AsrKeywordExtractor(workers=workers)
    started = monotonic()
    await extractor.start()
    reports = {"environment": {
        "backend": "process", "workers": workers, "concurrency": concurrency,
        "cpu_count": os.cpu_count(), "gateway_pid": os.getpid(),
        "worker_pids": extractor._worker_pids,
        "startup_ms": round((monotonic() - started) * 1000, 3),
    }}
    texts = {
        "short": "李白在杭州游览西湖，还去过北京。",
        "medium": "李白在杭州游览西湖，那里有美丽的风景。" * 10,
        "long": "这里有美丽的风景，可以慢慢欣赏，了解相关的历史和文化。" * 120 + "李白 杭州 西湖",
        "unknown_names": "阿尔塔洛斯与缇娜薇娅在克里斯塔维尔讨论艾尔瑟尼亚。" * 7,
    }
    try:
        for name, text in texts.items():
            durations = []
            roundtrips = []
            for _ in range(100):
                started = monotonic()
                result = await extractor.extract(text, deadline=started + 2)
                if result.status not in {"success", "empty"}:
                    raise RuntimeError(f"single extraction failed: {result.status}")
                durations.append(result.compute_seconds)
                roundtrips.append(monotonic() - started)
            reports[name] = {
                "chars": len(text), "compute": percentiles(durations),
                "roundtrip": percentiles(roundtrips),
            }

        class Worker:
            async def run(self):
                pass

        runtimes = [
            SessionRuntime(
                state=SessionState("u", str(i), 16000), asr_client=None,
                thinker_client=None, tts_client=None, receiver=Worker(),
                vad_worker=Worker(), sender=Worker(), asr_keyword_extractor=extractor,
            ) for i in range(concurrency)
        ]
        for runtime in runtimes:
            runtime.actor.state.turns[1] = TurnContext(
                1, "", None, stage=TurnStage.COMPLETED, reply_text="李白在杭州。",
                asr_keywords="李白 杭州", asr_keywords_status="success",
            )
        async def burst(text, budget_ms):
            lookup, latencies, compute, lags, statuses = [], [], [], [], Counter()
            stop = asyncio.Event()

            async def ticker():
                while not stop.is_set():
                    start = monotonic()
                    await asyncio.sleep(0.001)
                    lags.append(max(0, monotonic() - start - 0.001))

            async def job(i):
                start = monotonic()
                result = await extractor.extract(text, deadline=start + budget_ms / 1000)
                latencies.append(monotonic() - start)
                statuses[result.status] += 1
                if result.status in {"success", "empty"}:
                    compute.append(result.compute_seconds)
                start = monotonic()
                runtimes[i]._asr_context()
                lookup.append(monotonic() - start)

            ticker_task = asyncio.create_task(ticker())
            try:
                await asyncio.sleep(0.005)
                for _ in range(rounds):
                    await asyncio.gather(*(job(i) for i in range(concurrency)))
                    await asyncio.sleep(0.025)
                    # Do not let timed-out work contaminate the next sample.
                    while extractor._inflight:
                        await asyncio.sleep(0.001)
            finally:
                stop.set()
                await ticker_task
            return {
                "budget_ms": budget_ms, "jobs": sum(statuses.values()),
                "statuses": dict(statuses),
                "success_rate": round(statuses["success"] / sum(statuses.values()), 4),
                "latency": percentiles(latencies), "event_loop_lag": percentiles(lags),
                "event_loop_lag_max_ms": round(max(lags) * 1000, 3),
                "compute": percentiles(compute), "asr_cache_lookup": percentiles(lookup),
            }

        reports[f"{concurrency}_session_burst"] = {
            name: {str(budget): await burst(texts[name], budget) for budget in budgets_ms}
            for name in ("medium", "unknown_names", "long")
        }
        print(json.dumps(reports, ensure_ascii=False, indent=2))
    finally:
        await extractor.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--budgets-ms", nargs="+", type=float, default=[100])
    parser.add_argument("--rounds", type=int, default=15)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=30)
    args = parser.parse_args()
    if args.rounds < 1 or any(not math.isfinite(b) or b <= 0 for b in args.budgets_ms):
        parser.error("rounds and budgets must be positive and finite")
    if args.workers < 1 or args.concurrency < 1:
        parser.error("workers and concurrency must be positive")
    asyncio.run(main(args.budgets_ms, args.rounds, args.workers, args.concurrency))
