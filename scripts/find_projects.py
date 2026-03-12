import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from src.core.database import AsyncSessionLocal
from src.services.github_project_scraper import (  # noqa: E402
    DOMAIN_CONFIGS,
    save_candidates_to_file,
    scrape_domain_candidates,
    upsert_candidates,
)

BASEDIR = Path(__file__).resolve().parent.parent


def emit_progress(event: str, payload: dict) -> None:
    domain_name = payload.get("domain_name") or payload.get("domain") or "Unknown Domain"

    if event == "domain_start":
        print(
            f"\n[{domain_name}] Starting discovery: target={payload['target']} discovery_target={payload['discovery_target']} queries={payload['queries_total']}"
        )
        return

    if event == "query_start":
        print(
            f"[{domain_name}] Query {payload['query_index']}/{payload['queries_total']}: {payload['query']}"
        )
        return

    if event == "page_fetched":
        print(
            f"[{domain_name}]  Page {payload['page']}: fetched={payload['fetched']} scanned={payload['scanned']} accepted={payload['accepted']} filtered={payload['prefilter_rejected'] + payload['enrichment_rejected'] + payload['duplicate']}"
        )
        return

    if event == "candidate_accepted":
        print(
            f"[{domain_name}]  Accepted {payload['accepted']}/{payload['discovery_target']} -> {payload['repo']} (score={payload['score']})"
        )
        return

    if event == "domain_complete":
        print(
            f"[{domain_name}] Completed: selected={payload['selected']} collected={payload['collected']} scanned={payload['scanned']} duplicates={payload['duplicate']} prefilter_rejected={payload['prefilter_rejected']} enrichment_rejected={payload['enrichment_rejected']}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find GitHub projects that fit Project Hub collection requirements.")
    parser.add_argument("--domain", choices=[*DOMAIN_CONFIGS.keys(), "all"], default="all")
    parser.add_argument("--target", type=int, default=100, help="Number of candidates per domain")
    parser.add_argument("--per-page", type=int, default=30)
    parser.add_argument("--max-pages-per-query", type=int, default=8)
    parser.add_argument("--min-stars", type=int, default=60)
    parser.add_argument("--require-demo", action="store_true", help="Only keep repositories with a live demo/homepage")
    parser.add_argument("--use-ai", action="store_true", help="Generate case-study fields using the configured AI service")
    parser.add_argument("--upsert-db", action="store_true", help="Insert or update GitHub projects in the shared database")
    parser.add_argument("--output", type=str, default="", help="Optional JSON output path for the collected candidates")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    domains = list(DOMAIN_CONFIGS.keys()) if args.domain == "all" else [args.domain]

    all_candidates = []
    for domain in domains:
        candidates = await scrape_domain_candidates(
            domain_slug=domain,
            target_count=args.target,
            per_page=args.per_page,
            max_pages_per_query=args.max_pages_per_query,
            min_stars=args.min_stars,
            require_demo=args.require_demo,
            use_ai=args.use_ai,
            progress_callback=emit_progress,
        )
        all_candidates.extend(candidates)

    output_path = Path(args.output) if args.output else BASEDIR / "output" / f"project-candidates-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
    save_candidates_to_file(all_candidates, output_path)

    summary = {
        "domains": domains,
        "count": len(all_candidates),
        "output": str(output_path),
    }

    if args.upsert_db and all_candidates:
        async with AsyncSessionLocal() as db:
            db_summary = await upsert_candidates(db, all_candidates)
        summary["database"] = db_summary

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())