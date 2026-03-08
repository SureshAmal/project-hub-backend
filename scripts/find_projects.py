import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

basedir = Path(__file__).resolve().parent.parent
sys.path.append(str(basedir))

from src.core.database import AsyncSessionLocal
from src.services.github_project_scraper import (  # noqa: E402
    DOMAIN_CONFIGS,
    save_candidates_to_file,
    scrape_domain_candidates,
    upsert_candidates,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find GitHub projects that fit Project Hub collection requirements.")
    parser.add_argument("--domain", choices=[*DOMAIN_CONFIGS.keys(), "all"], default="all")
    parser.add_argument("--target", type=int, default=20, help="Number of candidates per domain")
    parser.add_argument("--per-page", type=int, default=30)
    parser.add_argument("--max-pages-per-query", type=int, default=3)
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
        )
        all_candidates.extend(candidates)

    output_path = Path(args.output) if args.output else basedir / "output" / f"project-candidates-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
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