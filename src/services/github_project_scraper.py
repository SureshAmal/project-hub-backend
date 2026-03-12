import asyncio
import base64
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.models.project import Domain, GitHubProject, Project
from src.services.ai_service import generate_project_details


DOMAIN_CONFIGS: dict[str, dict[str, Any]] = {
    "web-development": {
        "name": "Web Development",
        "queries": [
            "full stack web app demo in:name,description,readme stars:>80 archived:false fork:false",
            "dashboard web application live demo in:name,description,readme stars:>80 archived:false fork:false",
            "ecommerce web app deployed in:name,description,readme stars:>80 archived:false fork:false",
            "booking system web app demo in:name,description,readme stars:>60 archived:false fork:false",
        ],
        "keywords": {"web", "frontend", "backend", "dashboard", "cms", "ecommerce", "portal", "saas", "website", "fullstack"},
    },
    "artificial-intelligence": {
        "name": "Artificial Intelligence",
        "queries": [
            "ai assistant web app demo in:name,description,readme stars:>70 archived:false fork:false",
            "llm chatbot application live demo in:name,description,readme stars:>70 archived:false fork:false",
            "computer vision web app deployed in:name,description,readme stars:>70 archived:false fork:false",
            "generative ai application demo in:name,description,readme stars:>60 archived:false fork:false",
        ],
        "keywords": {"ai", "assistant", "agent", "llm", "computer-vision", "nlp", "chatbot", "rag", "vision"},
    },
    "machine-learning": {
        "name": "Machine Learning",
        "queries": [
            "machine learning web app demo in:name,description,readme stars:>70 archived:false fork:false",
            "ml application live demo in:name,description,readme stars:>70 archived:false fork:false",
            "recommendation system web app in:name,description,readme stars:>60 archived:false fork:false",
            "predictive analytics dashboard demo in:name,description,readme stars:>60 archived:false fork:false",
        ],
        "keywords": {"machine-learning", "ml", "prediction", "recommendation", "forecasting", "classification", "regression"},
    },
    "data-science": {
        "name": "Data Science",
        "queries": [
            "data science dashboard demo in:name,description,readme stars:>60 archived:false fork:false",
            "analytics platform live demo in:name,description,readme stars:>60 archived:false fork:false",
            "visualization web app deployed in:name,description,readme stars:>60 archived:false fork:false",
            "business intelligence dashboard demo in:name,description,readme stars:>60 archived:false fork:false",
        ],
        "keywords": {"data", "analytics", "visualization", "dashboard", "etl", "business-intelligence", "reporting", "streamlit"},
    },
    "cybersecurity": {
        "name": "Cybersecurity",
        "queries": [
            "security monitoring dashboard demo in:name,description,readme stars:>60 archived:false fork:false",
            "vulnerability scanner web app live demo in:name,description,readme stars:>60 archived:false fork:false",
            "threat detection dashboard deployed in:name,description,readme stars:>60 archived:false fork:false",
            "security lab platform demo in:name,description,readme stars:>60 archived:false fork:false",
        ],
        "keywords": {"security", "cybersecurity", "threat", "scanner", "siem", "forensics", "vulnerability", "soc", "monitoring"},
    },
}

EXCLUSION_KEYWORDS = {
    "library", "framework", "plugin", "sdk", "package", "module", "boilerplate", "starter", "template",
    "tutorial", "example", "course", "workshop", "awesome", "cheatsheet", "cookbook", "reference",
    "docs", "documentation", "book", "books", "free-programming-books", "snippets", "component-library", "ui-library", "toolkit", "cli",
    "awesome-list", "curated-list", "reading-list", "interview-questions",
    "dataset", "datasets", "vscode", "vs code", "extension", "openapi", "swagger", "postman", "collection",
    "devtool", "developer tool", "codegen", "generator", "api client", "chrome extension", "browser extension",
}
APP_KEYWORDS = {
    "app", "application", "platform", "dashboard", "system", "portal", "service", "management",
    "analytics", "monitoring", "automation", "booking", "chat", "marketplace", "tracking",
}
END_USER_PROJECT_KEYWORDS = {
    "customer", "user", "admin", "tenant", "booking", "order", "payment", "inventory", "crm",
    "erp", "cms", "blog", "store", "shop", "meeting", "video call", "collaboration", "workspace",
    "reporting", "visualization", "scanner", "detection", "alerts", "case management", "telemedicine",
}
LIVE_HOST_KEYWORDS = ("vercel.app", "netlify.app", "render.com", "railway.app", "fly.dev", "pages.dev", "web.app", "onrender.com", "streamlit.app", "huggingface.co/spaces")
LIVE_HINT_KEYWORDS = {"demo", "live demo", "preview", "deployed", "production", "try it", "visit", "open app", "website"}
APP_README_KEYWORDS = {
    "authentication", "login", "signup", "dashboard", "screenshots", "features", "deployment",
    "user", "admin", "workflow", "web app", "streamlit", "gradio", "frontend", "api",
}
LIBRARY_SIGNAL_KEYWORDS = {
    "library", "framework", "sdk", "package", "module", "toolkit", "plugin", "extension",
    "boilerplate", "starter", "template", "component library", "ui library", "design system",
    "engine", "runtime", "compiler", "dataset", "benchmark", "model zoo", "weights",
    "vscode", "vs code", "openapi", "swagger", "postman", "collection", "codegen", "generator",
    "api client", "developer tool", "devtool", "chrome extension", "browser extension", "copilot",
}
LIBRARY_TOPICS = {
    "library", "framework", "sdk", "package", "api-client", "plugin", "extension", "boilerplate",
    "template", "starter", "design-system", "component-library", "toolkit", "dataset", "benchmark",
    "openapi", "swagger", "postman", "vscode-extension", "chrome-extension", "browser-extension",
    "developer-tools", "cli", "api-wrapper", "sdk-generator",
}
FOUNDATIONAL_REPO_NAMES = {
    "pytorch", "tensorflow", "keras", "scikitlearn", "sklearn", "numpy", "pandas", "scipy",
    "matplotlib", "seaborn", "opencv", "pillow", "xgboost", "lightgbm", "catboost", "transformers",
    "diffusers", "langchain", "llamaindex", "react", "vue", "angular", "svelte", "nextjs", "nuxt",
    "django", "flask", "fastapi", "express", "nestjs", "spring", "rails", "laravel", "bootstrap",
    "tailwindcss", "electron", "metasploit", "nmap", "wireshark", "suricata", "zeek", "kali",
}
NON_PROJECT_REPO_NAMES = {
    "chatgpt-vscode", "openapi-generator", "swagger-ui", "swagger-editor", "postman-collection",
    "awesome-chatgpt-prompts", "public-apis", "datasets", "dataset", "openapi-typescript",
}
STUDENT_SCALE_EXCLUSION_KEYWORDS = {
    "distributed system", "distributed training", "deep learning framework", "machine learning framework",
    "database engine", "container runtime", "orchestration platform", "operating system", "compiler",
    "programming language", "browser engine", "inference engine", "model weights", "foundation model",
    "vscode extension", "visual studio code extension", "openapi specification", "swagger specification",
    "api schema", "prompt library", "prompt collection", "dataset collection", "developer productivity",
}
MAX_STUDENT_PROJECT_STARS = 15000
MAX_STUDENT_PROJECT_FORKS = 3000
MAX_STUDENT_PROJECT_SIZE = 150000
MAX_STUDENT_PROJECT_DURATION = 80
MAX_GITHUB_SEARCH_RESULTS = 1000

DIFFICULTY_RATIOS: dict[str, float] = {
    "EASY": 0.30,
    "MEDIUM": 0.40,
    "ADVANCED": 0.30,
}


@dataclass(slots=True)
class ScrapeCandidate:
    domain_slug: str
    domain_name: str
    title: str
    slug: str
    description: str
    repo_url: str
    repo_owner: str
    repo_name: str
    default_branch: str
    download_url: str
    live_url: str | None
    stars: int
    forks: int
    language: str | None
    tech_stack: list[str] = field(default_factory=list)
    difficulty: str = "MEDIUM"
    topics: list[str] = field(default_factory=list)
    project_type: str = "PROJECT"
    author: str = "Project Hub"
    introduction: str | None = None
    implementation: str | None = None
    technical_skills: list[str] = field(default_factory=list)
    tools_used: list[str] = field(default_factory=list)
    concepts_used: list[str] = field(default_factory=list)
    sub_domain: str | None = None
    case_study: str | None = None
    problem_statement: str | None = None
    solution_description: str | None = None
    prerequisites: list[str] = field(default_factory=list)
    deliverables: list[str] = field(default_factory=list)
    supposed_deadline: str | None = None
    requirements: list[str] = field(default_factory=list)
    requirements_text: str | None = None
    evaluation_criteria: str | None = None
    estimated_min_time: int = 10
    estimated_max_time: int = 40
    score: float = 0.0
    readme_excerpt: str | None = None

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


def build_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "project-hub-python-scraper",
    }
    if settings.GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {settings.GITHUB_TOKEN}"
    return headers


def _compute_github_retry_delay(response: httpx.Response) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 1.0)
        except ValueError:
            pass

    reset_header = response.headers.get("X-RateLimit-Reset")
    if reset_header:
        try:
            reset_ts = float(reset_header)
            now_ts = datetime.now(timezone.utc).timestamp()
            return max(reset_ts - now_ts + 1, 1.0)
        except ValueError:
            pass

    remaining = response.headers.get("X-RateLimit-Remaining")
    if remaining == "0":
        return 60.0

    return 20.0


def normalize_text(*parts: str | None) -> str:
    return " ".join(part.strip().lower() for part in parts if part).strip()


def slugify_repo(full_name: str) -> str:
    return full_name.lower().replace("/", "-")


def normalize_base_name(name: str) -> str:
    normalized = re.sub(r"(?:[-_](?:v|version)?\d+)$", "", name.lower())
    return re.sub(r"[^a-z0-9]+", "", normalized)


ProgressCallback = Callable[[str, dict[str, Any]], None]


def has_live_demo(repo: dict[str, Any], readme_text: str) -> bool:
    return pick_live_url(repo, readme_text) is not None


def is_foundational_or_library_repo(repo: dict[str, Any], readme_text: str, live_url: str | None) -> bool:
    metadata_text = normalize_text(repo.get("name"), repo.get("full_name"), repo.get("description"), " ".join(repo.get("topics") or []))
    readme_excerpt = normalize_text(readme_text[:3000])
    combined = f"{metadata_text} {readme_excerpt}".strip()
    topic_set = {str(topic).lower() for topic in (repo.get("topics") or [])}
    base_name = normalize_base_name(repo.get("name") or "")

    if base_name in FOUNDATIONAL_REPO_NAMES or base_name in NON_PROJECT_REPO_NAMES:
        return True

    library_score = 0
    app_score = 0

    for keyword in LIBRARY_SIGNAL_KEYWORDS:
        if keyword in metadata_text:
            library_score += 3
        elif keyword in combined:
            library_score += 1

    library_score += sum(2 for topic in topic_set if topic in LIBRARY_TOPICS)

    if any(phrase in combined for phrase in (
        "official library",
        "official framework",
        "python package",
        "javascript library",
        "typescript library",
        "sdk for",
        "install via pip",
        "install via npm",
    )):
        library_score += 4

    if live_url:
        app_score += 6

    for keyword in APP_KEYWORDS | LIVE_HINT_KEYWORDS | APP_README_KEYWORDS:
        if keyword in combined:
            app_score += 1

    if any(topic in topic_set for topic in {"webapp", "web-app", "dashboard", "saas", "streamlit", "gradio", "fullstack"}):
        app_score += 3

    return library_score >= max(4, app_score + 2)


def has_end_user_project_signals(repo: dict[str, Any], readme_text: str, live_url: str | None) -> bool:
    if not live_url:
        return False

    metadata_text = normalize_text(repo.get("name"), repo.get("description"), " ".join(repo.get("topics") or []))
    readme_excerpt = normalize_text(readme_text[:5000])
    combined = f"{metadata_text} {readme_excerpt}".strip()

    negative_signals = {
        "vscode extension", "visual studio code extension", "chrome extension", "browser extension",
        "openapi", "swagger", "postman", "dataset", "sdk", "library", "framework", "cli",
        "developer tool", "code generator", "api client", "copilot extension",
    }
    if any(signal in combined for signal in negative_signals):
        return False

    positive_score = 0
    if any(keyword in combined for keyword in APP_KEYWORDS):
        positive_score += 2
    if any(keyword in combined for keyword in END_USER_PROJECT_KEYWORDS):
        positive_score += 2
    if any(keyword in combined for keyword in APP_README_KEYWORDS):
        positive_score += 1
    if any(keyword in combined for keyword in LIVE_HINT_KEYWORDS):
        positive_score += 1
    if any(keyword in combined for keyword in {"screenshots", "demo", "deploy", "hosted", "production"}):
        positive_score += 1

    return positive_score >= 4


def is_student_buildable_repo(repo: dict[str, Any], readme_text: str, live_url: str | None, min_time: int, max_time: int) -> bool:
    if not live_url:
        return False

    if max_time > MAX_STUDENT_PROJECT_DURATION:
        return False

    if (repo.get("stargazers_count") or 0) > MAX_STUDENT_PROJECT_STARS:
        return False

    if (repo.get("forks_count") or 0) > MAX_STUDENT_PROJECT_FORKS:
        return False

    if (repo.get("size") or 0) > MAX_STUDENT_PROJECT_SIZE:
        return False

    combined = normalize_text(repo.get("name"), repo.get("description"), " ".join(repo.get("topics") or []), readme_text[:4000])
    if any(keyword in combined for keyword in STUDENT_SCALE_EXCLUSION_KEYWORDS):
        return False

    return True


def difficulty_from_repo(stars: int, size: int, topics: Iterable[str]) -> str:
    lowered_topics = {topic.lower() for topic in topics}
    if stars >= 4000 or size >= 150000 or {"microservices", "kubernetes", "distributed-system"} & lowered_topics:
        return "ADVANCED"
    if stars >= 1200 or size >= 60000:
        return "HARD"
    if stars >= 250 or size >= 20000:
        return "MEDIUM"
    return "EASY"


def estimate_duration(stars: int, size: int, readme_text: str) -> tuple[int, int, str]:
    complexity = 1
    lowered = readme_text.lower()
    if any(term in lowered for term in ["docker", "kubernetes", "microservice", "terraform", "training", "pipeline"]):
        complexity += 1
    if stars > 1500 or size > 75000:
        complexity += 1
    if stars > 5000 or size > 150000:
        complexity += 1

    if complexity <= 1:
        return 8, 20, "1 week"
    if complexity == 2:
        return 20, 40, "2 weeks"
    if complexity == 3:
        return 40, 80, "3-4 weeks"
    return 80, 120, "4-6 weeks"


def _build_regular_project_payload(
    *,
    title: str,
    domain_id: str,
    sub_domain: str | None,
    difficulty: str | None,
    min_time: int,
    max_time: int,
    tech_stack: list[str] | None,
    case_study: str | None,
    problem_statement: str | None,
    solution_description: str | None,
    supposed_deadline: str | None,
    prerequisites: list[str] | None,
    deliverables: list[str] | None,
    requirements: list[str] | None,
    requirements_text: str | None,
    evaluation_criteria: str | None,
    repo_url: str | None = None,
    live_url: str | None = None,
    download_url: str | None = None,
    skill_focus: list[str] | None = None,
) -> dict[str, Any]:
    normalized_problem_statement = (problem_statement or case_study or title).strip()
    normalized_case_study = (case_study or normalized_problem_statement).strip()
    normalized_solution_description = (solution_description or "Review the repository architecture and complete an implementation walkthrough.").strip()

    guide_lines = [
        "Imported from the GitHub curated catalog.",
        f"Source repository: {repo_url}" if repo_url else None,
        f"Live demo: {live_url}" if live_url else None,
        f"Download archive: {download_url}" if download_url else None,
    ]

    return {
        "title": title,
        "domain_id": domain_id,
        "sub_domain": sub_domain,
        "difficulty": (difficulty or "MEDIUM").upper(),
        "min_time": min_time,
        "max_time": max_time,
        "skill_focus": list(dict.fromkeys(skill_focus or tech_stack or [])),
        "case_study": normalized_case_study,
        "problem_statement": normalized_problem_statement,
        "solution_description": normalized_solution_description,
        "tech_stack": list(dict.fromkeys(tech_stack or [])),
        "supposed_deadline": supposed_deadline,
        "screenshots": [],
        "initialization_guide": "\n".join(line for line in guide_lines if line),
        "industry_context": normalized_case_study,
        "scope": normalized_solution_description,
        "prerequisites": prerequisites or [],
        "deliverables": deliverables or [],
        "requirements": requirements or [],
        "requirements_text": requirements_text,
        "advanced_extensions": None,
        "evaluation_criteria": evaluation_criteria,
        "is_published": True,
    }


async def sync_regular_project_from_payload(
    db: AsyncSession,
    *,
    project_payload: dict[str, Any],
    repo_url: str | None = None,
) -> Project:
    base_filters = [
        Project.domain_id == project_payload["domain_id"],
        Project.created_by_id.is_(None),
        Project.deleted_at.is_(None),
    ]

    existing = None
    if repo_url:
        result = await db.execute(
            select(Project).where(
                *base_filters,
                Project.initialization_guide.is_not(None),
                Project.initialization_guide.ilike(f"%{repo_url}%"),
            )
        )
        existing = result.scalar_one_or_none()

    if existing is None:
        result = await db.execute(
            select(Project).where(
                *base_filters,
                or_(
                    Project.title == project_payload["title"],
                    Project.problem_statement == project_payload["problem_statement"],
                ),
            )
        )
        existing = result.scalar_one_or_none()

    if existing:
        for field_name, value in project_payload.items():
            setattr(existing, field_name, value)
        return existing

    project = Project(**project_payload)
    db.add(project)
    await db.flush()
    return project


async def sync_regular_project_from_github_project(db: AsyncSession, github_project: GitHubProject) -> Project:
    payload = _build_regular_project_payload(
        title=github_project.title,
        domain_id=github_project.domain_id,
        sub_domain=github_project.sub_domain,
        difficulty=github_project.difficulty,
        min_time=github_project.estimated_min_time,
        max_time=github_project.estimated_max_time,
        tech_stack=github_project.tech_stack,
        case_study=github_project.case_study or github_project.introduction,
        problem_statement=github_project.problem_statement or github_project.description,
        solution_description=github_project.solution_description or github_project.implementation,
        supposed_deadline=github_project.supposed_deadline,
        prerequisites=github_project.prerequisites,
        deliverables=github_project.deliverables,
        requirements=github_project.requirements,
        requirements_text=github_project.requirements_text,
        evaluation_criteria=github_project.evaluation_criteria,
        repo_url=github_project.repo_url,
        live_url=github_project.live_url,
        download_url=github_project.download_url,
        skill_focus=(github_project.technical_skills or github_project.tech_stack),
    )
    return await sync_regular_project_from_payload(db, project_payload=payload, repo_url=github_project.repo_url)


async def sync_regular_project_from_candidate(db: AsyncSession, candidate: ScrapeCandidate, domain_id: str) -> Project:
    payload = _build_regular_project_payload(
        title=candidate.title,
        domain_id=domain_id,
        sub_domain=candidate.sub_domain,
        difficulty=candidate.difficulty,
        min_time=candidate.estimated_min_time,
        max_time=candidate.estimated_max_time,
        tech_stack=candidate.tech_stack,
        case_study=candidate.case_study or candidate.introduction,
        problem_statement=candidate.problem_statement or candidate.description,
        solution_description=candidate.solution_description or candidate.implementation,
        supposed_deadline=candidate.supposed_deadline,
        prerequisites=candidate.prerequisites,
        deliverables=candidate.deliverables,
        requirements=candidate.requirements,
        requirements_text=candidate.requirements_text,
        evaluation_criteria=candidate.evaluation_criteria,
        repo_url=candidate.repo_url,
        live_url=candidate.live_url,
        download_url=candidate.download_url,
        skill_focus=candidate.technical_skills or candidate.tech_stack,
    )
    return await sync_regular_project_from_payload(db, project_payload=payload, repo_url=candidate.repo_url)


def assign_balanced_difficulties(candidates: list[ScrapeCandidate]) -> list[ScrapeCandidate]:
    if len(candidates) < 3:
        return candidates

    ordered = sorted(
        candidates,
        key=lambda candidate: (
            candidate.estimated_max_time,
            candidate.estimated_min_time,
            candidate.stars,
            candidate.score,
        ),
    )
    total = len(ordered)

    easy_cut = max(1, round(total * DIFFICULTY_RATIOS["EASY"]))
    medium_cut = max(easy_cut + 1, round(total * (DIFFICULTY_RATIOS["EASY"] + DIFFICULTY_RATIOS["MEDIUM"])))
    medium_cut = min(medium_cut, total - 1)

    for index, candidate in enumerate(ordered):
        if index < easy_cut:
            candidate.difficulty = "EASY"
        elif index < medium_cut:
            candidate.difficulty = "MEDIUM"
        else:
            candidate.difficulty = "ADVANCED"

    return ordered


def compute_bucket_quotas(total: int) -> dict[str, int]:
    quotas = {difficulty: int(total * ratio) for difficulty, ratio in DIFFICULTY_RATIOS.items()}
    allocated = sum(quotas.values())
    order = ["MEDIUM", "ADVANCED", "EASY"]
    index = 0
    while allocated < total:
        quotas[order[index % len(order)]] += 1
        allocated += 1
        index += 1
    return quotas


def select_balanced_candidates(candidates: list[ScrapeCandidate], target_count: int) -> list[ScrapeCandidate]:
    rebalanced = assign_balanced_difficulties(candidates)
    buckets: dict[str, list[ScrapeCandidate]] = {"EASY": [], "MEDIUM": [], "ADVANCED": []}
    for candidate in rebalanced:
        bucket = candidate.difficulty if candidate.difficulty in buckets else "ADVANCED"
        buckets[bucket].append(candidate)

    for bucket in buckets.values():
        bucket.sort(key=lambda item: item.score, reverse=True)

    quotas = compute_bucket_quotas(min(target_count, len(candidates)))
    selected: list[ScrapeCandidate] = []
    leftovers: list[ScrapeCandidate] = []

    for difficulty, quota in quotas.items():
        bucket = buckets[difficulty]
        selected.extend(bucket[:quota])
        leftovers.extend(bucket[quota:])

    if len(selected) < min(target_count, len(candidates)):
        leftovers.sort(key=lambda item: item.score, reverse=True)
        missing = min(target_count, len(candidates)) - len(selected)
        selected.extend(leftovers[:missing])

    return sorted(selected, key=lambda item: item.score, reverse=True)


def repo_is_candidate(repo: dict[str, Any], domain_keywords: set[str], min_stars: int, require_demo: bool) -> bool:
    if repo.get("fork") or repo.get("archived") or repo.get("disabled") or repo.get("is_template"):
        return False

    if (repo.get("stargazers_count") or 0) < min_stars:
        return False

    updated_at = repo.get("updated_at")
    if updated_at:
        updated_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        if updated_dt < datetime.now(timezone.utc) - timedelta(days=365 * 3):
            return False

    search_text = normalize_text(repo.get("name"), repo.get("description"), " ".join(repo.get("topics") or []))
    repo_name = normalize_text(repo.get("name"), repo.get("full_name"))
    if not search_text or len(search_text) < 25:
        return False

    if normalize_base_name(repo.get("name") or "") in FOUNDATIONAL_REPO_NAMES | NON_PROJECT_REPO_NAMES:
        return False

    if any(keyword in search_text for keyword in EXCLUSION_KEYWORDS):
        return False

    if any(keyword in search_text for keyword in LIBRARY_SIGNAL_KEYWORDS):
        return False

    if any(flag in repo_name for flag in ["awesome", "free-programming-books", "programming-books", "tutorial", "course"]):
        return False

    if not any(keyword in search_text for keyword in domain_keywords):
        return False

    if not any(keyword in search_text for keyword in APP_KEYWORDS | LIVE_HINT_KEYWORDS):
        return False

    if require_demo and not repo.get("homepage"):
        return False

    return True


def score_repo(repo: dict[str, Any], domain_keywords: set[str], has_readme: bool, has_demo: bool) -> float:
    search_text = normalize_text(repo.get("name"), repo.get("description"), " ".join(repo.get("topics") or []))
    keyword_hits = sum(1 for keyword in domain_keywords if keyword in search_text)
    score = min((repo.get("stargazers_count") or 0) / 100, 60)
    score += min((repo.get("forks_count") or 0) / 50, 20)
    score += keyword_hits * 6
    score += 8 if has_readme else 0
    score += 8 if has_demo else 0
    score += 4 if repo.get("language") else 0
    return round(score, 2)


async def github_get(client: httpx.AsyncClient, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
    max_attempts = 5
    response: httpx.Response | None = None
    for attempt in range(1, max_attempts + 1):
        response = await client.get(path, params=params, headers=build_headers())

        if response.status_code in {403, 429}:
            body_text = response.text.lower()
            is_rate_limited = (
                response.status_code == 429
                or response.headers.get("X-RateLimit-Remaining") == "0"
                or "rate limit" in body_text
                or "secondary rate limit" in body_text
                or "abuse detection" in body_text
            )

            if is_rate_limited and attempt < max_attempts:
                delay = _compute_github_retry_delay(response)
                delay = min(max(delay, 1.0), 300.0)
                print(f"[GitHub] Rate limit hit for {path}. Waiting {math.ceil(delay)}s before retry {attempt}/{max_attempts}...")
                await asyncio.sleep(delay)
                continue

        response.raise_for_status()
        return response

    if response is None:
        raise RuntimeError(f"GitHub request did not execute for {path}")

    response.raise_for_status()
    return response


async def fetch_search_page(client: httpx.AsyncClient, query: str, page: int, per_page: int) -> list[dict[str, Any]]:
    if per_page <= 0:
        return []

    max_allowed_page = max(1, math.ceil(MAX_GITHUB_SEARCH_RESULTS / per_page))
    if page > max_allowed_page:
        return []

    try:
        response = await github_get(
            client,
            "https://api.github.com/search/repositories",
            {"q": query, "sort": "stars", "order": "desc", "page": page, "per_page": per_page},
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 422:
            print(f"[GitHub] Search page {page} exceeded GitHub search result limits for query: {query}")
            return []
        raise

    return response.json().get("items", [])


async def fetch_readme(client: httpx.AsyncClient, owner: str, repo: str) -> str:
    try:
        response = await github_get(client, f"https://api.github.com/repos/{owner}/{repo}/readme")
    except httpx.HTTPStatusError:
        return ""

    payload = response.json()
    content = payload.get("content")
    if not content:
        return ""

    try:
        return base64.b64decode(content).decode("utf-8", errors="ignore")
    except Exception:
        return ""


async def fetch_languages(client: httpx.AsyncClient, owner: str, repo: str) -> list[str]:
    try:
        response = await github_get(client, f"https://api.github.com/repos/{owner}/{repo}/languages")
    except httpx.HTTPStatusError:
        return []

    language_map = response.json()
    return list(language_map.keys())[:8]


def extract_links(readme_text: str) -> list[str]:
    return re.findall(r"https?://[^\s\)\]>]+", readme_text)


def pick_live_url(repo: dict[str, Any], readme_text: str) -> str | None:
    homepage = repo.get("homepage")
    if homepage and homepage.startswith("http") and "github.com" not in homepage:
        return homepage

    for url in extract_links(readme_text):
        if any(host in url for host in LIVE_HOST_KEYWORDS):
            return url
    return None


def extract_requirements(domain_name: str, repo: dict[str, Any], readme_text: str, languages: list[str]) -> tuple[list[str], list[str], list[str], str | None, str | None]:
    topics = [topic for topic in (repo.get("topics") or []) if topic]
    lowered_readme = readme_text.lower()
    tech_stack = list(dict.fromkeys(languages + topics[:8]))
    technical_skills_source = tech_stack + ([repo["language"]] if repo.get("language") else [])
    technical_skills = list(dict.fromkeys(technical_skills_source))
    tools_used = [tool for tool in ["GitHub", "VS Code", "PostgreSQL", "Docker", "Jupyter"] if tool.lower() in lowered_readme or tool == "GitHub"]
    requirements_text = "Validate setup, core workflow, and deployment or execution steps before publishing the project brief."
    evaluation_criteria = "Assess correctness, code quality, documentation, usability, and domain relevance."
    return tech_stack, technical_skills, tools_used, requirements_text, evaluation_criteria


async def enrich_candidate(
    repo: dict[str, Any],
    domain_slug: str,
    domain_name: str,
    domain_keywords: set[str],
    client: httpx.AsyncClient,
    use_ai: bool,
) -> ScrapeCandidate:
    owner = repo["owner"]["login"]
    repo_name = repo["name"]
    readme_text = await fetch_readme(client, owner, repo_name)
    languages = await fetch_languages(client, owner, repo_name)
    live_url = pick_live_url(repo, readme_text)
    has_demo = live_url is not None
    score = score_repo(repo, domain_keywords, bool(readme_text), has_demo)
    difficulty = difficulty_from_repo(repo.get("stargazers_count") or 0, repo.get("size") or 0, repo.get("topics") or [])
    min_time, max_time, deadline = estimate_duration(repo.get("stargazers_count") or 0, repo.get("size") or 0, readme_text)

    if is_foundational_or_library_repo(repo, readme_text, live_url):
        raise ValueError("Repository looks like a library/framework, not a live student project")

    if not is_student_buildable_repo(repo, readme_text, live_url, min_time, max_time):
        raise ValueError("Repository is not a live student-buildable project")

    if not has_end_user_project_signals(repo, readme_text, live_url):
        raise ValueError("Repository does not look like an end-user project")

    tech_stack, technical_skills, tools_used, requirements_text, evaluation_criteria = extract_requirements(
        domain_name, repo, readme_text, languages
    )

    introduction = (repo.get("description") or "").strip() or "No repository description was provided."
    implementation = " ".join(
        part.strip() for part in [
            f"Repository topics: {', '.join((repo.get('topics') or [])[:8])}." if repo.get("topics") else "",
            f"Primary language: {repo.get('language')}." if repo.get("language") else "",
            "Review the README and source layout to understand the system workflow, setup sequence, and deployment path.",
        ] if part.strip()
    )
    prerequisites = [
        f"Working knowledge of {repo.get('language') or domain_name} fundamentals.",
        "Ability to clone, configure, and run a GitHub repository locally.",
        "Comfort reading README setup and architecture notes.",
    ]
    deliverables = [
        "A working local setup with documented installation steps.",
        "A short implementation note explaining the system flow and core modules.",
        "A validated feature demo or execution proof with screenshots.",
    ]
    sub_domain = next(iter(repo.get("topics") or []), None)
    case_study = None
    problem_statement = None
    solution_description = None

    if use_ai:
        ai_payload = await generate_project_details(
            title=repo_name,
            description=introduction,
            language=repo.get("language"),
            topics=repo.get("topics") or [],
        )
        if ai_payload:
            case_study = ai_payload.get("case_study")
            problem_statement = ai_payload.get("problem_statement")
            solution_description = ai_payload.get("solution_description")
            prerequisites = ai_payload.get("prerequisites") or prerequisites
            deliverables = ai_payload.get("deliverables") or deliverables
            sub_domain = ai_payload.get("sub_domain") or sub_domain
            difficulty = ai_payload.get("difficulty") or difficulty
            min_time = int(ai_payload.get("estimated_min_time") or min_time)
            max_time = int(ai_payload.get("estimated_max_time") or max_time)

    download_url = f"https://github.com/{owner}/{repo_name}/archive/refs/heads/{repo.get('default_branch', 'main')}.zip"
    candidate = ScrapeCandidate(
        domain_slug=domain_slug,
        domain_name=domain_name,
        title=repo_name,
        slug=slugify_repo(repo["full_name"]),
        description=introduction,
        repo_url=repo["html_url"],
        repo_owner=owner,
        repo_name=repo_name,
        default_branch=repo.get("default_branch", "main"),
        download_url=download_url,
        live_url=live_url,
        stars=repo.get("stargazers_count") or 0,
        forks=repo.get("forks_count") or 0,
        language=repo.get("language"),
        tech_stack=tech_stack,
        difficulty=difficulty,
        topics=repo.get("topics") or [],
        project_type="PROJECT",
        author=owner,
        introduction=introduction,
        implementation=implementation,
        technical_skills=technical_skills,
        tools_used=tools_used,
        concepts_used=[concept for concept in [domain_name, "GitHub Workflow", "System Design"] if concept],
        sub_domain=sub_domain,
        case_study=case_study,
        problem_statement=problem_statement,
        solution_description=solution_description,
        prerequisites=prerequisites,
        deliverables=deliverables,
        supposed_deadline=deadline,
        requirements=[
            "Document setup, architecture, and testing steps.",
            "Explain how the project solves a real user or business problem.",
            "Provide screenshots or execution evidence during curation.",
        ],
        requirements_text=requirements_text,
        evaluation_criteria=evaluation_criteria,
        estimated_min_time=min_time,
        estimated_max_time=max_time,
        score=score,
        readme_excerpt=readme_text[:1500] if readme_text else None,
    )
    return candidate


async def scrape_domain_candidates(
    domain_slug: str,
    target_count: int = 100,
    per_page: int = 30,
    max_pages_per_query: int = 8,
    min_stars: int = 60,
    require_demo: bool = False,
    use_ai: bool = False,
    progress_callback: ProgressCallback | None = None,
) -> list[ScrapeCandidate]:
    config = DOMAIN_CONFIGS[domain_slug]
    collected: list[ScrapeCandidate] = []
    seen_repos: set[str] = set()
    seen_names: set[str] = set()
    discovery_target = max(target_count * 8, target_count + 100)
    effective_max_pages = min(max_pages_per_query, max(1, math.ceil(MAX_GITHUB_SEARCH_RESULTS / per_page)))
    stats = {
        "queries_total": len(config["queries"]),
        "queries_done": 0,
        "pages_total": len(config["queries"]) * effective_max_pages,
        "pages_done": 0,
        "scanned": 0,
        "accepted": 0,
        "duplicate": 0,
        "prefilter_rejected": 0,
        "enrichment_rejected": 0,
        "missing_live_demo": 0,
        "target": target_count,
        "discovery_target": discovery_target,
        "domain_name": config["name"],
    }

    if progress_callback:
        progress_callback("domain_start", dict(stats))

    async with httpx.AsyncClient(timeout=20.0) as client:
        for query_index, query in enumerate(config["queries"], start=1):
            if progress_callback:
                progress_callback(
                    "query_start",
                    {
                        **stats,
                        "query": query,
                        "query_index": query_index,
                    },
                )

            for page in range(1, effective_max_pages + 1):
                repos = await fetch_search_page(client, query, page, per_page)
                stats["pages_done"] += 1

                if progress_callback:
                    progress_callback(
                        "page_fetched",
                        {
                            **stats,
                            "query": query,
                            "query_index": query_index,
                            "page": page,
                            "fetched": len(repos),
                        },
                    )

                if not repos:
                    break

                for repo in repos:
                    stats["scanned"] += 1
                    full_name = repo["full_name"]
                    base_name = normalize_base_name(repo["name"])
                    if full_name in seen_repos or base_name in seen_names:
                        stats["duplicate"] += 1
                        continue
                    if not repo_is_candidate(repo, config["keywords"], min_stars, require_demo):
                        stats["prefilter_rejected"] += 1
                        continue

                    try:
                        candidate = await enrich_candidate(
                            repo=repo,
                            domain_slug=domain_slug,
                            domain_name=config["name"],
                            domain_keywords=config["keywords"],
                            client=client,
                            use_ai=use_ai,
                        )
                    except ValueError:
                        stats["enrichment_rejected"] += 1
                        continue

                    if not candidate.live_url:
                        stats["missing_live_demo"] += 1
                        continue

                    seen_repos.add(full_name)
                    seen_names.add(base_name)
                    collected.append(candidate)
                    stats["accepted"] += 1

                    if progress_callback and (
                        stats["accepted"] <= 5
                        or stats["accepted"] % 10 == 0
                        or len(collected) >= discovery_target
                    ):
                        progress_callback(
                            "candidate_accepted",
                            {
                                **stats,
                                "repo": full_name,
                                "score": candidate.score,
                            },
                        )

                    if len(collected) >= discovery_target:
                        break

                if len(collected) >= discovery_target:
                    break
                await asyncio.sleep(0.25)
            if len(collected) >= discovery_target:
                break
            stats["queries_done"] = query_index

        stats["queries_done"] = len(config["queries"])

    selected = select_balanced_candidates(collected, target_count)

    if progress_callback:
        progress_callback(
            "domain_complete",
            {
                **stats,
                "selected": len(selected),
                "collected": len(collected),
            },
        )

    return selected


async def ensure_domain(db: AsyncSession, domain_slug: str) -> Domain:
    config = DOMAIN_CONFIGS[domain_slug]
    result = await db.execute(select(Domain).where(Domain.slug == domain_slug))
    domain = result.scalar_one_or_none()
    if domain:
        return domain

    domain = Domain(name=config["name"], slug=domain_slug, description=f"Curated {config['name']} projects")
    db.add(domain)
    await db.flush()
    return domain


async def upsert_candidates(db: AsyncSession, candidates: list[ScrapeCandidate]) -> dict[str, int]:
    inserted = 0
    updated = 0
    synced_projects = 0
    by_domain: dict[str, Domain] = {}

    for candidate in candidates:
        domain = by_domain.get(candidate.domain_slug)
        if domain is None:
            domain = await ensure_domain(db, candidate.domain_slug)
            by_domain[candidate.domain_slug] = domain

        result = await db.execute(select(GitHubProject).where(GitHubProject.slug == candidate.slug))
        existing = result.scalar_one_or_none()

        payload = candidate.to_record()
        payload.pop("domain_slug", None)
        payload.pop("domain_name", None)
        payload.pop("score", None)
        payload.pop("readme_excerpt", None)

        if existing:
            for field_name, value in payload.items():
                setattr(existing, field_name, value)
            existing.domain_id = domain.id
            updated += 1
            await sync_regular_project_from_candidate(db, candidate, domain.id)
            synced_projects += 1
        else:
            project = GitHubProject(**payload, domain_id=domain.id, last_updated=datetime.now(timezone.utc))
            db.add(project)
            inserted += 1
            await db.flush()
            await sync_regular_project_from_candidate(db, candidate, domain.id)
            synced_projects += 1

    await db.commit()
    return {"inserted": inserted, "updated": updated, "synced_projects": synced_projects}


def save_candidates_to_file(candidates: list[ScrapeCandidate], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps([candidate.to_record() for candidate in candidates], indent=2), encoding="utf-8")
    return output_path