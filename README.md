# Project Hub API (FastAPI Backend)

This is the Python backend for the Project Hub platform, built on FastAPI. It acts as the high-performance API layer handling project management, GitHub repository integration, learning paths, gamification, and user tracking.

## 🏗️ Architecture Stack
- **Framework**: [FastAPI](https://fastapi.tiangolo.com/) (Python 3.12+)
- **Database**: PostgreSQL (shared with the Next.js frontend)
- **ORM**: SQLAlchemy 2.0 (using asynchronous `asyncpg` bindings)
- **Auth**: Custom JWT integration compatible with NextAuth.js
- **Package Manager**: [uv](https://github.com/astral-sh/uv)

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.12 or newer.
- [uv](https://github.com/astral-sh/uv) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`).
- A running instance of PostgreSQL (which should ideally be configured via the Next.js `frontend` `.env.local`).

### 2. Environment Configuration
Copy the `.env.example` file (or create an `.env`) in the `backend-python` root.
`DATABASE_URL` is used by the FastAPI runtime and is normalized to `asyncpg` automatically. `ALEMBIC_DATABASE_URL` is optional and can be set when migrations need an explicit sync URL.

Example `.env`:
```env
# Ensure the Secret Key perfectly matches the Next.js NEXTAUTH_SECRET to decode JWTs
SECRET_KEY="your-nextauth-secret-key"
DATABASE_URL="postgresql://postgres:password@localhost:5432/projecthub1"
ALEMBIC_DATABASE_URL="postgresql+psycopg2://postgres:password@localhost:5432/projecthub1"
BACKEND_CORS_ORIGINS='["http://localhost:3000"]'
```

Neon example:

```env
DATABASE_URL="postgresql://USER:PASSWORD@HOST/neondb?sslmode=require&channel_binding=require"
ALEMBIC_DATABASE_URL="postgresql+psycopg2://USER:PASSWORD@HOST/neondb?sslmode=require"
```

### 3. Installation
Install all dependencies in a lightning-fast virtual environment using `uv`:
```bash
uv sync   # Syncs dependencies using uv.lock
```

### 4. Running the Development Server
Since the backend uses ASGI, you initiate the application using `uvicorn`:

```bash
uv run uvicorn src.main:app --reload
```
You should now be able to visit http://localhost:8000/docs to view the interactive Swagger/OpenAPI documentation.

---

## 🗄️ Database Management
> **Note on Migrations**: The database schema is strictly owned and migrated by Prisma in the `frontend` folder. Do not run `alembic upgrade head` to apply schema changes unless explicitly separating the backends.

If you are developing a new feature:
1. First, modify the `frontend/prisma/schema.prisma` file.
2. Run `npx prisma db push` inside the frontend directory.
3. Update the equivalent Python SQLAlchemy definitions inside `backend-python/src/models/` to match. 

## 🧪 Testing
The test suite utilizes `pytest` with `pytest-asyncio` and `httpx`.
To execute the tests:
```bash
uv run pytest tests/
```

## 🔎 Project Collection Scraper
The Python backend now includes a GitHub project discovery scraper that is aligned to the platform's five collection domains:

- Web Development
- Artificial Intelligence
- Machine Learning
- Data Science
- Cybersecurity

Run it from the `backend-python` folder:

```bash
uv run python scripts/find_projects.py --domain all --target 20 --output output/candidates.json
```

Useful flags:

- `--require-demo` keeps only repositories with a detected live demo/homepage
- `--use-ai` fills case-study style fields using the configured AI service
- `--upsert-db` writes the curated results into the shared PostgreSQL database

Example:

```bash
uv run python scripts/find_projects.py --domain web-development --target 50 --require-demo --upsert-db
```

## 🚂 Railway Deployment With Docker

This backend can be deployed to Railway using the Docker configuration in [backend-python/Dockerfile](Dockerfile) and [backend-python/railway.json](railway.json).

### Files
- [Dockerfile](Dockerfile) - container build for the FastAPI app
- [railway.json](railway.json) - Railway build and deploy settings
- [.dockerignore](.dockerignore) - keeps the image small

### Railway setup
1. Create a new Railway service from this repository.
2. Set the service root directory to [backend-python](.)
3. Railway will detect [railway.json](railway.json) and build using Docker.
4. Add the required environment variables:

```env
DATABASE_URL=postgresql://...
ALEMBIC_DATABASE_URL=postgresql+psycopg2://...
SECRET_KEY=your-nextauth-secret
BACKEND_CORS_ORIGINS=["https://your-frontend-domain.com"]
GITHUB_TOKEN=optional
CEREBRAS_API_KEY=optional
GEMINI_API_KEY=optional
```

### Local Docker test

```bash
docker build -t project-hub-backend-python .
docker run --rm -p 8000:8000 \
	-e DATABASE_URL="postgresql://..." \
	-e ALEMBIC_DATABASE_URL="postgresql+psycopg2://..." \
	-e SECRET_KEY="your-secret" \
	-e BACKEND_CORS_ORIGINS='["http://localhost:3000"]' \
	project-hub-backend-python
```

Railway injects `PORT` automatically, and the container command binds `uvicorn` to `0.0.0.0` on that port.
