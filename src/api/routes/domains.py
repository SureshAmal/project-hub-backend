from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List

from src.core.database import get_db
from src.models.project import Domain
from src.schemas.project import DomainResponse

router = APIRouter()

SUPPORTED_DOMAIN_SLUGS = [
    "web-development",
    "artificial-intelligence",
    "machine-learning",
    "data-science",
    "cybersecurity",
]

@router.get("", response_model=List[DomainResponse])
async def get_domains(db: AsyncSession = Depends(get_db)):
    """
    Get a list of all professional domains.
    """
    result = await db.execute(
        select(Domain)
        .where(Domain.slug.in_(SUPPORTED_DOMAIN_SLUGS))
        .order_by(Domain.name.asc())
    )
    domains = result.scalars().all()
    return domains

@router.get("/slug/{domain_slug}", response_model=DomainResponse)
async def get_domain_by_slug(domain_slug: str, db: AsyncSession = Depends(get_db)):
    """
    Get a single domain by its slug.
    """
    if domain_slug not in SUPPORTED_DOMAIN_SLUGS:
        raise HTTPException(status_code=404, detail="Domain not found")

    result = await db.execute(select(Domain).where(Domain.slug == domain_slug))
    domain = result.scalars().first()
    
    if not domain:
        raise HTTPException(status_code=404, detail="Domain not found")
        
    return domain
