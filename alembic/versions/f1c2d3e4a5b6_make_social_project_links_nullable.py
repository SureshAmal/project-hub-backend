"""make social project links nullable

Revision ID: f1c2d3e4a5b6
Revises: e3a4c72bb3e1
Create Date: 2026-03-08 00:35:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1c2d3e4a5b6'
down_revision: Union[str, Sequence[str], None] = 'e3a4c72bb3e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _has_column(inspector, table_name: str, column_name: str) -> bool:
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_table(inspector, 'comments') and _has_column(inspector, 'comments', 'projectId'):
        op.alter_column(
            'comments',
            'projectId',
            existing_type=sa.String(),
            nullable=True,
        )

    if _has_table(inspector, 'likes') and _has_column(inspector, 'likes', 'projectId'):
        op.alter_column(
            'likes',
            'projectId',
            existing_type=sa.String(),
            nullable=True,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_table(inspector, 'likes') and _has_column(inspector, 'likes', 'projectId'):
        op.alter_column(
            'likes',
            'projectId',
            existing_type=sa.String(),
            nullable=False,
        )

    if _has_table(inspector, 'comments') and _has_column(inspector, 'comments', 'projectId'):
        op.alter_column(
            'comments',
            'projectId',
            existing_type=sa.String(),
            nullable=False,
        )
