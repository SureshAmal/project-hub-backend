"""add github social support and regular project requirements

Revision ID: e3a4c72bb3e1
Revises: bf8d5f5801e1
Create Date: 2026-03-08 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'e3a4c72bb3e1'
down_revision: Union[str, Sequence[str], None] = 'bf8d5f5801e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _has_column(inspector, table_name: str, column_name: str) -> bool:
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def _has_index(inspector, table_name: str, index_name: str) -> bool:
    return any(index["name"] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_table(inspector, 'projects'):
        if not _has_column(inspector, 'projects', 'requirements'):
            op.add_column('projects', sa.Column('requirements', postgresql.ARRAY(sa.String()), nullable=True, server_default='{}'))
        if not _has_column(inspector, 'projects', 'requirementsText'):
            op.add_column('projects', sa.Column('requirementsText', sa.Text(), nullable=True))

    if _has_table(inspector, 'comments') and not _has_column(inspector, 'comments', 'githubProjectId'):
        op.add_column('comments', sa.Column('githubProjectId', sa.String(), nullable=True))
        op.create_foreign_key(
            'fk_comments_githubProjectId_github_projects',
            'comments',
            'github_projects',
            ['githubProjectId'],
            ['id'],
            ondelete='CASCADE',
        )
        if not _has_index(inspector, 'comments', 'ix_comments_githubProjectId'):
            op.create_index('ix_comments_githubProjectId', 'comments', ['githubProjectId'], unique=False)

    if _has_table(inspector, 'likes') and not _has_column(inspector, 'likes', 'githubProjectId'):
        op.add_column('likes', sa.Column('githubProjectId', sa.String(), nullable=True))
        op.create_foreign_key(
            'fk_likes_githubProjectId_github_projects',
            'likes',
            'github_projects',
            ['githubProjectId'],
            ['id'],
            ondelete='CASCADE',
        )
        if not _has_index(inspector, 'likes', 'ix_likes_githubProjectId'):
            op.create_index('ix_likes_githubProjectId', 'likes', ['githubProjectId'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _has_table(inspector, 'likes') and _has_column(inspector, 'likes', 'githubProjectId'):
        if _has_index(inspector, 'likes', 'ix_likes_githubProjectId'):
            op.drop_index('ix_likes_githubProjectId', table_name='likes')
        try:
            op.drop_constraint('fk_likes_githubProjectId_github_projects', 'likes', type_='foreignkey')
        except Exception:
            pass
        op.drop_column('likes', 'githubProjectId')

    if _has_table(inspector, 'comments') and _has_column(inspector, 'comments', 'githubProjectId'):
        if _has_index(inspector, 'comments', 'ix_comments_githubProjectId'):
            op.drop_index('ix_comments_githubProjectId', table_name='comments')
        try:
            op.drop_constraint('fk_comments_githubProjectId_github_projects', 'comments', type_='foreignkey')
        except Exception:
            pass
        op.drop_column('comments', 'githubProjectId')

    if _has_table(inspector, 'projects'):
        if _has_column(inspector, 'projects', 'requirementsText'):
            op.drop_column('projects', 'requirementsText')
        if _has_column(inspector, 'projects', 'requirements'):
            op.drop_column('projects', 'requirements')
