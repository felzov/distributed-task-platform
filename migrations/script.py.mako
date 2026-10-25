"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

Written by hand. Use literal values (not imports from task_platform) so this revision never changes
when the application code does, and keep downgrade() working.
"""

from collections.abc import Sequence

from alembic import op

revision: str = ${repr(up_revision)}
down_revision: str | None = ${repr(down_revision)}
branch_labels: str | Sequence[str] | None = ${repr(branch_labels)}
depends_on: str | Sequence[str] | None = ${repr(depends_on)}


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
