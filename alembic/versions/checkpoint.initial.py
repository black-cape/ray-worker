"""empty message

Revision ID: 7a44763368c2
Revises:
Create Date: 2024-01-19 20:44:53.507898

"""
from alembic import op
import sqlalchemy as sa
import sqlmodel  # New


# revision identifiers, used by Alembic.
revision = "7a44763368c2"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "filestatusrecord",
        sa.Column("bucket_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("file_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column(
            "original_filename", sqlmodel.sql.sqltypes.AutoString(), nullable=False
        ),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("event_name", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("size", sa.Integer(), nullable=True),
        sa.Column("content_type", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("minio_metadata", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("user_dn", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("id", sqlmodel.sql.sqltypes.GUID(), nullable=False),
        sa.Column("created_dt", sa.DateTime(), nullable=False),
        sa.Column("updated_dt", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("filestatusrecord")
    # ### end Alembic commands ###
