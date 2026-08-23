"""Add the append-only public release promotion log.

The table starts empty. A release becomes active only after an operator
explicitly promotes one completed, succeeded ranking run. The highest
promotion_id is the active public release; repeated ranking_run_id values are
allowed so rollback is another append rather than a history rewrite.

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-23
"""

from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE public_release_promotions (
            promotion_id BIGSERIAL PRIMARY KEY,
            ranking_run_id TEXT NOT NULL,
            promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            promoted_by TEXT NOT NULL,
            note TEXT,
            CONSTRAINT fk_public_release_promotions_ranking_run
                FOREIGN KEY (ranking_run_id)
                REFERENCES ranking_runs (ranking_run_id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            CONSTRAINT public_release_promotions_ranking_run_id_nonblank
                CHECK (btrim(ranking_run_id) <> ''),
            CONSTRAINT public_release_promotions_promoted_by_nonblank
                CHECK (btrim(promoted_by) <> '')
        )
        """
    )
    op.execute(
        """
        CREATE INDEX idx_public_release_promotions_run_history
            ON public_release_promotions (ranking_run_id, promotion_id DESC)
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_public_release_promotion_run()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            PERFORM 1
            FROM ranking_runs
            WHERE ranking_run_id = NEW.ranking_run_id
              AND status = 'succeeded'
              AND finished_at IS NOT NULL
              AND error_message IS NULL
            FOR SHARE;

            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'ranking run ' || quote_nullable(NEW.ranking_run_id)
                        || ' is not a completed, error-free succeeded run';
            END IF;

            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_public_release_promotions_validate_run
        BEFORE INSERT ON public_release_promotions
        FOR EACH ROW
        EXECUTE FUNCTION validate_public_release_promotion_run()
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_public_release_promotion_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'public_release_promotions is append-only; '
                    || TG_OP || ' is not allowed';
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_public_release_promotions_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON public_release_promotions
        FOR EACH STATEMENT
        EXECUTE FUNCTION reject_public_release_promotion_mutation()
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_public_release_promotions_append_only
            ON public_release_promotions
        """
    )
    op.execute(
        """
        DROP TRIGGER IF EXISTS trg_public_release_promotions_validate_run
            ON public_release_promotions
        """
    )
    op.execute("DROP TABLE IF EXISTS public_release_promotions")
    op.execute("DROP FUNCTION IF EXISTS reject_public_release_promotion_mutation()")
    op.execute("DROP FUNCTION IF EXISTS validate_public_release_promotion_run()")
