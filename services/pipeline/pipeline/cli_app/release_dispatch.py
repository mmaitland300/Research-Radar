from __future__ import annotations

import json
import sys
from dataclasses import asdict

from pipeline.cli_app.dispatch_common import DispatchContext
from pipeline.public_release import PublicReleasePromotionError, promote_public_release


def handle_release_commands(args, ctx: DispatchContext) -> bool:
    if args.command != "public-release-promote":
        return False

    _ = ctx
    try:
        result = promote_public_release(
            ranking_run_id=args.ranking_run_id,
            database_url=args.database_url,
            dry_run=bool(args.dry_run),
        )
    except PublicReleasePromotionError as exc:
        print(f"public-release-promote: {exc}", file=sys.stderr)
        raise SystemExit(exc.code) from exc
    print(json.dumps(asdict(result), sort_keys=True, default=str))
    return True
