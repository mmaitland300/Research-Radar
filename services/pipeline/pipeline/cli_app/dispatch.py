from __future__ import annotations

from pipeline.cli_app import dispatch_common
from pipeline.cli_app.bridge_dispatch import handle_bridge_commands
from pipeline.cli_app.core_dispatch import handle_core_commands
from pipeline.cli_app.corpus_dispatch import handle_corpus_commands
from pipeline.cli_app.dispatch_common import DispatchContext
from pipeline.cli_app.ml_legacy_dispatch import handle_ml_legacy_commands
from pipeline.cli_app.release_dispatch import handle_release_commands
from pipeline.cli_app.review_dispatch import handle_review_commands


_HANDLERS = (
    handle_ml_legacy_commands,
    handle_release_commands,
    handle_corpus_commands,
    handle_review_commands,
    handle_bridge_commands,
    handle_core_commands,
)


def dispatch_command(args, parser, *, psycopg_module, compat_module=None) -> None:
    ctx = DispatchContext(
        parser=parser,
        psycopg_module=psycopg_module,
        compat=compat_module or dispatch_common,
    )
    for handler in _HANDLERS:
        if handler(args, ctx):
            return
