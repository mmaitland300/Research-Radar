from __future__ import annotations

from pipeline.cli_app import dispatch_common
from pipeline.cli_app.bridge_dispatch import handle_bridge_commands
from pipeline.cli_app.core_dispatch import handle_core_commands
from pipeline.cli_app.corpus_dispatch import handle_corpus_commands
from pipeline.cli_app.dispatch_common import DispatchContext
from pipeline.cli_app.ml_bridge_dispatch import handle_ml_bridge_commands
from pipeline.cli_app.ml_fresh_dispatch import handle_ml_fresh_commands
from pipeline.cli_app.ml_hybrid_dispatch import handle_ml_hybrid_commands
from pipeline.cli_app.ml_label_dispatch import handle_ml_label_commands
from pipeline.cli_app.ml_review_dispatch import handle_ml_review_commands
from pipeline.cli_app.ml_shadow_dispatch import handle_ml_shadow_commands
from pipeline.cli_app.ml_text_dispatch import handle_ml_text_commands
from pipeline.cli_app.review_dispatch import handle_review_commands


_HANDLERS = (
    handle_ml_review_commands,
    handle_ml_bridge_commands,
    handle_ml_text_commands,
    handle_ml_hybrid_commands,
    handle_ml_shadow_commands,
    handle_ml_fresh_commands,
    handle_ml_label_commands,
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
