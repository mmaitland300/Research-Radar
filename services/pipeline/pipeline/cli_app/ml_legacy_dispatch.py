from __future__ import annotations

from pipeline.cli_app.dispatch_common import DispatchContext
from pipeline.cli_app.ml_bridge_dispatch import handle_ml_bridge_commands
from pipeline.cli_app.ml_fresh_dispatch import handle_ml_fresh_commands
from pipeline.cli_app.ml_hybrid_dispatch import handle_ml_hybrid_commands
from pipeline.cli_app.ml_label_dispatch import handle_ml_label_commands
from pipeline.cli_app.ml_review_dispatch import handle_ml_review_commands
from pipeline.cli_app.ml_shadow_dispatch import handle_ml_shadow_commands
from pipeline.cli_app.ml_text_dispatch import handle_ml_text_commands


_ML_LEGACY_HANDLERS = (
    handle_ml_review_commands,
    handle_ml_bridge_commands,
    handle_ml_text_commands,
    handle_ml_hybrid_commands,
    handle_ml_shadow_commands,
    handle_ml_fresh_commands,
    handle_ml_label_commands,
)


def handle_ml_legacy_commands(args, ctx: DispatchContext) -> bool:
    for handler in _ML_LEGACY_HANDLERS:
        if handler(args, ctx):
            return True
    return False
