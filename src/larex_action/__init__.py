# SPDX-License-Identifier: MIT
import logging

from larex_actions.fastapi import create_larex_action_app

from .config import env
from .runner import runner


def configure_logging() -> None:
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    logging.getLogger('larex_actions.transport').setLevel(
        level=logging.DEBUG if env.LAREX_SDK_TRANSPORT_DEBUG else logging.INFO
    )
    logging.getLogger(__name__).setLevel(
        level=logging.DEBUG if env.ACTION_DEBUG else logging.INFO
    )
        

configure_logging()

app = create_larex_action_app(
    handler=runner,
    processor_id=env.LAREX_PROCESSOR_ID,
    dispatch_secret=env.LAREX_DISPATCH_HMAC_SECRET,
    route_prefixes=env.LAREX_ROUTE_PREFIXES,
    allowed_callback_origins=env.LAREX_ALLOWED_CALLBACK_ORIGINS,
)
