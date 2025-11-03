import asyncio
import logging
from datetime import timedelta, datetime

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from prometheus_client import (
    REGISTRY,
    GC_COLLECTOR,
    PROCESS_COLLECTOR,
    PLATFORM_COLLECTOR,
)

from monitoring_probes.checks.mediawiki_bot_allowed_to_run import (
    get_bot_administrator_allow_run,
)
from monitoring_probes.checks.mediawiki_contribution_time import (
    get_last_user_contribution_time,
)
from monitoring_probes.checks.mediawiki_recent_edits import (
    get_recent_user_contributions_count,
)

logger = logging.getLogger(__name__)


class PrometheusResponse(Response):
    media_type = CONTENT_TYPE_LATEST


app = FastAPI()


@app.on_event("startup")
async def startup_event():
    # Disable the default metrics
    REGISTRY.unregister(GC_COLLECTOR)
    REGISTRY.unregister(PROCESS_COLLECTOR)
    REGISTRY.unregister(PLATFORM_COLLECTOR)


@app.get("/metrics", response_class=PrometheusResponse)
async def _render_metrics():
    recent_window_start_time = datetime.now() - timedelta(hours=24)
    await asyncio.gather(
        get_last_user_contribution_time("ClueBot NG"),
        get_last_user_contribution_time("ClueBot III"),
        get_last_user_contribution_time("ClueBot NG Review Interface"),
        get_recent_user_contributions_count("ClueBot NG", recent_window_start_time),
        get_recent_user_contributions_count("ClueBot III", recent_window_start_time),
        get_bot_administrator_allow_run("ClueBot NG"),
        get_bot_administrator_allow_run("ClueBot III"),
    )

    return generate_latest()


@app.get("/health")
async def _render_health():
    return "OK"
