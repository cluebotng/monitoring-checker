import logging

import aiohttp
from prometheus_client import Gauge

from monitoring_probes.checks import METRIC_PREFIX

logger = logging.getLogger(__name__)

bot_administrator_allow_run = Gauge(
    f"{METRIC_PREFIX}_bot_administrator_allow_run",
    "If the user's (bot's) run page is allowing running",
    ["domain", "username"],
)


async def get_bot_administrator_allow_run(
    username: str, domain: str = "en.wikipedia.org"
) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://{domain}/w/api.php",
            params={
                "action": "query",
                "prop": "revisions",
                "rvslots": "main",
                "titles": f"User:{username}/Run",
                "rvlimit": 1,
                "rvdir": "older",
                "rvprop": "content",
                "format": "json",
            },
            headers={"User-Agent": "ClueBot NG Monitoring"},
        ) as r:
            if r.status != 200:
                logger.error(
                    f"Wikipedia API returned non 200 for revisions query: {r.status}: {await r.text()}"
                )
                return

            data = await r.json()
            if pages := data.get("query", {}).get("pages", {}):
                page = next(iter(pages.values()))
                if revisions := page.get("revisions", []):
                    if (
                        content := revisions[0]
                        .get("slots", {})
                        .get("main", {})
                        .get("*")
                    ):
                        bot_administrator_allow_run.labels(
                            domain=domain, username=username
                        ).set(1 if "true" in content.strip().lower() else 0)
