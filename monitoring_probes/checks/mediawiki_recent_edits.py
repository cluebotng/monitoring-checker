import logging
import os
from datetime import datetime

# asyncmy tries to lookup the current username using pwd.getpwuid to use as the default
# since we don't have a proper user in the container this raises.. just patch it for now
import getpass

getpass.getuser = lambda: "yolo"

from asyncmy import connect

from prometheus_client import Gauge

from monitoring_probes.checks import METRIC_PREFIX

logger = logging.getLogger(__name__)

last_user_contribution_time = Gauge(
    f"{METRIC_PREFIX}_recent_user_contributions_count",
    "Number of recent user contributions",
    ["domain", "username"],
)

DOMAIN_TO_DATABASE_MAPPING = {"en.wikipedia.org": ("enwiki.labsdb", "enwiki_p")}


async def get_recent_user_contributions_count(
    username: str, since_time: datetime, domain: str = "en.wikipedia.org"
) -> None:
    if domain not in DOMAIN_TO_DATABASE_MAPPING:
        logger.error(f"Missing database mapping entry for {domain}")
        return

    database_host, database_schema = DOMAIN_TO_DATABASE_MAPPING[domain]

    database_user = os.environ.get("TOOL_REPLICA_USER")
    database_password = os.environ.get("TOOL_REPLICA_PASSWORD")
    if not database_user or not database_password:
        logger.error("Missing TOOL_REPLICA_USER / TOOL_REPLICA_PASSWORD")
        return

    async with connect(
        host=database_host,
        user=database_user,
        password=database_password,
        database=database_schema,
        echo=True,
    ) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT COUNT(*) FROM `revision_userindex` "
                "WHERE "
                "`rev_actor` = (SELECT actor_id FROM actor WHERE `actor_name` = %s) "
                "AND "
                "`rev_timestamp` >= %s",
                [
                    username,
                    since_time.isoformat(),
                ],
            )
            if ret := await cursor.fetchone():
                last_user_contribution_time.labels(
                    domain=domain, username=username
                ).set(ret[0])
