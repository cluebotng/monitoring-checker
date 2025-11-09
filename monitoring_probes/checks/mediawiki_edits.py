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

recent_user_contributions_count = Gauge(
    f"{METRIC_PREFIX}_recent_user_contributions_count",
    "Number of recent user contributions",
    ["domain", "username"],
)

user_contributions_count = Gauge(
    f"{METRIC_PREFIX}_user_contributions_count",
    "Number of user contributions",
    ["domain", "username"],
)

DOMAIN_TO_DATABASE_MAPPING = {"en.wikipedia.org": ("enwiki.labsdb", "enwiki_p")}


async def get_user_contributions_count(
    username: str, domain: str = "en.wikipedia.org", since_time: datetime | None = None
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

    if since_time:
        target_metric = recent_user_contributions_count
        query = (
            "SELECT COUNT(*) FROM `revision_userindex` "
            "INNER JOIN `actor` ON `rev_actor` = `actor_id` "
            "WHERE `actor_name` = %s AND `rev_timestamp` >= %s"
        )
        query_params = [username, since_time.strftime("%Y%m%d%H%M%S")]
    else:
        target_metric = user_contributions_count
        query = (
            "SELECT `user_editcount` FROM `user` "
            "INNER JOIN `actor` ON `user_id` = `actor_user` "
            "WHERE `actor_name` = %s"
        )
        query_params = [username]

    async with connect(
        host=database_host,
        user=database_user,
        password=database_password,
        database=database_schema,
        echo=True,
    ) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(query, query_params)
            if ret := await cursor.fetchone():
                target_metric.labels(domain=domain, username=username).set(ret[0])
