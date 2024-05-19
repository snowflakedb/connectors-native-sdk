import re
import requests
from snowflake.snowpark import Session
from snowflake.snowpark.functions import column
from snowflake.snowpark.types import (
    StructType,
    StructField,
    VariantType,
    IntegerType,
    StringType,
)
from snowflake.snowpark.table import WhenMatchedClause, WhenNotMatchedClause

from snowflake_github_connector.common import sink_table_name, APP_ROLE


def get_next_page_link(response: requests.Response):
    page_links = response.headers.get("Link", "")
    for link in page_links.split(","):
        match = re.match('.*<(.+)>; rel="next"', link)
        if match:
            return match.group(1)
    return None


def fetch_single_batch(
    session: Session,
    destination_db: str,
    resource_name: str,
    github_token: str,
    page_link: str = None,
):
    table_name = sink_table_name(destination_db, resource_name)
    url = page_link or f"https://api.github.com/repos/{resource_name}/issues"
    response = requests.get(url, headers={"Authorization": f"Bearer {github_token}"})
    response.raise_for_status()

    # Save data
    source = session.create_dataframe(
        [{"raw": r} for r in response.json()],
        schema=StructType([StructField("raw", VariantType())]),
    )
    target = session.table(table_name)
    target.merge(
        source,
        target["raw"]["id"] == source["raw"]["id"],
        [
            WhenMatchedClause().update({"raw": source["raw"]}),
            WhenNotMatchedClause().insert({"raw": source["raw"]}),
        ],
    )

    # Return state for next iteration
    return {
        "table_name": table_name,
        "rows_count": session.table(table_name).count(),
        "extras": {
            "next_page_link": get_next_page_link(response),
            "current_page_link": url,
        },
    }


def create_sink_table_and_view(destination_db: str, resource_id: str, session: Session):
    # Create sink table
    table_name = sink_table_name(destination_db, resource_id)
    view_name = table_name + "_FLATTENED"
    session.create_dataframe([], schema=StructType([StructField("raw", VariantType())])).write.mode(
        "ignore"
    ).save_as_table(table_name)
    # Create flatten data view
    columns = {
        "id": IntegerType(),
        "node_id": StringType(),
        "url": StringType(),
        "repository_url": StringType(),
        "labels_url": StringType(),
        "comments_url": StringType(),
        "events_url": StringType(),
        "html_url": StringType(),
        "number": IntegerType(),
        "state": StringType(),
        "title": StringType(),
        "body": StringType(),
        "user": VariantType(),
    }
    session.table(table_name).select(
        *[column("raw")[k].cast(t).alias(k) for k, t in columns.items()]
    ).create_or_replace_view(view_name)
    # Privileges
    session.sql(f"GRANT SELECT ON TABLE {table_name} TO APPLICATION ROLE {APP_ROLE}").collect()
    session.sql(f"GRANT SELECT ON VIEW {view_name} TO APPLICATION ROLE {APP_ROLE}").collect()
