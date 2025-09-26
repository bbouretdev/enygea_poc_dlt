import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponseCursorPaginator
import os

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")

notion_client = RESTClient(
    base_url="https://api.notion.com/v1/",
    headers={
        "Authorization": f"{NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    },
    paginator=JSONResponseCursorPaginator(
        cursor_path="next_cursor",
        cursor_body_path="start_cursor"
    ),
    data_selector="results",
)

@dlt.resource
def points():
    for page in notion_client.paginate(
        f"/databases/c96f87a460fd42ad8b644ef8f53d34fb/query",
        method="POST",
        json={"page_size": 100}
    ):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="points",
    destination="filesystem",
    dataset_name="points"
)
load_info = pipeline.run(points)
print(load_info)