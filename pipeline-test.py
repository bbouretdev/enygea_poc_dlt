import dlt

pipeline = dlt.pipeline(
    pipeline_name="test_pipeline",
    destination="filesystem",
    dataset_name="test_data"
)

load_info = pipeline.run([{"hello": "world"}], table_name="messages")
print(load_info)
