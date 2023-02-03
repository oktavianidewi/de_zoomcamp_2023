from prefect.deployments import Deployment
from q4_answer import etl_web_to_gcs_parent
from prefect.filesystems import GitHub


github_block = GitHub.load("ingest-data")

deploy_flow = Deployment.build_from_flow(
    flow=etl_web_to_gcs_parent,
    name="github-flow",
    infrastructure=github_block,
)


if __name__ == "__main__":
    deploy_flow.apply()
