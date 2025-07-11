import argparse
from pathlib import Path
from databricks.sdk import WorkspaceClient
from loguru import logger

def run_demo_setup():
    """Sets up the DataPact demo environment in a Databricks workspace."""
    parser = argparse.ArgumentParser(description="Set up the DataPact demo environment.")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile to use.")
    args = parser.parse_args()

    logger.info(f"Initializing WorkspaceClient with profile '{args.profile}'...")
    w = WorkspaceClient(profile=args.profile)
    user_name = w.currentUser.me().workspace_user_name
    notebook_path = f"/Users/{user_name}/datapact_demo_setup"

    logger.info("Uploading demo setup notebook...")
    notebook_content = (Path(__file__).parent / "setup_notebook.py").read_bytes()
    w.workspace.upload(path=notebook_path, content=notebook_content, overwrite=True)

    logger.info(f"Running setup notebook at {notebook_path}...")
    run = w.jobs.run_now(
        job_name="datapact_demo_setup_job",
        notebook_task={"notebook_path": notebook_path},
    ).result(timeout=600)

    logger.success("✅ Demo environment setup complete!")
    logger.info("You can now run the demo validation with:")
    logger.info("datapact run --config demo/demo_config.yml --warehouse <your-warehouse-name>")

if __name__ == "__main__":
    run_demo_setup()
