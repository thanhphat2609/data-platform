from __future__ import annotations

import os
import subprocess
from dagster import AssetExecutionContext


def run_docker_pipeline(
    context: AssetExecutionContext,
    image: str,
    cmd: list[str],
    env_vars: dict[str, str] | None = None,
) -> None:
    """Run a short-lived Docker container and stream its logs to the Dagster run log.

    Args:
        image:    Full image reference, e.g. "dlt-pipeline:latest" or
                  "myregistry.azurecr.io/spark-pipeline:v1.2".
        cmd:      Command (and args) appended after the image name, e.g.
                  ["pipelines/postgres_pipeline.py"] or
                  ["spark-pipelines", "run", "--spec", "/opt/spark/pipeline/ecommerce/pipeline.yml"].
        env_vars: Env vars to forward into the container via -e KEY=VALUE.
    """
    docker_cmd = ["docker", "run", "--rm"]

    for k, v in (env_vars or {}).items():
        docker_cmd += ["-e", f"{k}={v}"]

    # Join the host Docker network so the container can reach sibling services
    # (e.g. postgres-platform, mysql-ecommerce) in demo / compose environments.
    if network := os.getenv("DOCKER_NETWORK"):
        docker_cmd += ["--network", network]

    docker_cmd.append(image)
    docker_cmd.extend(cmd)

    context.log.info("$ %s", " ".join(docker_cmd))

    proc = subprocess.Popen(
        docker_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    for line in proc.stdout:
        context.log.info(line.rstrip())
    proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(
            f"Container exited {proc.returncode}: {image} {' '.join(cmd)}"
        )
