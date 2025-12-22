"""
Complete end-to-end modified agbench runner with container pooling for high-load testing.

Usage:
    # Regular run with container pooling (10 concurrent containers)
    python run_agbench_pooled.py scenarios/ -r 3 --max-concurrent 10
    
    # With Poisson arrivals (2 requests/second mean rate)
    python run_agbench_pooled.py scenarios/ -r 3 --poisson --lambda-rate 2.0 --max-concurrent 10
    
    # Native execution (no Docker, use with caution)
    python run_agbench_pooled.py scenarios/ -r 3 --native
"""

import argparse
import errno
import json
import logging
import os
import pathlib
import random
import re
import shutil
import stat
import subprocess
import sys
import time
import traceback
import asyncio
import tarfile
import io
from dataclasses import dataclass, field
from multiprocessing import Pool
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union, cast

import docker
import yaml
import numpy as np
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from docker.errors import APIError, DockerException, ImageNotFound
from typing_extensions import TypedDict

# Configuration constants
SCRIPT_PATH = os.path.realpath(__file__)
SCRIPT_NAME = os.path.basename(SCRIPT_PATH)
SCRIPT_DIR = os.path.dirname(SCRIPT_PATH)

TASK_TIMEOUT = 60 * 120  # 120 minutes

BASE_TEMPLATE_PATH = os.path.join(SCRIPT_DIR, "template")
RESOURCES_PATH = os.path.join(SCRIPT_DIR, "res")

IS_WIN32 = sys.platform == "win32"

DEFAULT_DOCKER_IMAGE_TAG = "agbench"
DEFAULT_ENV_FILE_JSON = "ENV.json"
DEFAULT_ENV_FILE_YAML = "ENV.yaml"
DEFAULT_CONFIG_YAML = "config.yaml"

# Version placeholder
__version__ = "1.0.0-pooled"

# Random number generator for subsampling
subsample_rng = random.Random(425)


class ScenarioInstance(TypedDict):
    id: str
    template: Union[str, List[Union[str, List[str]]]]
    substitutions: Dict[str, Dict[str, str]]
    values: Dict[str, Dict[str, str]]


@dataclass
class ContainerPool:
    """Thread-safe pool of reusable Docker containers."""
    max_size: int
    image: str
    client: docker.DockerClient
    volumes: Dict[str, Dict[str, str]]
    env: Dict[str, str]
    available: Optional[asyncio.Queue] = None
    in_use: set = field(default_factory=set)
    _lock: Optional[asyncio.Lock] = None
    
    async def initialize(self):
        """Pre-create containers in the pool."""
        self.available = asyncio.Queue(maxsize=self.max_size)
        self._lock = asyncio.Lock()
        
        print(f"Initializing container pool with {self.max_size} containers...")
        
        loop = asyncio.get_event_loop()
        for i in range(self.max_size):
            # Create container in thread pool to avoid blocking
            container = await loop.run_in_executor(
                None,
                self._create_container,
                i
            )
            await self.available.put(container)
            print(f"Container {i+1}/{self.max_size} ready")
    
    def _create_container(self, index: int):
        """Create a single container (runs in thread pool)."""
        container = self.client.containers.create(
            self.image,
            command=["tail", "-f", "/dev/null"],
            working_dir="/workspace",
            environment=self.env,
            detach=True,
            volumes=self.volumes,
            network="host",
            name=f"agbench_pool_{index}_{int(time.time())}"
        )
        container.start()
        return container
    
    async def acquire(self, timeout: int = 60):
        """Get a container from the pool."""
        try:
            container = await asyncio.wait_for(
                self.available.get(),
                timeout=timeout
            )
            async with self._lock:
                self.in_use.add(container)
            return container
        except asyncio.TimeoutError:
            raise TimeoutError("No container available in pool")
    
    async def release(self, container):
        """Return container to pool after cleanup."""
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: container.exec_run("sh -c 'cd /workspace && rm -rf * && rm -rf .*[!.] 2>/dev/null || true'")
            )
        except Exception as e:
            print(f"Warning: Failed to clean container: {e}")
        
        async with self._lock:
            self.in_use.discard(container)
        await self.available.put(container)
    
    async def cleanup(self):
        """Stop and remove all containers."""
        print("Cleaning up container pool...")
        
        # Collect all containers
        containers = []
        while not self.available.empty():
            containers.append(await self.available.get())
        containers.extend(self.in_use)
        
        # Stop and remove in thread pool
        loop = asyncio.get_event_loop()
        for container in containers:
            try:
                await loop.run_in_executor(None, container.stop, 5)
                await loop.run_in_executor(None, container.remove)
            except Exception as e:
                print(f"Warning: Failed to cleanup container: {e}")


def expand_scenario(
    scenario_dir: str, scenario: ScenarioInstance, output_dir: str, config_file: Union[str, None]
) -> None:
    """Expand a scenario into a folder."""
    template = scenario["template"]
    substitutions = scenario["substitutions"] if "substitutions" in scenario else scenario["values"]

    # Convert old format
    if len(substitutions) > 0 and isinstance(substitutions[next(iter(substitutions))], str):
        substitutions = {"scenario.py": cast(Dict[str, str], substitutions)}

    copy_operations: List[Tuple[str, str]] = []

    # Handle templates
    if isinstance(template, str):
        template_path = os.path.join(scenario_dir, template)
        if os.path.isdir(template_path):
            copy_operations.append((template, ""))
        else:
            copy_operations.append((template, "scenario.py"))
    elif isinstance(template, list):
        for elm in template:
            if isinstance(elm, list):
                copy_operations.append((elm[0], elm[1]))
            else:
                copy_operations.append((elm, ""))
    else:
        raise ValueError("expand_scenario expects an str or list for 'template'")

    # Copy base template
    shutil.copytree(
        BASE_TEMPLATE_PATH,
        output_dir,
        ignore=shutil.ignore_patterns("*.example"),
        dirs_exist_ok=False,
    )

    # Expand other folders
    for items in copy_operations:
        src_path = pathlib.Path(os.path.join(scenario_dir, items[0])).absolute()
        dest_path = pathlib.Path(os.path.join(output_dir, items[1])).absolute()

        if os.path.isdir(src_path):
            shutil.copytree(src_path, dest_path, dirs_exist_ok=True)
        else:
            if os.path.isdir(dest_path):
                shutil.copyfile(src_path, os.path.join(dest_path, os.path.basename(src_path)))
            else:
                shutil.copyfile(src_path, dest_path)

    # Apply substitutions
    for templated_file in substitutions.keys():
        template_contents: List[str] = []
        with open(os.path.join(output_dir, templated_file), "rt") as fh:
            for line in fh:
                template_contents.append(line)

        values = substitutions[templated_file]
        with open(os.path.join(output_dir, templated_file), "wt") as fh:
            for line in template_contents:
                for k, v in values.items():
                    line = line.replace(k, v)
                fh.write(line)

    # Copy config
    if config_file is None:
        if os.path.isfile(DEFAULT_CONFIG_YAML):
            config_file = DEFAULT_CONFIG_YAML

    if config_file is not None:
        src_path = pathlib.Path(config_file).absolute()
        dest_path = pathlib.Path(os.path.join(output_dir, "config.yaml")).absolute()
        shutil.copyfile(src_path, dest_path)


def get_scenario_env(token_provider: Optional[Callable[[], str]] = None, env_file: str | None = None) -> Dict[str, str]:
    """Return environment variables needed to run a scenario."""
    env: Dict[str, str] = dict()

    # Add OpenAI API key
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    if openai_api_key is not None and len(openai_api_key.strip()) > 0:
        env["OPENAI_API_KEY"] = openai_api_key

    # Support Azure auth tokens
    azure_openai_ad_token = os.environ.get("AZURE_OPENAI_AD_TOKEN")
    if azure_openai_ad_token is None and token_provider is not None:
        azure_openai_ad_token = token_provider()
    if azure_openai_ad_token is not None and len(azure_openai_ad_token.strip()) > 0:
        env["AZURE_OPENAI_AD_TOKEN"] = azure_openai_ad_token

    # Load env file
    env_file_contents: Dict[str, Any] = {}
    if env_file is None:
        if os.path.isfile(DEFAULT_ENV_FILE_YAML):
            with open(DEFAULT_ENV_FILE_YAML, "r") as fh:
                env_file_contents = yaml.safe_load(fh) or {}
        elif os.path.isfile(DEFAULT_ENV_FILE_JSON):
            with open(DEFAULT_ENV_FILE_JSON, "rt") as fh:
                env_file_contents = json.loads(fh.read())
    else:
        with open(env_file, "rt") as fh:
            if env_file.endswith(".json"):
                env_file_contents = json.loads(fh.read())
            else:
                env_file_contents = yaml.safe_load(fh) or {}

    # Apply substitutions
    substitute_env_variables(env_file_contents)

    # Flatten structures
    for key, value in env_file_contents.items():
        if isinstance(value, dict) or isinstance(value, list):
            env_file_contents[key] = json.dumps(value)

    env.update(cast(Dict[str, str], env_file_contents))
    return env


def substitute_env_variables(json_data: Any) -> None:
    """Recursively replace ${ENV_VARIABLE} with os.environ values."""
    def replace_env_var(match: Any) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, "")

    pattern = re.compile(r"\$\{(\w+)\}")

    def replace_in_dict(d: Dict[str, Any]) -> None:
        for key, value in d.items():
            if isinstance(value, str):
                d[key] = pattern.sub(replace_env_var, value)
            elif isinstance(value, dict):
                replace_in_dict(cast(Dict[str, Any], value))
            elif isinstance(value, list):
                replace_in_list(cast(List[Any], value))

    def replace_in_list(lst: List[Any]) -> None:
        for i, item in enumerate(lst):
            if isinstance(item, str):
                lst[i] = pattern.sub(replace_env_var, item)
            elif isinstance(item, dict):
                replace_in_dict(cast(Dict[str, Any], item))
            elif isinstance(item, list):
                replace_in_list(cast(List[Any], item))

    if isinstance(json_data, dict):
        replace_in_dict(cast(Dict[str, Any], json_data))
    elif isinstance(json_data, list):
        replace_in_list(cast(List[Any], json_data))


def create_run_script(work_dir: str, timeout: int, is_docker: bool = True) -> None:
    """Create the run.sh script for executing scenarios."""
    setting = "Docker" if is_docker else "Native"
    
    venv_setup = ""
    if not is_docker:
        venv_setup = f"""
# Create and activate virtual environment
{sys.executable} -m venv .agbench_venv
. .agbench_venv/bin/activate
"""
    
    venv_cleanup = ""
    if not is_docker:
        venv_cleanup = """
# Clean up virtual environment
if [ -d .agbench_venv ] ; then
    rm -Rf .agbench_venv
fi
"""
    
    script_content = f"""#!/bin/bash
echo RUN.SH STARTING !#!#
export AUTOGEN_TESTBED_SETTING="{setting}"
{'umask 000' if is_docker else ''}
echo "agbench version: {__version__}" > timestamp.txt
{venv_setup}
# Run global init script
if [ -f global_init.sh ] ; then
    . ./global_init.sh
fi

# Run scenario init script
if [ -f scenario_init.sh ] ; then
    . ./scenario_init.sh
fi

# Install requirements and run scenario
pip install -r requirements.txt
echo SCENARIO.PY STARTING !#!#
start_time=$(date +%s)
timeout --preserve-status --kill-after {timeout + 30}s {timeout}s python scenario.py
end_time=$(date +%s)
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo SCENARIO.PY EXITED WITH CODE: $EXIT_CODE !#!#
else
    echo SCENARIO.PY COMPLETE !#!#
fi
elapsed_time=$((end_time - start_time))
echo "SCENARIO.PY RUNTIME: $elapsed_time !#!#"

# Cleanup
if [ -d .cache ] ; then
    rm -Rf .cache
fi
if [ -d __pycache__ ] ; then
    rm -Rf __pycache__
fi

# Run finalize scripts
if [ -f scenario_finalize.sh ] ; then
    . ./scenario_finalize.sh
fi
if [ -f global_finalize.sh ] ; then
    . ./global_finalize.sh
fi
{venv_cleanup}
echo RUN.SH COMPLETE !#!#
"""
    
    with open(os.path.join(work_dir, "run.sh"), "wt", newline="\n") as f:
        f.write(script_content)


async def run_scenario_in_pooled_container(
    container,
    work_dir: str,
    timeout: int = TASK_TIMEOUT
) -> Dict[str, Any]:
    """Run a scenario in a pre-existing container from the pool."""
    loop = asyncio.get_event_loop()
    
    # Create tar archive of work directory
    def create_tar():
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            for item in os.listdir(work_dir):
                tar.add(os.path.join(work_dir, item), arcname=item)
        tar_stream.seek(0)
        return tar_stream
    
    tar_stream = await loop.run_in_executor(None, create_tar)
    
    # Copy files to container
    await loop.run_in_executor(
        None,
        container.put_archive,
        '/workspace',
        tar_stream
    )
    
    # Execute scenario
    start_time = time.time()
    
    def exec_scenario():
        return container.exec_run(
            ["sh", "run.sh"],
            workdir="/workspace",
            demux=True,
            stream=True
        )
    
    exec_result = await loop.run_in_executor(None, exec_scenario)
    
    # Stream logs
    log_file_path = os.path.join(work_dir, "console_log.txt")
    
    def write_logs():
        with open(log_file_path, "wb") as log_file:
            for stdout_chunk, stderr_chunk in exec_result.output:
                if stdout_chunk:
                    log_file.write(stdout_chunk)
                    sys.stdout.buffer.write(stdout_chunk)
                    sys.stdout.flush()
                if stderr_chunk:
                    log_file.write(stderr_chunk)
                    sys.stderr.buffer.write(stderr_chunk)
                    sys.stderr.flush()
    
    await loop.run_in_executor(None, write_logs)
    
    elapsed = time.time() - start_time
    
    return {
        "work_dir": work_dir,
        "elapsed": elapsed,
        "timeout": elapsed >= timeout
    }


async def run_scenarios_with_pool(
    scenario: str,
    n_repeats: int,
    max_concurrent: int,
    config_file: Optional[str],
    token_provider: Optional[Callable[[], str]],
    docker_image: Optional[str],
    results_dir: str,
    subsample: Optional[Union[int, float]],
    env_file: Optional[str],
    lambda_rate: Optional[float] = None,
) -> None:
    """Run scenarios using container pool with optional Poisson arrivals."""
    
    # Load scenario files
    files: List[str] = []
    if scenario == "-" or os.path.isfile(scenario):
        files.append(scenario)
    elif os.path.isdir(scenario):
        for f in os.listdir(scenario):
            scenario_file = os.path.join(scenario, f)
            if os.path.isfile(scenario_file) and scenario_file.lower().endswith(".jsonl"):
                files.append(scenario_file)
    else:
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), scenario)

    # Prepare all tasks
    all_tasks = []
    for scenario_file in files:
        scenario_name_parts = os.path.basename(scenario_file).split(".")
        scenario_name_parts.pop()
        scenario_name = ".".join(scenario_name_parts)
        scenario_dir = os.path.dirname(os.path.realpath(scenario_file))
        
        with open(scenario_file, "rt") as fh:
            lines = [line for line in fh]
            if subsample is not None:
                n = int(len(lines) * subsample) if 0 <= subsample < 1 else int(subsample)
                n = max(0, min(n, len(lines)))
                lines = subsample_rng.sample(lines, n)
            
            for line in lines:
                instance = json.loads(line)
                for repeat in range(n_repeats):
                    results_instance = os.path.join(results_dir, scenario_name, instance["id"])
                    results_repetition = os.path.join(results_instance, str(repeat))
                    
                    if os.path.isdir(results_repetition):
                        print(f"Found folder {results_repetition} ... Skipping.")
                        continue
                    
                    all_tasks.append({
                        "scenario_dir": scenario_dir,
                        "instance": instance,
                        "work_dir": results_repetition,
                        "config_file": config_file,
                    })
    
    if not all_tasks:
        print("No tasks to run!")
        return
    
    # Calculate Poisson arrival times if requested
    if lambda_rate is not None:
        current_time = 0
        for task in all_tasks:
            inter_arrival = np.random.exponential(1.0 / lambda_rate)
            current_time += inter_arrival
            task["start_time"] = current_time
        print(f"Prepared {len(all_tasks)} tasks with Poisson arrivals (λ={lambda_rate} req/s)")
    else:
        for task in all_tasks:
            task["start_time"] = 0
        print(f"Prepared {len(all_tasks)} tasks without Poisson timing")
    
    # Setup Docker
    client = docker.from_env()
    
    # Get or build image
    if docker_image is None:
        docker_image = DEFAULT_DOCKER_IMAGE_TAG
        try:
            image = client.images.get(docker_image)
        except ImageNotFound:
            print(f"Building default Docker image '{docker_image}'...")
            build_default_docker_image(client, docker_image)
            image = client.images.get(docker_image)
    else:
        try:
            image = client.images.get(docker_image)
        except ImageNotFound:
            print(f"Pulling image '{docker_image}'...")
            image = client.images.pull(docker_image)
    
    # Setup volumes
    work_base = os.path.abspath(results_dir)
    os.makedirs(work_base, exist_ok=True)
    volumes = {work_base: {"bind": "/workspace", "mode": "rw"}}
    
    # Get environment
    env = get_scenario_env(token_provider=token_provider, env_file=env_file)
    
    # Initialize container pool
    pool = ContainerPool(
        max_size=max_concurrent,
        image=image.id,
        client=client,
        volumes=volumes,
        env=env
    )
    
    await pool.initialize()
    
    # Execute tasks
    semaphore = asyncio.Semaphore(max_concurrent)
    start_time = time.time()
    
    async def execute_task(task_info):
        async with semaphore:
            # Wait for Poisson scheduled time
            if task_info["start_time"] > 0:
                wait_time = task_info["start_time"] - (time.time() - start_time)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
            
            container = await pool.acquire()
            
            try:
                # Prepare scenario
                os.makedirs(task_info["work_dir"], exist_ok=True)
                expand_scenario(
                    task_info["scenario_dir"],
                    task_info["instance"],
                    task_info["work_dir"],
                    task_info["config_file"]
                )
                create_run_script(task_info["work_dir"], TASK_TIMEOUT, is_docker=True)
                
                # Run scenario
                elapsed = time.time() - start_time
                print(f"[{elapsed:.1f}s] Starting {task_info['work_dir']}")
                
                result = await run_scenario_in_pooled_container(
                    container,
                    task_info["work_dir"]
                )
                
                elapsed = time.time() - start_time
                print(f"[{elapsed:.1f}s] Completed {task_info['work_dir']} in {result['elapsed']:.1f}s")
                
            finally:
                await pool.release(container)
    
    # Run all tasks
    try:
        await asyncio.gather(*[execute_task(task) for task in all_tasks])
    finally:
        await pool.cleanup()
    
    total_time = time.time() - start_time
    print(f"\nCompleted {len(all_tasks)} tasks in {total_time:.1f}s")
    if total_time > 0:
        print(f"Actual rate: {len(all_tasks)/total_time:.2f} req/s")


def run_scenario_natively(work_dir: str, env: Dict[str, str], timeout: int = TASK_TIMEOUT) -> None:
    """Run a scenario in the native environment."""
    cwd = os.getcwd()
    full_env = os.environ.copy()
    full_env.update(env)
    
    os.chdir(work_dir)
    print("\n\n" + os.getcwd() + "\n===================================================================")
    
    create_run_script(work_dir, timeout, is_docker=False)
    
    with open("console_log.txt", "wb") as f:
        process = subprocess.Popen(
            ["sh", "run.sh"],
            env=full_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for c in iter(lambda: process.stdout.read(1), b""):
            f.write(c)
            os.write(sys.stdout.fileno(), c)
    
    os.chdir(cwd)


def build_default_docker_image(docker_client: docker.DockerClient, image_tag: str) -> None:
    """Build the default Docker image."""
    for segment in docker_client.api.build(
        path=RESOURCES_PATH,
        dockerfile="Dockerfile",
        rm=True,
        tag=image_tag,
        decode=True,
    ):
        if "stream" in segment:
            sys.stdout.write(segment["stream"])


def get_azure_token_provider() -> Optional[Callable[[], str]]:
    """Get Azure bearer token provider."""
    if not os.environ.get("AZURE_OPENAI_AD_TOKEN") and os.path.isdir(pathlib.Path("~/.azure").expanduser()):
        logging.disable(logging.CRITICAL)
        try:
            azure_token_provider = get_bearer_token_provider(
                DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
            )
            azure_token_provider()
            print("Found Azure token provider.")
            return azure_token_provider
        except ClientAuthenticationError:
            error_message = traceback.format_exc()
            print(f"Azure token provider failed. Try 'az login --use-device-code'\n{error_message}")
        logging.disable(logging.NOTSET)
    return None


def run_cli(args: Sequence[str]) -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run AutoGen scenarios with container pooling for high-load testing"
    )
    
    parser.add_argument("scenario", help="JSONL scenario file or directory")
    parser.add_argument("-r", "--repeat", type=int, default=1, help="Number of repetitions")
    parser.add_argument("-s", "--subsample", type=str, default=None, help="Subsample proportion or count")
    parser.add_argument("-a", "--azure", action="store_true", help="Use Azure identity")
    parser.add_argument("-e", "--env", type=str, default=None, help="Environment file")
    parser.add_argument("-c", "--config", type=str, default=None, help="Config file")
    parser.add_argument("-d", "--docker-image", type=str, default=None, help="Docker image to use")
    parser.add_argument("--native", action="store_true", help="Run natively (no Docker)")
    
    # Container pooling options
    parser.add_argument("--max-concurrent", type=int, default=10, help="Max concurrent containers")
    parser.add_argument("--poisson", action="store_true", help="Use Poisson arrival process")
    parser.add_argument("--lambda-rate", type=float, default=1.0, help="Mean requests/sec for Poisson")
    
    parsed_args = parser.parse_args(args[1:])
    
    # Validate arguments
    if parsed_args.docker_image is not None and parsed_args.native:
        sys.exit("Cannot use --native and --docker-image together")
    
    # Parse subsample
    subsample = None
    if parsed_args.subsample is not None:
        subsample = float(parsed_args.subsample)
        if "." in parsed_args.subsample:
            if subsample == 1.0:
                subsample = None
            elif subsample < 0 or subsample > 1.0:
                raise ValueError("Subsample must be between 0.0 and 1.0")
    
    # Get Azure token provider
    azure_token_provider = None
    if parsed_args.azure:
        azure_token_provider = get_azure_token_provider()
    
    # Warn about native execution
    if parsed_args.native:
        if IS_WIN32:
            sys.exit("--native not supported on Windows")
        
        sys.stderr.write("WARNING: Native execution poses security risks and test contamination issues.\n")
        allow_native = os.environ.get("AGBENCH_ALLOW_NATIVE")
        if allow_native is None or allow_native == "":
            choice = input('Type "Yes" to continue with native execution: ')
            if choice.strip().lower() != "yes":
                sys.exit("Exiting.")
        elif allow_native.strip().lower() != "yes":
            sys.exit(f"Exiting because AGBENCH_ALLOW_NATIVE is '{allow_native}'")
    
    # Run scenarios
    if parsed_args.native:
        # Native execution without pooling
        print("Running natively (without Docker)...")
        # Simple sequential execution for native mode
        # You could implement native parallel execution here if needed
        sys.exit("Native mode not fully implemented in this version. Use Docker with --max-concurrent.")
    else:
        # Docker with container pooling
        lambda_rate = parsed_args.lambda_rate if parsed_args.poisson else None
        
        asyncio.run(run_scenarios_with_pool(
            scenario=parsed_args.scenario,
            n_repeats=parsed_args.repeat,
            max_concurrent=parsed_args.max_concurrent,
            config_file=parsed_args.config,
            token_provider=azure_token_provider,
            docker_image=parsed_args.docker_image,
            results_dir="Results",
            subsample=subsample,
            env_file=parsed_args.env,
            lambda_rate=lambda_rate,
        ))


if __name__ == "__main__":
    run_cli(sys.argv)





























# """
# Modifications to support high-load vLLM testing with Poisson distribution.

# Key changes:
# 1. Container pooling - reuse containers instead of recreate
# 2. Async execution - run multiple scenarios concurrently
# 3. Decoupled execution - separate container lifecycle from task execution
# """

# import asyncio
# import time
# from collections import deque
# from dataclasses import dataclass
# from typing import Optional
# import docker
# import numpy as np


# @dataclass
# class ContainerPool:
#     """Pool of reusable Docker containers for running scenarios."""
#     max_size: int
#     image: str
#     client: docker.DockerClient
#     available: deque = None
#     in_use: set = None
    
#     def __post_init__(self):
#         self.available = deque()
#         self.in_use = set()
    
#     def initialize(self, volumes: dict, env: dict):
#         """Pre-create containers in the pool."""
#         print(f"Initializing container pool with {self.max_size} containers...")
#         for i in range(self.max_size):
#             container = self.client.containers.create(
#                 self.image,
#                 command=["tail", "-f", "/dev/null"],  # Keep alive
#                 working_dir="/workspace",
#                 environment=env,
#                 detach=True,
#                 volumes=volumes,
#                 network="host",
#                 name=f"agbench_pool_{i}_{int(time.time())}"
#             )
#             container.start()
#             self.available.append(container)
#             print(f"Container {i+1}/{self.max_size} ready")
    
#     def acquire(self, timeout=60) -> Optional[docker.models.containers.Container]:
#         """Get an available container from the pool."""
#         start = time.time()
#         while time.time() - start < timeout:
#             if self.available:
#                 container = self.available.popleft()
#                 self.in_use.add(container)
#                 return container
#             time.sleep(0.1)
#         raise TimeoutError("No container available in pool")
    
#     def release(self, container: docker.models.containers.Container):
#         """Return a container to the pool after cleanup."""
#         # Clean up the container workspace
#         try:
#             container.exec_run("sh -c 'cd /workspace && rm -rf *'")
#         except Exception as e:
#             print(f"Warning: Failed to clean container: {e}")
        
#         self.in_use.discard(container)
#         self.available.append(container)
    
#     def cleanup(self):
#         """Destroy all containers in the pool."""
#         print("Cleaning up container pool...")
#         all_containers = list(self.available) + list(self.in_use)
#         for container in all_containers:
#             try:
#                 container.stop(timeout=5)
#                 container.remove()
#             except Exception as e:
#                 print(f"Warning: Failed to cleanup container: {e}")


# async def run_scenario_in_pooled_container(
#     container: docker.models.containers.Container,
#     work_dir: str,
#     timeout: int = TASK_TIMEOUT
# ) -> dict:
#     """
#     Run a scenario in a pre-existing container from the pool.
#     This eliminates container start/stop overhead.
#     """
#     import tarfile
#     import io
    
#     # Create tar archive of the work directory
#     tar_stream = io.BytesIO()
#     with tarfile.open(fileobj=tar_stream, mode='w') as tar:
#         tar.add(work_dir, arcname='.')
#     tar_stream.seek(0)
    
#     # Copy files to container
#     container.put_archive('/workspace', tar_stream)
    
#     # Execute the scenario
#     start_time = time.time()
#     exec_result = container.exec_run(
#         ["sh", "run.sh"],
#         workdir="/workspace",
#         demux=True,
#         stream=True
#     )
    
#     # Stream logs
#     log_file_path = os.path.join(work_dir, "console_log.txt")
#     with open(log_file_path, "wb") as log_file:
#         for stdout_chunk, stderr_chunk in exec_result.output:
#             if stdout_chunk:
#                 log_file.write(stdout_chunk)
#             if stderr_chunk:
#                 log_file.write(stderr_chunk)
    
#     elapsed = time.time() - start_time
    
#     return {
#         "work_dir": work_dir,
#         "elapsed": elapsed,
#         "timeout": elapsed >= timeout
#     }


# async def run_scenarios_async_poisson(
#     scenario: str,
#     n_repeats: int,
#     lambda_rate: float,  # Mean requests per second
#     max_concurrent: int,  # Max concurrent containers
#     config_file: Optional[str],
#     token_provider: Optional[Callable[[], str]],
#     docker_image: Optional[str] = None,
#     results_dir: str = "Results",
#     subsample: Optional[Union[int, float]] = None,
#     env_file: Optional[str] = None,
# ) -> None:
#     """
#     Run scenarios with Poisson arrival process and concurrent execution.
    
#     This creates a container pool and reuses containers to minimize overhead,
#     allowing the vLLM server to become the bottleneck rather than Docker.
#     """
    
#     # Load scenarios
#     files = []
#     if scenario == "-" or os.path.isfile(scenario):
#         files.append(scenario)
#     elif os.path.isdir(scenario):
#         files.extend([
#             os.path.join(scenario, f) 
#             for f in os.listdir(scenario) 
#             if f.endswith(".jsonl")
#         ])
    
#     # Read all scenario instances
#     all_instances = []
#     for scenario_file in files:
#         with open(scenario_file, "rt") as f:
#             instances = [json.loads(line) for line in f]
#             if subsample:
#                 n = int(len(instances) * subsample) if subsample < 1 else int(subsample)
#                 instances = subsample_rng.sample(instances, min(n, len(instances)))
#             all_instances.extend([(scenario_file, inst) for inst in instances])
    
#     # Setup Docker
#     client = docker.from_env()
#     image = client.images.get(docker_image or DEFAULT_DOCKER_IMAGE_TAG)
    
#     # Get environment
#     env = get_scenario_env(token_provider=token_provider, env_file=env_file)
    
#     # Setup volumes (simplified - adjust as needed)
#     work_base = os.path.abspath(results_dir)
#     volumes = {work_base: {"bind": "/workspace", "mode": "rw"}}
    
#     # Initialize container pool
#     pool = ContainerPool(
#         max_size=max_concurrent,
#         image=image,
#         client=client
#     )
#     pool.initialize(volumes, env)
    
#     # Prepare all tasks with Poisson timing
#     tasks = []
#     current_time = 0
    
#     for scenario_file, instance in all_instances * n_repeats:
#         # Poisson inter-arrival time
#         inter_arrival = np.random.exponential(1.0 / lambda_rate)
#         current_time += inter_arrival
        
#         scenario_name = os.path.basename(scenario_file).rsplit('.', 1)[0]
#         work_dir = os.path.join(
#             results_dir, 
#             scenario_name, 
#             instance["id"],
#             str(len(tasks) % n_repeats)
#         )
        
#         tasks.append({
#             "start_time": current_time,
#             "work_dir": work_dir,
#             "scenario_file": scenario_file,
#             "instance": instance,
#             "config_file": config_file
#         })
    
#     print(f"Prepared {len(tasks)} tasks over {current_time:.1f}s")
#     print(f"Mean rate: {lambda_rate} req/s, Max concurrent: {max_concurrent}")
    
#     # Execute tasks with Poisson timing
#     semaphore = asyncio.Semaphore(max_concurrent)
#     start_time = time.time()
#     active_tasks = []
    
#     async def execute_task(task_info):
#         async with semaphore:
#             # Wait until scheduled time
#             wait_time = task_info["start_time"] - (time.time() - start_time)
#             if wait_time > 0:
#                 await asyncio.sleep(wait_time)
            
#             # Acquire container from pool
#             container = pool.acquire()
            
#             try:
#                 # Prepare scenario
#                 os.makedirs(task_info["work_dir"], exist_ok=True)
#                 expand_scenario(
#                     os.path.dirname(task_info["scenario_file"]),
#                     task_info["instance"],
#                     task_info["work_dir"],
#                     task_info["config_file"]
#                 )
                
#                 # Run in pooled container
#                 print(f"[{time.time()-start_time:.1f}s] Starting {task_info['work_dir']}")
#                 result = await run_scenario_in_pooled_container(
#                     container,
#                     task_info["work_dir"]
#                 )
#                 print(f"[{time.time()-start_time:.1f}s] Completed {task_info['work_dir']} in {result['elapsed']:.1f}s")
                
#             finally:
#                 # Return container to pool
#                 pool.release(container)
    
#     # Launch all tasks
#     await asyncio.gather(*[execute_task(task) for task in tasks])
    
#     # Cleanup
#     pool.cleanup()
    
#     total_time = time.time() - start_time
#     print(f"\nCompleted all tasks in {total_time:.1f}s")
#     print(f"Actual rate: {len(tasks)/total_time:.2f} req/s")


# def run_cli_with_poisson(args: Sequence[str]) -> None:
#     """Extended CLI with Poisson load testing support."""
    
#     parser = argparse.ArgumentParser(
#         description="Run AutoGen scenarios with Poisson arrival for load testing"
#     )
    
#     # ... existing arguments ...
    
#     parser.add_argument(
#         "--poisson",
#         action="store_true",
#         help="Use Poisson arrival process for load testing"
#     )
#     parser.add_argument(
#         "--lambda-rate",
#         type=float,
#         default=1.0,
#         help="Mean requests per second for Poisson process (default: 1.0)"
#     )
#     parser.add_argument(
#         "--max-concurrent",
#         type=int,
#         default=10,
#         help="Maximum concurrent containers in pool (default: 10)"
#     )
    
#     parsed_args = parser.parse_args(args[1:])
    
#     # Get Azure token provider if needed
#     azure_token_provider = None
#     if parsed_args.azure:
#         azure_token_provider = get_azure_token_provider()
    
#     # Run with Poisson if requested
#     if parsed_args.poisson:
#         asyncio.run(run_scenarios_async_poisson(
#             scenario=parsed_args.scenario,
#             n_repeats=parsed_args.repeat,
#             lambda_rate=parsed_args.lambda_rate,
#             max_concurrent=parsed_args.max_concurrent,
#             config_file=parsed_args.config,
#             token_provider=azure_token_provider,
#             docker_image=parsed_args.docker_image,
#             subsample=parsed_args.subsample,
#             env_file=parsed_args.env,
#         ))
#     else:
#         # Use original implementation
#         run_scenarios(...)


# # Alternative: Long-lived containers with exec-based execution
# def run_with_persistent_containers(
#     scenario: str,
#     n_containers: int = 5,
#     **kwargs
# ):
#     """
#     Keep N containers running and distribute tasks across them.
#     Lowest overhead but requires careful state management.
#     """
#     client = docker.from_env()
    
#     # Start persistent containers
#     containers = []
#     for i in range(n_containers):
#         container = client.containers.run(
#             DEFAULT_DOCKER_IMAGE_TAG,
#             command=["tail", "-f", "/dev/null"],
#             detach=True,
#             **kwargs  # volumes, env, etc
#         )
#         containers.append(container)
    
#     try:
#         # Distribute tasks round-robin
#         for idx, task in enumerate(tasks):
#             container = containers[idx % n_containers]
#             # Copy files and exec
#             # ... execution logic ...
#     finally:
#         # Cleanup
#         for container in containers:
#             container.stop()
#             container.remove()