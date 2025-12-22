import asyncio
import os
import yaml
import warnings
from autogen_ext.agents.magentic_one import MagenticOneCoderAgent
from autogen_agentchat.teams import MagenticOneGroupChat
from autogen_agentchat.ui import Console
from autogen_core.models import ModelFamily
from autogen_ext.code_executors.local import LocalCommandLineCodeExecutor
from autogen_agentchat.conditions import TextMentionTermination
from autogen_core.models import ChatCompletionClient
from autogen_ext.agents.web_surfer import MultimodalWebSurfer
from autogen_ext.agents.file_surfer import FileSurfer
from autogen_agentchat.agents import CodeExecutorAgent
from autogen_agentchat.messages import TextMessage
from typing import Any

async def main() -> None:
    print("PALAK: inside scenario's main method")
    # Load model configuration and create the model client.
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    orchestrator_client = ChatCompletionClient.load_component(config["orchestrator_client"])
    coder_client = ChatCompletionClient.load_component(config["coder_client"])
    web_surfer_client = ChatCompletionClient.load_component(config["web_surfer_client"])
    file_surfer_client = ChatCompletionClient.load_component(config["file_surfer_client"])

    
    # Read the prompt
    prompt = ""
    with open("prompt.txt", "rt") as fh:
        prompt = fh.read().strip()
    filename = "__FILE_NAME__".strip()

    #### PALAK
    task_id = "__TASK_ID__".strip()
    MASK_64_BITS = (1 << 64) - 1

    print("PALAK: task_id: ", task_id)
    import uuid
    if task_id == "":
        custom_request_id_suffix = f"no_task_id_{uuid.uuid4()}"
    else:
        custom_request_id_suffix = f"{task_id}:{uuid.uuid4().int & MASK_64_BITS:016x}" ###### from vllm.utils random_uuid()

    print("PALAK: custom_request_id_suffix: ", custom_request_id_suffix)

    # Set up the team
    coder = MagenticOneCoderAgent(
        "Assistant",
        model_client = coder_client,
        custom_request_id_suffix = custom_request_id_suffix
    )

    executor = CodeExecutorAgent("ComputerTerminal", code_executor=LocalCommandLineCodeExecutor())

    file_surfer = FileSurfer(
        name="FileSurfer",
        model_client = file_surfer_client,
        custom_request_id_suffix = custom_request_id_suffix
    )
                
    web_surfer = MultimodalWebSurfer(
        name="WebSurfer",
        model_client = web_surfer_client,
        downloads_folder=os.getcwd(),
        debug_dir="logs",
        to_save_screenshots=True,
        custom_request_id_suffix = custom_request_id_suffix
    )

    team = MagenticOneGroupChat(
        [coder, executor, file_surfer, web_surfer],
        # [coder, executor, file_surfer],
        model_client=orchestrator_client,
        max_turns=20, ### 20 initially
        final_answer_prompt= f""",
We have completed the following task:

{prompt}

The above messages contain the conversation that took place to complete the task.
Read the above conversation and output a FINAL ANSWER to the question.
To output the final answer, use the following template: FINAL ANSWER: [YOUR FINAL ANSWER]
Your FINAL ANSWER should be a number OR as few words as possible OR a comma separated list of numbers and/or strings.
ADDITIONALLY, your FINAL ANSWER MUST adhere to any formatting instructions specified in the original question (e.g., alphabetization, sequencing, units, rounding, decimal places, etc.)
If you are asked for a number, express it numerically (i.e., with digits rather than words), don't use commas, and don't include units such as $ or percent signs unless specified otherwise.
If you are asked for a string, don't use articles or abbreviations (e.g. for cities), unless specified otherwise. Don't output any final sentence punctuation such as '.', '!', or '?'.
If you are asked for a comma separated list, apply the above rules depending on whether the elements are numbers or strings.
""".strip(),
    custom_request_id_suffix = custom_request_id_suffix
    )

    # Prepare the prompt
    filename_prompt = ""
    if len(filename) > 0:
        filename_prompt = f"The question is about a file, document or image, which can be accessed by the filename '{filename}' in the current working directory."
    task = f"{prompt}\n\n{filename_prompt}"

    # # # Run the task
    # stream = team.run_stream(task=task.strip())
    # await Console(stream, output_stats=True)

    import time
    start_time = time.perf_counter()
    stream = team.run_stream(task=task.strip())
    # await Console(stream)
    await Console(stream, output_stats=True)
    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print(f"Task start time: ", start_time)
    print(f"Task end time: ", end_time)
    print(f"Task completed in {elapsed:.3f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
