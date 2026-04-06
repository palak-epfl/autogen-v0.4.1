import asyncio
import inspect
import json
import logging
import math
import os
import re
import warnings
from asyncio import Task
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
    Union,
    cast,
)

import tiktoken
from autogen_core import (
    EVENT_LOGGER_NAME,
    TRACE_LOGGER_NAME,
    CancellationToken,
    Component,
    FunctionCall,
    Image,
)
from autogen_core.logging import LLMCallEvent, LLMStreamEndEvent, LLMStreamStartEvent
from autogen_core.models import (
    AssistantMessage,
    ChatCompletionClient,
    ChatCompletionTokenLogprob,
    CreateResult,
    LLMMessage,
    ModelCapabilities,  # type: ignore
    ModelFamily,
    ModelInfo,
    RequestUsage,
    SystemMessage,
    TopLogprob,
    UserMessage,
    validate_model_info,
)
from autogen_core.tools import Tool, ToolSchema
from openai import NOT_GIVEN, AsyncAzureOpenAI, AsyncOpenAI
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionChunk,
    ChatCompletionContentPartParam,
    ChatCompletionMessageParam,
    ChatCompletionRole,
    ChatCompletionToolParam,
    ParsedChatCompletion,
    ParsedChoice,
    completion_create_params,
)
from openai.types.chat.chat_completion import Choice
from openai.types.shared_params import (
    FunctionDefinition,
    FunctionParameters,
    ResponseFormatJSONObject,
    ResponseFormatText,
)
from pydantic import BaseModel, SecretStr
from typing_extensions import Self, Unpack

from .._utils.normalize_stop_reason import normalize_stop_reason
from .._utils.parse_r1_content import parse_r1_content
from . import _model_info
from ._transformation import (
    get_transformer,
)
from ._utils import assert_valid_name
from .config import (
    AzureOpenAIClientConfiguration,
    AzureOpenAIClientConfigurationConfigModel,
    OpenAIClientConfiguration,
    OpenAIClientConfigurationConfigModel,
)

logger = logging.getLogger(EVENT_LOGGER_NAME)
trace_logger = logging.getLogger(TRACE_LOGGER_NAME)

openai_init_kwargs = set(inspect.getfullargspec(AsyncOpenAI.__init__).kwonlyargs)
aopenai_init_kwargs = set(inspect.getfullargspec(AsyncAzureOpenAI.__init__).kwonlyargs)

create_kwargs = set(completion_create_params.CompletionCreateParamsBase.__annotations__.keys()) | set(
    ("timeout", "stream", "extra_body")
)
# Only single choice allowed
disallowed_create_args = set(["stream", "messages", "function_call", "functions", "n"])
required_create_args: Set[str] = set(["model"])

USER_AGENT_HEADER_NAME = "User-Agent"

try:
    version_info = version("autogen-ext")
except PackageNotFoundError:
    version_info = "dev"
AZURE_OPENAI_USER_AGENT = f"autogen-python/{version_info}"

# ###### PALAK (for online external assement progress)

import json
import re
import asyncio
import logging
from typing import Optional

judge_file_handler = logging.FileHandler("judge_log.jsonl", mode="a")
judge_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

_judge_logger = logging.getLogger("online_judge")
_judge_logger.setLevel(logging.DEBUG)
_judge_logger.addHandler(judge_file_handler)


TOTAL_ROUND = 20

##### with answer candidate detection
_JUDGE_SYSTEM_PROMPT = f"""You are an expert at analyzing AI agent tool call trajectories.

The agent you are analyzing is part of MagenticOne, a multi-agent system built on top of an LLM orchestrator.
MagenticOne consists of the following specialized agents that the orchestrator can delegate to:

- Orchestrator: The lead agent that plans, delegates tasks to other agents, and synthesizes results.
- WebSurfer: Browses the web, searches for information, and navigates web pages.
- FileSurfer: Reads and navigates local files (PDFs, text files, spreadsheets, etc.).
- Coder: Writes and reasons about code to solve problems programmatically.
- ComputerTerminal: Executes code and shell commands in a sandboxed environment.

Important: Even if early steps show WebSurfer or FileSurfer struggling, the orchestrator can \
fall back to Coder + ComputerTerminal to solve tasks programmatically. Do not penalize the agent \
prematurely for early failures if there are still unexplored avenues available.

The system has a maximum of {TOTAL_ROUND} rounds. The trajectory includes [Round X/{TOTAL_ROUND}] markers showing \
the orchestrator's self-assessment at each round boundary. Pay close attention to rounds remaining.

You will be given:
1. The original task/prompt.
2. The trajectory so far: tool calls with agent reasoning, round boundary assessments,
   and ComputerTerminal synthetic steps (purpose only, code hidden).
3. How many steps and rounds have elapsed.
4. Whether a candidate answer was already detected in a previous judge call.

Key pattern — answer candidate detection:
Set answer_candidate_detected=true if ANYWHERE in the trajectory the agent's reasoning or a tool \
output mentions any specific value that could be the answer — a number, a name, a date, a hex \
code, a count, etc. — even in passing, even without confidence.

How to weigh answer_candidate_detected against rounds remaining:
- answer_candidate_detected=true: strong signal of success.
- answer_candidate_detected=false: strong signal of failure, especially with few rounds left.

IMPORTANT — answer_candidate_detected is monotonic: if you are told a candidate was already \
detected in a previous call, you MUST set answer_candidate_detected=true regardless of whether \
recent steps re-state it.

Based on the task and the trajectory, predict whether the agent will ultimately succeed or fail.

Consider factors like:
- Is the agent's approach appropriate for the given task?
- Is the agent making progress toward answering the task or going in circles?
- Are the tool calls logical and building toward a solution?
- Are there signs of confusion, repetition, or errors that persist across multiple agents?
- Is the most recent tool result returning useful information or errors/empty results?
- Has the orchestrator tried or signaled intent to use Coder/ComputerTerminal as a fallback?
- Is the agent interpreting tool results correctly and adjusting its strategy?
- How many rounds remain (out of 20)? Is there realistically enough time to finish?
- What does the orchestrator's own self-assessment say about progress and loops?

Respond with EXACTLY one JSON object (no markdown, no explanation):
{{
  "prediction": "Correct" | "Incorrect",
  "confidence": <0.0-1.0>,
  "reasoning": "<one line reason, keep it very concise!>",
  "answer_candidate_detected": <true|false>
}}

Where:
- "Correct" means the task will be completed successfully.
- "Incorrect" means the task will fail. If you believe the failure is due to running out of rounds \
(the agent was still making meaningful progress but will hit the 20-round limit), you MUST include \
the exact token "max_round_reached" in your reasoning field so this can be tracked separately.
"""

# ---------------------------------------------------------------------------
# Task prompt extraction
# ---------------------------------------------------------------------------

def _extract_task_prompt(messages: list) -> Optional[str]:
    """Extract the original task from the first orchestrator gather_facts message.
    Looks for the filled-in ORCHESTRATOR_TASK_LEDGER_FACTS_PROMPT template."""
    for msg in messages:
        content = msg.get("content", "")
        if not isinstance(content, str):
            continue
        if "Here is the request:" in content and "Here is the pre-survey:" in content:
            try:
                start = content.index("Here is the request:") + len("Here is the request:")
                end = content.index("Here is the pre-survey:")
                return content[start:end].strip()
            except ValueError:
                continue
    return None


def _format_trajectory_for_judge(trajectory: list) -> str:
    """Format accumulated trajectory steps into a prompt string for the judge.
    Only the last step's tool output is included (same as offline script)."""
    if not trajectory:
        return "(no steps yet)"

    # _judge_logger.debug("raw trajectory: ", trajectory)

    lines = []
    last_tool_idx = None
    for i in range(len(trajectory) - 1, -1, -1):
        if trajectory[i].get("step_type") == "tool_call":
            last_tool_idx = i
            break

    for i, step in enumerate(trajectory):
        stype = step.get("step_type")
        # print("stype: ", stype)

        if stype == "progress_ledger":
            round_num = step.get("round_num", "?")
            assessment = step.get("assessment", {})
            lines.append(
                f"[Round {round_num}/{TOTAL_ROUND}] Orchestrator assessment:\n"
                f"  Task satisfied: {assessment.get('is_request_satisfied', '?')} "
                f"— {assessment.get('is_request_satisfied_reason', '')}\n"
                f"  In loop: {assessment.get('is_in_loop', '?')}\n"
                f"  Progress being made: {assessment.get('is_progress_being_made', '?')} "
                f"— {assessment.get('is_progress_being_made_reason', '')}"
            )

            # next_speaker may be a plain string or {"answer": "ComputerTerminal", ...}
            next_speaker_raw = assessment.get("next_speaker", "")
            print("next_speaker_raw: ", next_speaker_raw)
            reason_raw = ""
            if isinstance(next_speaker_raw, dict):
                next_speaker_raw = next_speaker_raw.get("answer", "")
                reason_raw = next_speaker_raw.get("reason", "")
            next_speaker_str = str(next_speaker_raw).strip().lower()
            print("next_speaker_str: ", next_speaker_str)

            if reason_raw == "":
                reason_raw = assessment.get("instruction_or_question_reason", "")
            if isinstance(reason_raw, dict):
                reason_raw = reason_raw.get("answer", reason_raw.get("reason", ""))
            reason = str(reason_raw)

            if next_speaker_str == "computerterminal":
                # if len(reason) > 400:
                #     reason = reason[:400] + "..."
                lines.append(
                    f"Step {step.get('step_num', '?')} [Synthetic]: ComputerTerminal invoked\n"
                    f"  Purpose: {reason}"
                )

        elif stype == "coder_reasoning":
            rc = step.get("reasoning_content", "")
            ct = step.get("content", "")
            lines.append(
                f"Step {step.get('step_num', '?')}: coder_agent "
                # f"(reasoning_content: {rc[:400] if rc else ''}, content: {ct[:200] if ct else ''})"
                f"(reasoning_content: {rc if rc else ''}, content: {ct if ct else ''})"
            )

        elif stype == "tool_call":
            is_last = (i == last_tool_idx)
            tool_name = step.get("tool_name", "unknown")
            args_str = step.get("args_str", "{}")

            line = f"Step {step.get('step_num', '?')}: {tool_name}({args_str})"
            reasoning = step.get("reasoning", "")
            if reasoning:
                # line += f"\n  Reasoning: {reasoning[:400]}"
                line += f"\n  Reasoning: {reasoning}"
            if is_last and step.get("tool_output"):
                output = step["tool_output"]
                # if len(output) > 800:
                #     output = output[:800] + "..."
                line += f"\n  Output:\n    {output}"
            lines.append(line)

    print(".join(lines): ", ("\n\n".join(lines)))
    return "\n\n".join(lines)


# ---------------------------------------------------------------------------
# LLM response parsing (mirrors predict_tool_calls.py)
# ---------------------------------------------------------------------------

def _extract_json_from_text_judge(text: str):
    """Try multiple strategies to extract a JSON prediction object."""
    if not text:
        return None, None
    text = text.strip()

    # Strategy 1: ```json ... ``` code block
    if "```" in text:
        for block in text.split("```"):
            block = block.strip()
            if block.startswith("json"):
                block = block[4:].strip()
            if block.startswith("{"):
                try:
                    return json.loads(block), "json_codeblock"
                except json.JSONDecodeError:
                    pass

    # Strategy 2: starts directly with {
    if text.startswith("{"):
        try:
            return json.loads(text), "direct_json"
        except json.JSONDecodeError:
            pass

    # Strategy 3: last { to end (handles Qwen3 prose-before-JSON pattern)
    last_brace = text.rfind("{")
    if last_brace != -1:
        try:
            return json.loads(text[last_brace:]), "last_brace_json"
        except json.JSONDecodeError:
            pass

    # Strategy 4: regex for any {...} containing "prediction"
    for m in re.finditer(r'\{[^{}]*"prediction"[^{}]*\}', text):
        try:
            return json.loads(m.group()), "regex_json"
        except json.JSONDecodeError:
            continue

    return None, None


def _parse_judge_response(content: str, reasoning_content: str):
    """Parse judge LLM response into structured prediction dict."""
    for label, text in [("content", content), ("reasoning", reasoning_content)]:
        if not text:
            continue
        parsed, strategy = _extract_json_from_text_judge(text)
        print("parsed judge response:" )
        print("parsed: ", parsed)
        print("strategy: ", strategy)
        if parsed and "prediction" in parsed:
            return parsed, f"{strategy}_in_{label}"

    # Heuristic fallback
    for label, text in [("reasoning", reasoning_content), ("content", content)]:
        if not text:
            continue
        cl = text.lower()
        if "prediction" in cl and ("correct" in cl or "incorrect" in cl):
            after_pred = cl.split("prediction")[-1][:80]
            pred = "Correct" if ("correct" in after_pred and "incorrect" not in after_pred) else "Incorrect"
            cm = re.search(r'confidence["\s:]+(\d+\.?\d*)', cl)
            conf = float(cm.group(1)) if cm else 0.5
            if conf > 1.0:
                conf /= 100.0
            return {
                "prediction": pred,
                "confidence": min(conf, 1.0),
                "reasoning": f"(heuristic) {text[-200:]}",
                "answer_candidate_detected": None,
            }, f"heuristic_from_{label}"

    return None, "failed"


# ---------------------------------------------------------------------------
# Fire-and-forget judge call
# ---------------------------------------------------------------------------

async def _fire_judge(task_id: str, client, model: str, task_state: dict) -> None:
    """
    Async fire-and-forget judge call.
    Updates PALAK_JUDGE_PREDICTION and PALAK_JUDGE_ACD on the class via task_state dict.
    task_state is a mutable dict of references into the class variables for this task_id.
    """
    print("PALAK: inside _fire_judge method: ")
    try:
        trajectory = task_state["trajectory"]
        task_prompt = task_state["task_prompt"]
        prior_acd = task_state["prior_acd"]
        step_count = task_state["step_count"]
        rounds_so_far = task_state["rounds_so_far"]
        rounds_remaining = TOTAL_ROUND - rounds_so_far
        # rounds_remaining = 20 - rounds_so_far

        # Build prompt section
        prompt_section = ""
        if task_prompt:
            # truncated = task_prompt[:5000] + ("..." if len(task_prompt) > 5000 else "")
            truncated = task_prompt
            prompt_section = f'Original task/prompt:\n"""{truncated}"""\n\n'
            print("prompt section: ", prompt_section)

        acd_note = (
            "IMPORTANT: A specific candidate answer value was already detected in an earlier "
            "judge call for this trajectory. You MUST set answer_candidate_detected=true."
            if prior_acd else
            "No candidate answer has been detected in earlier judge calls yet."
        )


        # print("trajectory: ", trajectory)
        trajectory_text = _format_trajectory_for_judge(trajectory)
        print("trajectory_text: ", trajectory_text)

        user_msg = (
            f"{prompt_section}"
            f"Trajectory context: {step_count} steps so far | "
            f"{rounds_so_far}/{TOTAL_ROUND} rounds elapsed | "
            f"~{rounds_remaining} rounds remaining.\n"
            f"{acd_note}\n\n"
            f"Trajectory:\n{trajectory_text}\n\n"
            f"Respond with EXACTLY one JSON object with fields: "
            f"prediction, confidence, reasoning, answer_candidate_detected."
        )

        print("user_msg: ", user_msg)

        messages = [
            {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ]

        print("messages: ", messages)

        _judge_logger.debug("[judge][%s] Firing judge call steps=%d rounds=%d/%d prior_acd=%s",
                            task_id, step_count, rounds_so_far, TOTAL_ROUND, prior_acd)

        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.0,
            max_tokens=1024,
            extra_body={"priority": 0},  # judge calls always jump the queue
        )

        print("judge_response: ", response)

        choice = response.choices[0]
        content = (choice.message.content or "").strip()
        reasoning_content = ""
        if choice.message.model_extra is not None:
            reasoning_content = choice.message.model_extra.get("reasoning_content", "") or ""

        parsed, method = _parse_judge_response(content, reasoning_content)

        if parsed:
            prediction = parsed.get("prediction", "Unknown")
            acd = parsed.get("answer_candidate_detected", False)
            reasoning = parsed.get("reasoning", "")

            # ACD is monotonic: once True stays True
            new_acd = prior_acd or (acd is True)

            task_state["prediction_out"] = prediction
            task_state["acd_out"] = new_acd

            _judge_logger.info(
                "[judge][%s] prediction=%s confidence=%.2f acd=%s (prior=%s) method=%s | %s",
                task_id, prediction, parsed.get("confidence", 0.0),
                new_acd, prior_acd, method, reasoning[:120],
            )
        else:
            _judge_logger.warning("[judge][%s] Parse failed content=%.100s", task_id, content)
            task_state["prediction_out"] = None
            task_state["acd_out"] = prior_acd  # preserve prior state on failure

    except Exception as e:
        print("An exception occurred inside _fire_judge: ", e)
        _judge_logger.exception("[judge][%s] Judge call failed: %s", task_id, e)
        task_state["prediction_out"] = None
        task_state["acd_out"] = task_state.get("prior_acd", False)
    finally:
        task_state["firing_done"] = True


# _STEP_GROUPS = [(10, 1), (20, 5), (30, 10), (40, 15)]
_STEP_GROUPS = [(10, 1), (20, 5), (30, 10), (40, 15)]
_STEP_MAX_PRIORITY = 20
_STEP_MAX_PENALTY  = 25

def _step_priority(step_count: int) -> int:
    for threshold, priority in _STEP_GROUPS:
        if step_count <= threshold:
            return priority
    return _STEP_MAX_PRIORITY

def _next_group_priority(current_step_priority: int) -> int:
    priorities = [p for _, p in _STEP_GROUPS] + [_STEP_MAX_PRIORITY]
    try:
        idx = priorities.index(current_step_priority)
        if idx + 1 < len(priorities):
            return priorities[idx + 1]
    except ValueError:
        pass
    return _STEP_MAX_PENALTY


# ###### PALAK
from datetime import datetime
class LogHandler(logging.FileHandler):
    def __init__(self, filename: str = "magentic_one_log.jsonl", print_message: bool = True) -> None:
        super().__init__(filename, mode="w")
        self.print_message = print_message

    def emit(self, record: logging.LogRecord) -> None:
        # print("PALAK: here's out logger emit event :)")
        # print("PALAK: record: ", record)
        # print("PALAK: type(record): ", type(record))
        try:
            ts = datetime.fromtimestamp(record.created).isoformat()
            msg = record.msg
            if isinstance(msg, LLMCallEvent):
                payload = {
                    "timestamp": ts,
                    "type": "LLMCall",
                    "messages": msg.kwargs.get("messages"),
                    "response": msg.kwargs.get("response"),
                    "tools": msg.kwargs.get("tools"),
                    "prompt_tokens": msg.kwargs.get("prompt_tokens"),
                    "completion_tokens": msg.kwargs.get("completion_tokens"),
                }
            
                original_msg = record.msg
                record.msg = json.dumps(payload, ensure_ascii=False)
                super().emit(record)
                record.msg = original_msg

        except Exception:
            print("error in logHandler.emit", flush=True)
            self.handleError(record)

file_handler = LogHandler()
file_handler.setFormatter(logging.Formatter("%(message)s"))
event_logger = logging.getLogger(EVENT_LOGGER_NAME)
event_logger.setLevel(logging.DEBUG)
event_logger.addHandler(file_handler)



def _azure_openai_client_from_config(config: Mapping[str, Any]) -> AsyncAzureOpenAI:
    # Take a copy
    copied_config = dict(config).copy()
    # Shave down the config to just the AzureOpenAIChatCompletionClient kwargs
    azure_config = {k: v for k, v in copied_config.items() if k in aopenai_init_kwargs}

    DEFAULT_HEADERS_KEY = "default_headers"
    if DEFAULT_HEADERS_KEY not in azure_config:
        azure_config[DEFAULT_HEADERS_KEY] = {}

    azure_config[DEFAULT_HEADERS_KEY][USER_AGENT_HEADER_NAME] = (
        f"{AZURE_OPENAI_USER_AGENT} {azure_config[DEFAULT_HEADERS_KEY][USER_AGENT_HEADER_NAME]}"
        if USER_AGENT_HEADER_NAME in azure_config[DEFAULT_HEADERS_KEY]
        else AZURE_OPENAI_USER_AGENT
    )

    return AsyncAzureOpenAI(**azure_config)


def _openai_client_from_config(config: Mapping[str, Any]) -> AsyncOpenAI:
    # Shave down the config to just the OpenAI kwargs
    openai_config = {k: v for k, v in config.items() if k in openai_init_kwargs}
    return AsyncOpenAI(**openai_config)


def _create_args_from_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    create_args = {k: v for k, v in config.items() if k in create_kwargs}
    create_args_keys = set(create_args.keys())
    if not required_create_args.issubset(create_args_keys):
        raise ValueError(f"Required create args are missing: {required_create_args - create_args_keys}")
    if disallowed_create_args.intersection(create_args_keys):
        raise ValueError(f"Disallowed create args are present: {disallowed_create_args.intersection(create_args_keys)}")
    return create_args


# TODO check types
# oai_system_message_schema = type2schema(ChatCompletionSystemMessageParam)
# oai_user_message_schema = type2schema(ChatCompletionUserMessageParam)
# oai_assistant_message_schema = type2schema(ChatCompletionAssistantMessageParam)
# oai_tool_message_schema = type2schema(ChatCompletionToolMessageParam)


def type_to_role(message: LLMMessage) -> ChatCompletionRole:
    if isinstance(message, SystemMessage):
        return "system"
    elif isinstance(message, UserMessage):
        return "user"
    elif isinstance(message, AssistantMessage):
        return "assistant"
    else:
        return "tool"


def to_oai_type(
    message: LLMMessage,
    prepend_name: bool = False,
    model: str = "unknown",
    model_family: str = ModelFamily.UNKNOWN,
    include_name_in_message: bool = True,
) -> Sequence[ChatCompletionMessageParam]:
    context = {
        "prepend_name": prepend_name,
        "include_name_in_message": include_name_in_message,
    }
    transformers = get_transformer("openai", model, model_family)

    def raise_value_error(message: LLMMessage, context: Dict[str, Any]) -> Sequence[ChatCompletionMessageParam]:
        raise ValueError(f"Unknown message type: {type(message)}")

    transformer: Callable[[LLMMessage, Dict[str, Any]], Sequence[ChatCompletionMessageParam]] = transformers.get(
        type(message), raise_value_error
    )
    result = transformer(message, context)
    return result


def calculate_vision_tokens(image: Image, detail: str = "auto") -> int:
    MAX_LONG_EDGE = 2048
    BASE_TOKEN_COUNT = 85
    TOKENS_PER_TILE = 170
    MAX_SHORT_EDGE = 768
    TILE_SIZE = 512

    if detail == "low":
        return BASE_TOKEN_COUNT

    width, height = image.image.size

    # Scale down to fit within a MAX_LONG_EDGE x MAX_LONG_EDGE square if necessary

    if width > MAX_LONG_EDGE or height > MAX_LONG_EDGE:
        aspect_ratio = width / height
        if aspect_ratio > 1:
            # Width is greater than height
            width = MAX_LONG_EDGE
            height = int(MAX_LONG_EDGE / aspect_ratio)
        else:
            # Height is greater than or equal to width
            height = MAX_LONG_EDGE
            width = int(MAX_LONG_EDGE * aspect_ratio)

    # Resize such that the shortest side is MAX_SHORT_EDGE if both dimensions exceed MAX_SHORT_EDGE
    aspect_ratio = width / height
    if width > MAX_SHORT_EDGE and height > MAX_SHORT_EDGE:
        if aspect_ratio > 1:
            # Width is greater than height
            height = MAX_SHORT_EDGE
            width = int(MAX_SHORT_EDGE * aspect_ratio)
        else:
            # Height is greater than or equal to width
            width = MAX_SHORT_EDGE
            height = int(MAX_SHORT_EDGE / aspect_ratio)

    # Calculate the number of tiles based on TILE_SIZE

    tiles_width = math.ceil(width / TILE_SIZE)
    tiles_height = math.ceil(height / TILE_SIZE)
    total_tiles = tiles_width * tiles_height
    # Calculate the total tokens based on the number of tiles and the base token count

    total_tokens = BASE_TOKEN_COUNT + TOKENS_PER_TILE * total_tiles

    return total_tokens


def _add_usage(usage1: RequestUsage, usage2: RequestUsage) -> RequestUsage:
    return RequestUsage(
        prompt_tokens=usage1.prompt_tokens + usage2.prompt_tokens,
        completion_tokens=usage1.completion_tokens + usage2.completion_tokens,
    )


def convert_tools(
    tools: Sequence[Tool | ToolSchema],
) -> List[ChatCompletionToolParam]:
    result: List[ChatCompletionToolParam] = []
    for tool in tools:
        if isinstance(tool, Tool):
            tool_schema = tool.schema
        else:
            assert isinstance(tool, dict)
            tool_schema = tool

        result.append(
            ChatCompletionToolParam(
                type="function",
                function=FunctionDefinition(
                    name=tool_schema["name"],
                    description=(tool_schema["description"] if "description" in tool_schema else ""),
                    parameters=(
                        cast(FunctionParameters, tool_schema["parameters"]) if "parameters" in tool_schema else {}
                    ),
                    strict=(tool_schema["strict"] if "strict" in tool_schema else False),
                ),
            )
        )
    # Check if all tools have valid names.
    for tool_param in result:
        assert_valid_name(tool_param["function"]["name"])
    return result


def convert_tool_choice(tool_choice: Tool | Literal["auto", "required", "none"]) -> Any:
    """Convert tool_choice parameter to OpenAI API format.

    Args:
        tool_choice: A single Tool object to force the model to use, "auto" to let the model choose any available tool, "required" to force tool usage, or "none" to disable tool usage.

    Returns:
        OpenAI API compatible tool_choice value or None if not specified.
    """
    if tool_choice == "none":
        return "none"

    if tool_choice == "auto":
        return "auto"

    if tool_choice == "required":
        return "required"

    # Must be a Tool object
    if isinstance(tool_choice, Tool):
        return {"type": "function", "function": {"name": tool_choice.schema["name"]}}
    else:
        raise ValueError(f"tool_choice must be a Tool object, 'auto', 'required', or 'none', got {type(tool_choice)}")


def normalize_name(name: str) -> str:
    """
    LLMs sometimes ask functions while ignoring their own format requirements, this function should be used to replace invalid characters with "_".

    Prefer _assert_valid_name for validating user configuration or input
    """
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name)[:64]


def count_tokens_openai(
    messages: Sequence[LLMMessage],
    model: str,
    *,
    add_name_prefixes: bool = False,
    tools: Sequence[Tool | ToolSchema] = [],
    model_family: str = ModelFamily.UNKNOWN,
    include_name_in_message: bool = True,
) -> int:
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        # trace_logger.warning(f"Model {model} not found. Using cl100k_base encoding.") #### PALAK
        encoding = tiktoken.get_encoding("cl100k_base")
    tokens_per_message = 3
    tokens_per_name = 1
    num_tokens = 0

    # Message tokens.
    for message in messages:
        num_tokens += tokens_per_message
        oai_message = to_oai_type(
            message,
            prepend_name=add_name_prefixes,
            model=model,
            model_family=model_family,
            include_name_in_message=include_name_in_message,
        )
        for oai_message_part in oai_message:
            for key, value in oai_message_part.items():
                if value is None:
                    continue

                if isinstance(message, UserMessage) and isinstance(value, list):
                    typed_message_value = cast(List[ChatCompletionContentPartParam], value)

                    assert len(typed_message_value) == len(
                        message.content
                    ), "Mismatch in message content and typed message value"

                    # We need image properties that are only in the original message
                    for part, content_part in zip(typed_message_value, message.content, strict=False):
                        if isinstance(content_part, Image):
                            # TODO: add detail parameter
                            num_tokens += calculate_vision_tokens(content_part)
                        elif isinstance(part, str):
                            num_tokens += len(encoding.encode(part))
                        else:
                            try:
                                serialized_part = json.dumps(part)
                                num_tokens += len(encoding.encode(serialized_part))
                            except TypeError:
                                trace_logger.warning(f"Could not convert {part} to string, skipping.")
                else:
                    if not isinstance(value, str):
                        try:
                            value = json.dumps(value)
                        except TypeError:
                            trace_logger.warning(f"Could not convert {value} to string, skipping.")
                            continue
                    num_tokens += len(encoding.encode(value))
                    if key == "name":
                        num_tokens += tokens_per_name
    num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>

    # Tool tokens.
    oai_tools = convert_tools(tools)
    for tool in oai_tools:
        function = tool["function"]
        tool_tokens = len(encoding.encode(function["name"]))
        if "description" in function:
            tool_tokens += len(encoding.encode(function["description"]))
        tool_tokens -= 2
        if "parameters" in function:
            parameters = function["parameters"]
            if "properties" in parameters:
                assert isinstance(parameters["properties"], dict)
                for propertiesKey in parameters["properties"]:  # pyright: ignore
                    assert isinstance(propertiesKey, str)
                    tool_tokens += len(encoding.encode(propertiesKey))
                    v = parameters["properties"][propertiesKey]  # pyright: ignore
                    for field in v:  # pyright: ignore
                        if field == "type":
                            tool_tokens += 2
                            tool_tokens += len(encoding.encode(v["type"]))  # pyright: ignore
                        elif field == "description":
                            tool_tokens += 2
                            tool_tokens += len(encoding.encode(v["description"]))  # pyright: ignore
                        elif field == "anyOf":
                            tool_tokens -= 3
                            for o in v["anyOf"]:  # type: ignore
                                tool_tokens += 3
                                tool_tokens += len(encoding.encode(str(o["type"])))  # pyright: ignore
                        elif field == "default":
                            tool_tokens += 2
                            tool_tokens += len(encoding.encode(json.dumps(v["default"])))
                        elif field == "title":
                            tool_tokens += 2
                            tool_tokens += len(encoding.encode(str(v["title"])))  # pyright: ignore
                        elif field == "enum":
                            tool_tokens -= 3
                            for o in v["enum"]:  # pyright: ignore
                                tool_tokens += 3
                                tool_tokens += len(encoding.encode(o))  # pyright: ignore
                        else:
                            trace_logger.warning(f"Not supported field {field}")
                tool_tokens += 11
                if len(parameters["properties"]) == 0:  # pyright: ignore
                    tool_tokens -= 2
        num_tokens += tool_tokens

    if oai_tools:
        num_tokens += 12
    return num_tokens


@dataclass
class CreateParams:
    messages: List[ChatCompletionMessageParam]
    tools: List[ChatCompletionToolParam]
    response_format: Optional[Type[BaseModel]]
    create_args: Dict[str, Any]


class BaseOpenAIChatCompletionClient(ChatCompletionClient):
    # ##### PALAK
    PALAK_TASK_STEP_COUNT = {}
    PALAK_TASK_ORCHESTRATOR_SIGNALS = {}
    # ---- new judge state (all keyed by task_id) ----
    PALAK_JUDGE_PREDICTION = {}   # task_id -> "Correct" | "Incorrect" | None
    PALAK_JUDGE_ACD = {}          # task_id -> bool (running ACD state passed forward)
    PALAK_JUDGE_FIRING = {}       # task_id -> bool (True while a judge call is in-flight)
    PALAK_TASK_TRAJECTORY = {}    # task_id -> list of step dicts
    PALAK_TASK_PROMPT = {}        # task_id -> str (original task question)
    PALAK_ROUND_COUNT = {}        # task_id -> int (progress_ledger entries seen)
    PALAK_JUDGE_HISTORY = {}
    PALAK_JUDGE_LAST_PRIORITY = {} 

    def __init__(
        self,
        client: Union[AsyncOpenAI, AsyncAzureOpenAI],
        *,
        create_args: Dict[str, Any],
        model_capabilities: Optional[ModelCapabilities] = None,  # type: ignore
        model_info: Optional[ModelInfo] = None,
        add_name_prefixes: bool = False,
        include_name_in_message: bool = True,
    ):
        print("inside BaseOpenAIChatCompletionClient init method :)")
        self._client = client
        self._add_name_prefixes = add_name_prefixes
        self._include_name_in_message = include_name_in_message
        if model_capabilities is None and model_info is None:
            try:
                self._model_info = _model_info.get_info(create_args["model"])
            except KeyError as err:
                raise ValueError("model_info is required when model name is not a valid OpenAI model") from err
        elif model_capabilities is not None and model_info is not None:
            raise ValueError("model_capabilities and model_info are mutually exclusive")
        elif model_capabilities is not None and model_info is None:
            warnings.warn(
                "model_capabilities is deprecated, use model_info instead",
                DeprecationWarning,
                stacklevel=2,
            )
            info = cast(ModelInfo, model_capabilities)
            info["family"] = ModelFamily.UNKNOWN
            self._model_info = info
        elif model_capabilities is None and model_info is not None:
            self._model_info = model_info

        # Validate model_info, check if all required fields are present
        validate_model_info(self._model_info)

        self._resolved_model: Optional[str] = None
        if "model" in create_args:
            self._resolved_model = _model_info.resolve_model(create_args["model"])

        if (
            not self._model_info["json_output"]
            and "response_format" in create_args
            and (
                isinstance(create_args["response_format"], dict)
                and create_args["response_format"]["type"] == "json_object"
            )
        ):
            raise ValueError("Model does not support JSON output.")

        self._create_args = create_args
        self._total_usage = RequestUsage(prompt_tokens=0, completion_tokens=0)
        self._actual_usage = RequestUsage(prompt_tokens=0, completion_tokens=0)

    @classmethod
    def create_from_config(cls, config: Dict[str, Any]) -> ChatCompletionClient:
        return OpenAIChatCompletionClient(**config)

    def _rstrip_last_assistant_message(self, messages: Sequence[LLMMessage]) -> Sequence[LLMMessage]:
        """
        Remove the last assistant message if it is empty.
        """
        # When Claude models last message is AssistantMessage, It could not end with whitespace
        if isinstance(messages[-1], AssistantMessage):
            if isinstance(messages[-1].content, str):
                messages[-1].content = messages[-1].content.rstrip()

        return messages

    def _process_create_args(
        self,
        messages: Sequence[LLMMessage],
        tools: Sequence[Tool | ToolSchema],
        tool_choice: Tool | Literal["auto", "required", "none"],
        json_output: Optional[bool | type[BaseModel]],
        extra_create_args: Mapping[str, Any],
    ) -> CreateParams:
        # Make sure all extra_create_args are valid
        extra_create_args_keys = set(extra_create_args.keys())
        if not create_kwargs.issuperset(extra_create_args_keys):
            raise ValueError(f"Extra create args are invalid: {extra_create_args_keys - create_kwargs}")

        # Copy the create args and overwrite anything in extra_create_args
        create_args = self._create_args.copy()
        create_args.update(extra_create_args)

        # The response format value to use for the beta client.
        response_format_value: Optional[Type[BaseModel]] = None

        if "response_format" in create_args:
            # Legacy support for getting beta client mode from response_format.
            value = create_args["response_format"]
            if isinstance(value, type) and issubclass(value, BaseModel):
                if self.model_info["structured_output"] is False:
                    raise ValueError("Model does not support structured output.")
                warnings.warn(
                    "Using response_format to specify the BaseModel for structured output type will be deprecated. "
                    "Use json_output in create and create_stream instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                response_format_value = value
                # Remove response_format from create_args to prevent passing it twice.
                del create_args["response_format"]
            # In all other cases when response_format is set to something else, we will
            # use the regular client.

        if json_output is not None:
            if self.model_info["json_output"] is False and json_output is True:
                raise ValueError("Model does not support JSON output.")
            if json_output is True:
                # JSON mode.
                create_args["response_format"] = ResponseFormatJSONObject(type="json_object")
            elif json_output is False:
                # Text mode.
                create_args["response_format"] = ResponseFormatText(type="text")
            elif isinstance(json_output, type) and issubclass(json_output, BaseModel):
                if self.model_info["structured_output"] is False:
                    raise ValueError("Model does not support structured output.")
                if response_format_value is not None:
                    raise ValueError(
                        "response_format and json_output cannot be set to a Pydantic model class at the same time."
                    )
                # Beta client mode with Pydantic model class.
                response_format_value = json_output
            else:
                raise ValueError(f"json_output must be a boolean or a Pydantic model class, got {type(json_output)}")

        if response_format_value is not None and "response_format" in create_args:
            warnings.warn(
                "response_format is found in extra_create_args while json_output is set to a Pydantic model class. "
                "Skipping the response_format in extra_create_args in favor of the json_output. "
                "Structured output will be used.",
                UserWarning,
                stacklevel=2,
            )
            # If using beta client, remove response_format from create_args to prevent passing it twice
            del create_args["response_format"]

        # TODO: allow custom handling.
        # For now we raise an error if images are present and vision is not supported
        if self.model_info["vision"] is False:
            for message in messages:
                if isinstance(message, UserMessage):
                    if isinstance(message.content, list) and any(isinstance(x, Image) for x in message.content):
                        raise ValueError("Model does not support vision and image was provided")

        if self.model_info["json_output"] is False and json_output is True:
            raise ValueError("Model does not support JSON output.")

        if not self.model_info.get("multiple_system_messages", False):
            # Some models accept only one system message(or, it will read only the last one)
            # So, merge system messages into one (if multiple and continuous)
            system_message_content = ""
            _messages: List[LLMMessage] = []
            _first_system_message_idx = -1
            _last_system_message_idx = -1
            # Index of the first system message for adding the merged system message at the correct position
            for idx, message in enumerate(messages):
                if isinstance(message, SystemMessage):
                    if _first_system_message_idx == -1:
                        _first_system_message_idx = idx
                    elif _last_system_message_idx + 1 != idx:
                        # That case, system message is not continuous
                        # Merge system messages only contiues system messages
                        raise ValueError(
                            "Multiple and Not continuous system messages are not supported if model_info['multiple_system_messages'] is False"
                        )
                    system_message_content += message.content + "\n"
                    _last_system_message_idx = idx
                else:
                    _messages.append(message)
            system_message_content = system_message_content.rstrip()
            if system_message_content != "":
                system_message = SystemMessage(content=system_message_content)
                _messages.insert(_first_system_message_idx, system_message)
            messages = _messages

        # in that case, for ad-hoc, we using startswith instead of model_family for code consistency
        if create_args.get("model", "unknown").startswith("claude-"):
            # When Claude models last message is AssistantMessage, It could not end with whitespace
            messages = self._rstrip_last_assistant_message(messages)

        oai_messages_nested = [
            to_oai_type(
                m,
                prepend_name=self._add_name_prefixes,
                model=create_args.get("model", "unknown"),
                model_family=self._model_info["family"],
                include_name_in_message=self._include_name_in_message,
            )
            for m in messages
        ]

        oai_messages = [item for sublist in oai_messages_nested for item in sublist]

        if self.model_info["function_calling"] is False and len(tools) > 0:
            raise ValueError("Model does not support function calling")

        converted_tools = convert_tools(tools)

        # Process tool_choice parameter
        if isinstance(tool_choice, Tool):
            if len(tools) == 0:
                raise ValueError("tool_choice specified but no tools provided")

            # Validate that the tool exists in the provided tools
            tool_names_available: List[str] = []
            for tool in tools:
                if isinstance(tool, Tool):
                    tool_names_available.append(tool.schema["name"])
                else:
                    tool_names_available.append(tool["name"])

            # tool_choice is a single Tool object
            tool_name = tool_choice.schema["name"]
            if tool_name not in tool_names_available:
                raise ValueError(f"tool_choice references '{tool_name}' but it's not in the provided tools")

        if len(converted_tools) > 0:
            # Convert to OpenAI format and add to create_args
            converted_tool_choice = convert_tool_choice(tool_choice)
            create_args["tool_choice"] = converted_tool_choice

        return CreateParams(
            messages=oai_messages,
            tools=converted_tools,
            response_format=response_format_value,
            create_args=create_args,
        )

    #### PALAK: external assessment progress
    async def create(
        self,
        messages,
        *,
        tools=[],
        tool_choice="auto",
        json_output=None,
        extra_create_args={},
        cancellation_token=None,
        custom_request_id=None,
    ):
        create_params = self._process_create_args(
            messages, tools, tool_choice, json_output, extra_create_args,
        )

        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).isoformat(timespec="microseconds")

        # ----------------------------------------------------------------
        # Task identification and step counting (unchanged)
        # ----------------------------------------------------------------
        task_id = custom_request_id.split('_')[-1]

        if task_id not in BaseOpenAIChatCompletionClient.PALAK_TASK_STEP_COUNT:
            BaseOpenAIChatCompletionClient.PALAK_TASK_STEP_COUNT[task_id] = 0
        BaseOpenAIChatCompletionClient.PALAK_TASK_STEP_COUNT[task_id] += 1
        step_count = BaseOpenAIChatCompletionClient.PALAK_TASK_STEP_COUNT[task_id]

        # Identify which phase/agent this call is from
        agent_phases = [
            "file_surfer", "orchestrator_create_plan", "orchestrator_gather_facts",
            "orchestrator_prepare_final_ans", "web_surfer_summarize_agent",
            "web_surfer_generate_agent", "coder", "orchestrator_update_plan",
            "orchestrator_update_fact", "progress_ledger",
        ]
        which_phase = "file_surfer"
        for key in agent_phases:
            if key in custom_request_id:
                which_phase = key
                break

        # ----------------------------------------------------------------
        # Extract task prompt once (from first orchestrator gather_facts call)
        # ----------------------------------------------------------------
        if (task_id not in BaseOpenAIChatCompletionClient.PALAK_TASK_PROMPT
                and which_phase == "orchestrator_gather_facts"):
            task_prompt = _extract_task_prompt(create_params.messages)
            if task_prompt:
                print("Here's the task prompt: ", task_prompt)
                BaseOpenAIChatCompletionClient.PALAK_TASK_PROMPT[task_id] = task_prompt
                _judge_logger.debug("[judge][%s] Task prompt extracted: %.80s", task_id, task_prompt)


        if task_id not in BaseOpenAIChatCompletionClient.PALAK_JUDGE_HISTORY:
            BaseOpenAIChatCompletionClient.PALAK_JUDGE_HISTORY[task_id] = []

        base_priority    = _step_priority(step_count)
        latest_judge     = BaseOpenAIChatCompletionClient.PALAK_JUDGE_PREDICTION.get(task_id)
        judge_history    = BaseOpenAIChatCompletionClient.PALAK_JUDGE_HISTORY.get(task_id, [])

        #### orbit-6 : orbit-2-1
        if step_count <= 10:
            task_priority = 1
        elif latest_judge is None:
            task_priority = BaseOpenAIChatCompletionClient.PALAK_JUDGE_LAST_PRIORITY[task_id]
        elif latest_judge == "Correct":
            task_priority = max(2, BaseOpenAIChatCompletionClient.PALAK_JUDGE_LAST_PRIORITY[task_id] - 1)
        else:
            task_priority = BaseOpenAIChatCompletionClient.PALAK_JUDGE_LAST_PRIORITY[task_id] + 1
        print(f"[{task_id}] step={step_count} base={base_priority} "
            f"judge={latest_judge} final_priority={task_priority}")

        BaseOpenAIChatCompletionClient.PALAK_JUDGE_LAST_PRIORITY[task_id] = task_priority

        # ----------------------------------------------------------------
        # Send the actual LLM request (unchanged logic, priority now live)
        # ----------------------------------------------------------------
        print(f"PALAK: IMPORTANT: [{ts}] SENT THIS REQUEST: {custom_request_id}")
        if create_params.response_format is not None:
            future = asyncio.ensure_future(
                self._client.beta.chat.completions.parse(
                    messages=create_params.messages,
                    tools=(create_params.tools if len(create_params.tools) > 0 else NOT_GIVEN),
                    response_format=create_params.response_format,
                    **create_params.create_args,
                    extra_headers={"x-request-id": custom_request_id},
                    extra_body={"priority": task_priority},
                )
            )
        else:
            future = asyncio.ensure_future(
                self._client.chat.completions.create(
                    messages=create_params.messages,
                    stream=False,
                    tools=(create_params.tools if len(create_params.tools) > 0 else NOT_GIVEN),
                    **create_params.create_args,
                    extra_headers={"x-request-id": custom_request_id},
                    extra_body={"priority": task_priority},
                )
            )

        if cancellation_token is not None:
            cancellation_token.link_future(future)
        result = await future
        if create_params.response_format is not None:
            result = cast(ParsedChatCompletion[Any], result)

        # ----------------------------------------------------------------
        # Usage (unchanged)
        # ----------------------------------------------------------------
        usage = RequestUsage(
            prompt_tokens=getattr(result.usage, "prompt_tokens", 0) if result.usage else 0,
            completion_tokens=getattr(result.usage, "completion_tokens", 0) if result.usage else 0,
        )

        # ----------------------------------------------------------------
        # Log the LLM call event (unchanged)
        # ----------------------------------------------------------------
        temp_LLMCallEvent = LLMCallEvent(
            messages=cast(List[Dict[str, Any]], create_params.messages),
            response=result.model_dump(),
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            tools=create_params.tools,
        )
        event_logger.info(temp_LLMCallEvent)

        # ----------------------------------------------------------------
        # Orchestrator signals (unchanged)
        # ----------------------------------------------------------------
        try:
            parsed_result = result.model_dump()["choices"][0]["message"]["parsed"]
            if parsed_result and "is_in_loop" in parsed_result and "is_progress_being_made" in parsed_result:
                is_loop = parsed_result["is_in_loop"]["answer"]
                is_progress = parsed_result["is_progress_being_made"]["answer"]
                if task_id not in BaseOpenAIChatCompletionClient.PALAK_TASK_ORCHESTRATOR_SIGNALS:
                    BaseOpenAIChatCompletionClient.PALAK_TASK_ORCHESTRATOR_SIGNALS[task_id] = []
                BaseOpenAIChatCompletionClient.PALAK_TASK_ORCHESTRATOR_SIGNALS[task_id].append(
                    (is_loop, is_progress)
                )
        except Exception:
            pass

        # ----------------------------------------------------------------
        # NEW: Accumulate trajectory step
        # ----------------------------------------------------------------
        result_dump = result.model_dump()
        choice_msg = result_dump["choices"][0]["message"]

        if task_id not in BaseOpenAIChatCompletionClient.PALAK_TASK_TRAJECTORY:
            BaseOpenAIChatCompletionClient.PALAK_TASK_TRAJECTORY[task_id] = []
        if task_id not in BaseOpenAIChatCompletionClient.PALAK_ROUND_COUNT:
            BaseOpenAIChatCompletionClient.PALAK_ROUND_COUNT[task_id] = 0

        step_num = step_count
        is_progress_ledger = (which_phase == "progress_ledger")

        if is_progress_ledger:
            # Increment round counter
            BaseOpenAIChatCompletionClient.PALAK_ROUND_COUNT[task_id] += 1
            round_num = BaseOpenAIChatCompletionClient.PALAK_ROUND_COUNT[task_id]

            # Extract orchestrator assessment from parsed result
            assessment = {}

            try:
                p = choice_msg.get("parsed") or {}

                def _flat(val, fallback=""):
                    if isinstance(val, dict):
                        return val.get("answer", val.get("reason", fallback))
                    return val if val is not None else fallback

                # p may be a Pydantic LedgerEntry object or a plain dict
                # handle both with getattr-with-dict-fallback
                def _get(obj, key, fallback=None):
                    if isinstance(obj, dict):
                        return obj.get(key, fallback)
                    return getattr(obj, key, fallback)

                is_req_sat     = _get(p, "is_request_satisfied", {})
                is_loop        = _get(p, "is_in_loop", {})
                is_progress    = _get(p, "is_progress_being_made", {})
                next_spk       = _get(p, "next_speaker", "")
                iq_reason      = _get(p, "instruction_or_question_reason", "")

                assessment = {
                    "is_request_satisfied":        _flat(_get(is_req_sat,  "answer", "?") if is_req_sat else "?"),
                    "is_request_satisfied_reason": _flat(_get(is_req_sat,  "reason", "")  if is_req_sat else ""),
                    "is_in_loop":                  _flat(_get(is_loop,     "answer", "?") if is_loop    else "?"),
                    "is_progress_being_made":      _flat(_get(is_progress, "answer", "?") if is_progress else "?"),
                    "is_progress_being_made_reason": _flat(_get(is_progress, "reason", "") if is_progress else ""),
                    "next_speaker":                _flat(next_spk),
                    "instruction_or_question_reason": _flat(iq_reason),
                }
            except Exception as e:
                _judge_logger.debug("[judge][%s] assessment extraction failed: %s", task_id, e)
                pass

            BaseOpenAIChatCompletionClient.PALAK_TASK_TRAJECTORY[task_id].append({
                "step_type": "progress_ledger",
                "step_num": step_num,
                "round_num": round_num,
                "assessment": assessment,
            })

        elif which_phase == "coder":
            reasoning_content = ""
            content_text = choice_msg.get("content", "")
            if choice_msg.get("model_extra"):
                reasoning_content = choice_msg["model_extra"].get("reasoning_content", "") or ""

            BaseOpenAIChatCompletionClient.PALAK_TASK_TRAJECTORY[task_id].append({
                "step_type": "coder_reasoning",
                "step_num": step_num,
                # "reasoning_content": reasoning_content[:800] if reasoning_content else "",
                "reasoning_content": reasoning_content,
                # "content": content_text[:400] if content_text else "",
                "content": content_text,
            })

        else:
            # Tool call step (WebSurfer, FileSurfer, etc.)
            tool_calls = choice_msg.get("tool_calls") or []
            tool_name = "unknown"
            args_str = "{}"
            if tool_calls:
                fn = tool_calls[0].get("function", {})
                tool_name = fn.get("name", "unknown")
                args_raw = fn.get("arguments", "{}")
                try:
                    args_parsed = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
                    args_str = json.dumps(args_parsed, ensure_ascii=False)
                    # if len(args_str) > 2000:
                    #     args_str = args_str[:2000] + "..."
                except Exception:
                    # args_str = str(args_raw)[:2000]
                    args_str = str(args_raw)

            # Reasoning: from thought/reasoning_content if available
            reasoning = ""
            if choice_msg.get("model_extra"):
                reasoning = choice_msg["model_extra"].get("reasoning_content", "") or ""
            if not reasoning and choice_msg.get("content"):
                reasoning = choice_msg.get("content", "")
            # if len(reasoning) > 2000:
            #     reasoning = reasoning[:2000] + "..."

            # Tool output: extract from the messages that were passed IN
            # (the tool result messages from the previous turn, present in create_params.messages)
            tool_output = ""
            for msg in reversed(create_params.messages):
                role = msg.get("role", "")
                if role == "tool":
                    raw = msg.get("content", "")
                    if isinstance(raw, list):
                        parts = [p.get("text", "") for p in raw if isinstance(p, dict)]
                        tool_output = " ".join(parts)
                    else:
                        tool_output = str(raw)
                    if len(tool_output) > 2000:
                        tool_output = tool_output[:2000] + "..."
                    break  # only the most recent tool result

            BaseOpenAIChatCompletionClient.PALAK_TASK_TRAJECTORY[task_id].append({
                "step_type": "tool_call",
                "step_num": step_num,
                "tool_name": tool_name,
                "args_str": args_str,
                "reasoning": reasoning,
                "tool_output": tool_output,  # only last step's output used at format time
            })

        # ----------------------------------------------------------------
        # NEW: Fire judge if conditions met
        # step_count > 10 AND this is a progress_ledger entry
        # AND no judge call currently in-flight
        # ----------------------------------------------------------------
        print("step count > 10: ", step_count > 10)
        print("is_progress_ledger: ", is_progress_ledger)
        print("not BaseOpenAIChatCompletionClient.PALAK_JUDGE_FIRING.get(task_id, False): ", (not BaseOpenAIChatCompletionClient.PALAK_JUDGE_FIRING.get(task_id, False)))
        if (
            step_count > 10
            and is_progress_ledger
            and not BaseOpenAIChatCompletionClient.PALAK_JUDGE_FIRING.get(task_id, False)
        ):
            BaseOpenAIChatCompletionClient.PALAK_JUDGE_FIRING[task_id] = True

            # Snapshot the state to pass into the coroutine
            # (trajectory is a list ref — we pass a shallow copy so appends
            #  during the async call don't affect what the judge sees)
            task_state = {
                "trajectory": list(BaseOpenAIChatCompletionClient.PALAK_TASK_TRAJECTORY[task_id]),
                "task_prompt": BaseOpenAIChatCompletionClient.PALAK_TASK_PROMPT.get(task_id, ""),
                "prior_acd": BaseOpenAIChatCompletionClient.PALAK_JUDGE_ACD.get(task_id, False),
                "step_count": step_count,
                "rounds_so_far": BaseOpenAIChatCompletionClient.PALAK_ROUND_COUNT[task_id],
                "prediction_out": None,
                "acd_out": False,
                "firing_done": False,
            }

            print("task_state: ", task_state)

            async def _judge_callback(ts=task_state, tid=task_id):
                print("PALAK: calling _fire_judge")
                await _fire_judge(tid, self._client, self._create_args["model"], ts)
                if ts["prediction_out"] is not None:
                    BaseOpenAIChatCompletionClient.PALAK_JUDGE_PREDICTION[tid] = ts["prediction_out"]
                    BaseOpenAIChatCompletionClient.PALAK_JUDGE_HISTORY[tid].append(ts["prediction_out"])
                BaseOpenAIChatCompletionClient.PALAK_JUDGE_ACD[tid] = ts["acd_out"]
                BaseOpenAIChatCompletionClient.PALAK_JUDGE_FIRING[tid] = False

            asyncio.ensure_future(_judge_callback())
            _judge_logger.debug("[judge][%s] Judge call fired (fire-and-forget) step=%d", task_id, step_count)

        # ----------------------------------------------------------------
        # Rest of create() is unchanged from original
        # ----------------------------------------------------------------
        if self._resolved_model is not None:
            if self._resolved_model != result.model:
                import warnings
                warnings.warn(
                    f"Resolved model mismatch: {self._resolved_model} != {result.model}.",
                    stacklevel=2,
                )

        choice = result.choices[0]
        content = None
        thought = None

        if choice.message.function_call is not None:
            raise ValueError("function_call is deprecated and is not supported by this model client.")
        elif choice.message.tool_calls is not None and len(choice.message.tool_calls) > 0:
            if choice.message.content is not None and choice.message.content != "":
                thought = choice.message.content
            content = []
            for tool_call in choice.message.tool_calls:
                if not isinstance(tool_call.function.arguments, str):
                    if isinstance(tool_call.function.arguments, dict):
                        tool_call.function.arguments = json.dumps(tool_call.function.arguments)
                content.append(
                    FunctionCall(
                        id=tool_call.id,
                        arguments=tool_call.function.arguments,
                        name=normalize_name(tool_call.function.name),
                    )
                )
            finish_reason = "tool_calls"
        else:
            finish_reason = choice.finish_reason
            content = choice.message.content or ""
            if choice.message.model_extra is not None:
                reasoning_content = choice.message.model_extra.get("reasoning_content")
                if reasoning_content is not None:
                    thought = reasoning_content

        logprobs = None
        if choice.logprobs and choice.logprobs.content:
            logprobs = [
                ChatCompletionTokenLogprob(
                    token=x.token,
                    logprob=x.logprob,
                    top_logprobs=[TopLogprob(logprob=y.logprob, bytes=y.bytes) for y in x.top_logprobs],
                    bytes=x.bytes,
                )
                for x in choice.logprobs.content
            ]

        if isinstance(content, str) and self._model_info["family"] == ModelFamily.R1 and thought is None:
            thought, content = parse_r1_content(content)

        response = CreateResult(
            finish_reason=normalize_stop_reason(finish_reason),
            content=content,
            usage=usage,
            cached=False,
            logprobs=logprobs,
            thought=thought,
        )

        self._total_usage = _add_usage(self._total_usage, usage)
        self._actual_usage = _add_usage(self._actual_usage, usage)
        return response



    async def create_stream(
        self,
        messages: Sequence[LLMMessage],
        *,
        tools: Sequence[Tool | ToolSchema] = [],
        tool_choice: Tool | Literal["auto", "required", "none"] = "auto",
        json_output: Optional[bool | type[BaseModel]] = None,
        extra_create_args: Mapping[str, Any] = {},
        cancellation_token: Optional[CancellationToken] = None,
        max_consecutive_empty_chunk_tolerance: int = 0,
        include_usage: Optional[bool] = None,
    ) -> AsyncGenerator[Union[str, CreateResult], None]:
        """Create a stream of string chunks from the model ending with a :class:`~autogen_core.models.CreateResult`.

        Extends :meth:`autogen_core.models.ChatCompletionClient.create_stream` to support OpenAI API.

        In streaming, the default behaviour is not return token usage counts.
        See: `OpenAI API reference for possible args <https://platform.openai.com/docs/api-reference/chat/create>`_.

        You can set set the `include_usage` flag to True or `extra_create_args={"stream_options": {"include_usage": True}}`. If both the flag and `stream_options` are set, but to different values, an exception will be raised.
        (if supported by the accessed API) to
        return a final chunk with usage set to a :class:`~autogen_core.models.RequestUsage` object
        with prompt and completion token counts,
        all preceding chunks will have usage as `None`.
        See: `OpenAI API reference for stream options <https://platform.openai.com/docs/api-reference/chat/create#chat-create-stream_options>`_.

        Other examples of supported arguments that can be included in `extra_create_args`:
            - `temperature` (float): Controls the randomness of the output. Higher values (e.g., 0.8) make the output more random, while lower values (e.g., 0.2) make it more focused and deterministic.
            - `max_tokens` (int): The maximum number of tokens to generate in the completion.
            - `top_p` (float): An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass.
            - `frequency_penalty` (float): A value between -2.0 and 2.0 that penalizes new tokens based on their existing frequency in the text so far, decreasing the likelihood of repeated phrases.
            - `presence_penalty` (float): A value between -2.0 and 2.0 that penalizes new tokens based on whether they appear in the text so far, encouraging the model to talk about new topics.
        """

        create_params = self._process_create_args(
            messages,
            tools,
            tool_choice,
            json_output,
            extra_create_args,
        )

        if include_usage is not None:
            if "stream_options" in create_params.create_args:
                stream_options = create_params.create_args["stream_options"]
                if "include_usage" in stream_options and stream_options["include_usage"] != include_usage:
                    raise ValueError(
                        "include_usage and extra_create_args['stream_options']['include_usage'] are both set, but differ in value."
                    )
            else:
                # If stream options are not present, add them.
                create_params.create_args["stream_options"] = {"include_usage": True}

        if max_consecutive_empty_chunk_tolerance != 0:
            warnings.warn(
                "The 'max_consecutive_empty_chunk_tolerance' parameter is deprecated and will be removed in the future releases. All of empty chunks will be skipped with a warning.",
                DeprecationWarning,
                stacklevel=2,
            )

        if create_params.response_format is not None:
            chunks = self._create_stream_chunks_beta_client(
                tool_params=create_params.tools,
                oai_messages=create_params.messages,
                response_format=create_params.response_format,
                create_args_no_response_format=create_params.create_args,
                cancellation_token=cancellation_token,
            )
        else:
            chunks = self._create_stream_chunks(
                tool_params=create_params.tools,
                oai_messages=create_params.messages,
                create_args=create_params.create_args,
                cancellation_token=cancellation_token,
            )

        # Prepare data to process streaming chunks.
        chunk: ChatCompletionChunk | None = None
        stop_reason = None
        maybe_model = None
        content_deltas: List[str] = []
        thought_deltas: List[str] = []
        full_tool_calls: Dict[int, FunctionCall] = {}
        logprobs: Optional[List[ChatCompletionTokenLogprob]] = None

        empty_chunk_warning_has_been_issued: bool = False
        empty_chunk_warning_threshold: int = 10
        empty_chunk_count = 0
        first_chunk = True
        is_reasoning = False

        # Process the stream of chunks.
        async for chunk in chunks:
            if first_chunk:
                first_chunk = False
                # Emit the start event.
                logger.info(
                    LLMStreamStartEvent(
                        messages=cast(List[Dict[str, Any]], create_params.messages),
                    )
                )

            # Set the model from the lastest chunk.
            maybe_model = chunk.model

            # Empty chunks has been observed when the endpoint is under heavy load.
            #  https://github.com/microsoft/autogen/issues/4213
            if len(chunk.choices) == 0:
                empty_chunk_count += 1
                if not empty_chunk_warning_has_been_issued and empty_chunk_count >= empty_chunk_warning_threshold:
                    empty_chunk_warning_has_been_issued = True
                    warnings.warn(
                        f"Received more than {empty_chunk_warning_threshold} consecutive empty chunks. Empty chunks are being ignored.",
                        stacklevel=2,
                    )
                continue
            else:
                empty_chunk_count = 0

            if len(chunk.choices) > 1:
                # This is a multi-choice chunk, we need to warn the user.
                warnings.warn(
                    f"Received a chunk with {len(chunk.choices)} choices. Only the first choice will be used.",
                    UserWarning,
                    stacklevel=2,
                )

            # Set the choice to the first choice in the chunk.
            choice = chunk.choices[0]

            # for liteLLM chunk usage, do the following hack keeping the pervious chunk.stop_reason (if set).
            # set the stop_reason for the usage chunk to the prior stop_reason
            stop_reason = choice.finish_reason if chunk.usage is None and stop_reason is None else stop_reason
            maybe_model = chunk.model

            reasoning_content: str | None = None
            if choice.delta.model_extra is not None and "reasoning_content" in choice.delta.model_extra:
                # If there is a reasoning_content field, then we populate the thought field. This is for models such as R1.
                reasoning_content = choice.delta.model_extra.get("reasoning_content")

            if isinstance(reasoning_content, str) and len(reasoning_content) > 0:
                if not is_reasoning:
                    # Enter reasoning mode.
                    reasoning_content = "<think>" + reasoning_content
                    is_reasoning = True
                thought_deltas.append(reasoning_content)
                yield reasoning_content
            elif reasoning_content is None and is_reasoning:
                # Exit reasoning mode only when reasoning_content is None (not when it's an empty string).
                reasoning_content = "</think>"
                thought_deltas.append(reasoning_content)
                is_reasoning = False
                yield reasoning_content

            # First try get content
            if choice.delta.content:
                content_deltas.append(choice.delta.content)
                if len(choice.delta.content) > 0:
                    yield choice.delta.content
                # NOTE: for OpenAI, tool_calls and content are mutually exclusive it seems, so we can skip the rest of the loop.
                # However, this may not be the case for other APIs -- we should expect this may need to be updated.
                continue
            # Otherwise, get tool calls
            if choice.delta.tool_calls is not None:
                for tool_call_chunk in choice.delta.tool_calls:
                    idx = tool_call_chunk.index
                    if idx not in full_tool_calls:
                        # We ignore the type hint here because we want to fill in type when the delta provides it
                        full_tool_calls[idx] = FunctionCall(id="", arguments="", name="")

                    if tool_call_chunk.id is not None:
                        full_tool_calls[idx].id += tool_call_chunk.id

                    if tool_call_chunk.function is not None:
                        if tool_call_chunk.function.name is not None:
                            full_tool_calls[idx].name += tool_call_chunk.function.name
                        if tool_call_chunk.function.arguments is not None:
                            full_tool_calls[idx].arguments += tool_call_chunk.function.arguments
            if choice.logprobs and choice.logprobs.content:
                logprobs = [
                    ChatCompletionTokenLogprob(
                        token=x.token,
                        logprob=x.logprob,
                        top_logprobs=[TopLogprob(logprob=y.logprob, bytes=y.bytes) for y in x.top_logprobs],
                        bytes=x.bytes,
                    )
                    for x in choice.logprobs.content
                ]

        # Finalize the CreateResult.

        # TODO: can we remove this?
        if stop_reason == "function_call":
            raise ValueError("Function calls are not supported in this context")

        # We need to get the model from the last chunk, if available.
        model = maybe_model or create_params.create_args["model"]
        model = model.replace("gpt-35", "gpt-3.5")  # hack for Azure API

        # Because the usage chunk is not guaranteed to be the last chunk, we need to check if it is available.
        if chunk and chunk.usage:
            prompt_tokens = chunk.usage.prompt_tokens
            completion_tokens = chunk.usage.completion_tokens
        else:
            prompt_tokens = 0
            completion_tokens = 0
        usage = RequestUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )

        # Detect whether it is a function call or just text.
        content: Union[str, List[FunctionCall]]
        thought: str | None = None
        # Determine the content and thought based on what was collected
        if full_tool_calls:
            # This is a tool call response
            content = list(full_tool_calls.values())
            if content_deltas:
                # Store any text alongside tool calls as thoughts
                thought = "".join(content_deltas)
        else:
            # This is a text response (possibly with thoughts)
            if content_deltas:
                content = "".join(content_deltas)
            else:
                warnings.warn(
                    "No text content or tool calls are available. Model returned empty result.",
                    stacklevel=2,
                )
                content = ""

            # Set thoughts if we have any reasoning content.
            if thought_deltas:
                thought = "".join(thought_deltas).lstrip("<think>").rstrip("</think>")

            # This is for local R1 models whose reasoning content is within the content string.
            if isinstance(content, str) and self._model_info["family"] == ModelFamily.R1 and thought is None:
                thought, content = parse_r1_content(content)

        # Create the result.
        result = CreateResult(
            finish_reason=normalize_stop_reason(stop_reason),
            content=content,
            usage=usage,
            cached=False,
            logprobs=logprobs,
            thought=thought,
        )

        # Log the end of the stream.
        logger.info(
            LLMStreamEndEvent(
                response=result.model_dump(),
                prompt_tokens=usage.prompt_tokens,
                completion_tokens=usage.completion_tokens,
            )
        )

        # Update the total usage.
        self._total_usage = _add_usage(self._total_usage, usage)
        self._actual_usage = _add_usage(self._actual_usage, usage)

        # Yield the CreateResult.
        yield result

    async def _create_stream_chunks(
        self,
        tool_params: List[ChatCompletionToolParam],
        oai_messages: List[ChatCompletionMessageParam],
        create_args: Dict[str, Any],
        cancellation_token: Optional[CancellationToken],
    ) -> AsyncGenerator[ChatCompletionChunk, None]:
        stream_future = asyncio.ensure_future(
            self._client.chat.completions.create(
                messages=oai_messages,
                stream=True,
                tools=tool_params if len(tool_params) > 0 else NOT_GIVEN,
                **create_args,
            )
        )
        if cancellation_token is not None:
            cancellation_token.link_future(stream_future)
        stream = await stream_future
        while True:
            try:
                chunk_future = asyncio.ensure_future(anext(stream))
                if cancellation_token is not None:
                    cancellation_token.link_future(chunk_future)
                chunk = await chunk_future
                yield chunk
            except StopAsyncIteration:
                break

    async def _create_stream_chunks_beta_client(
        self,
        tool_params: List[ChatCompletionToolParam],
        oai_messages: List[ChatCompletionMessageParam],
        create_args_no_response_format: Dict[str, Any],
        response_format: Optional[Type[BaseModel]],
        cancellation_token: Optional[CancellationToken],
    ) -> AsyncGenerator[ChatCompletionChunk, None]:
        async with self._client.beta.chat.completions.stream(
            messages=oai_messages,
            tools=tool_params if len(tool_params) > 0 else NOT_GIVEN,
            response_format=(response_format if response_format is not None else NOT_GIVEN),
            **create_args_no_response_format,
        ) as stream:
            while True:
                try:
                    event_future = asyncio.ensure_future(anext(stream))
                    if cancellation_token is not None:
                        cancellation_token.link_future(event_future)
                    event = await event_future

                    if event.type == "chunk":
                        chunk = event.chunk
                        yield chunk
                    # We don't handle other event types from the beta client stream.
                    # As the other event types are auxiliary to the chunk event.
                    # See: https://github.com/openai/openai-python/blob/main/helpers.md#chat-completions-events.
                    # Once the beta client is stable, we can move all the logic to the beta client.
                    # Then we can consider handling other event types which may simplify the code overall.
                except StopAsyncIteration:
                    break

    async def close(self) -> None:
        await self._client.close()

    def actual_usage(self) -> RequestUsage:
        return self._actual_usage

    def total_usage(self) -> RequestUsage:
        return self._total_usage

    def count_tokens(self, messages: Sequence[LLMMessage], *, tools: Sequence[Tool | ToolSchema] = []) -> int:
        return count_tokens_openai(
            messages,
            self._create_args["model"],
            add_name_prefixes=self._add_name_prefixes,
            tools=tools,
            model_family=self._model_info["family"],
            include_name_in_message=self._include_name_in_message,
        )

    def remaining_tokens(self, messages: Sequence[LLMMessage], *, tools: Sequence[Tool | ToolSchema] = []) -> int:
        token_limit = _model_info.get_token_limit(self._create_args["model"])
        return token_limit - self.count_tokens(messages, tools=tools)

    @property
    def capabilities(self) -> ModelCapabilities:  # type: ignore
        warnings.warn(
            "capabilities is deprecated, use model_info instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._model_info

    @property
    def model_info(self) -> ModelInfo:
        return self._model_info


class OpenAIChatCompletionClient(BaseOpenAIChatCompletionClient, Component[OpenAIClientConfigurationConfigModel]):
    """Chat completion client for OpenAI hosted models.

    To use this client, you must install the `openai` extra:

    .. code-block:: bash

        pip install "autogen-ext[openai]"

    You can also use this client for OpenAI-compatible ChatCompletion endpoints.
    **Using this client for non-OpenAI models is not tested or guaranteed.**

    For non-OpenAI models, please first take a look at our `community extensions <https://microsoft.github.io/autogen/dev/user-guide/extensions-user-guide/index.html>`_
    for additional model clients.

    Args:
        model (str): Which OpenAI model to use.
        api_key (optional, str): The API key to use. **Required if 'OPENAI_API_KEY' is not found in the environment variables.**
        organization (optional, str): The organization ID to use.
        base_url (optional, str): The base URL to use. **Required if the model is not hosted on OpenAI.**
        timeout: (optional, float): The timeout for the request in seconds.
        max_retries (optional, int): The maximum number of retries to attempt.
        model_info (optional, ModelInfo): The capabilities of the model. **Required if the model name is not a valid OpenAI model.**
        frequency_penalty (optional, float):
        logit_bias: (optional, dict[str, int]):
        max_tokens (optional, int):
        n (optional, int):
        presence_penalty (optional, float):
        response_format (optional, Dict[str, Any]): the format of the response. Possible options are:

            .. code-block:: text

                # Text response, this is the default.
                {"type": "text"}

            .. code-block:: text

                # JSON response, make sure to instruct the model to return JSON.
                {"type": "json_object"}

            .. code-block:: text

                # Structured output response, with a pre-defined JSON schema.
                {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "name of the schema, must be an identifier.",
                        "description": "description for the model.",
                        # You can convert a Pydantic (v2) model to JSON schema
                        # using the `model_json_schema()` method.
                        "schema": "<the JSON schema itself>",
                        # Whether to enable strict schema adherence when
                        # generating the output. If set to true, the model will
                        # always follow the exact schema defined in the
                        # `schema` field. Only a subset of JSON Schema is
                        # supported when `strict` is `true`.
                        # To learn more, read
                        # https://platform.openai.com/docs/guides/structured-outputs.
                        "strict": False,  # or True
                    },
                }

            It is recommended to use the `json_output` parameter in
            :meth:`~autogen_ext.models.openai.BaseOpenAIChatCompletionClient.create` or
            :meth:`~autogen_ext.models.openai.BaseOpenAIChatCompletionClient.create_stream`
            methods instead of `response_format` for structured output.
            The `json_output` parameter is more flexible and allows you to
            specify a Pydantic model class directly.

        seed (optional, int):
        stop (optional, str | List[str]):
        temperature (optional, float):
        top_p (optional, float):
        parallel_tool_calls (optional, bool): Whether to allow parallel tool calls. When not set, defaults to server behavior.
        user (optional, str):
        default_headers (optional, dict[str, str]):  Custom headers; useful for authentication or other custom requirements.
        add_name_prefixes (optional, bool): Whether to prepend the `source` value
            to each :class:`~autogen_core.models.UserMessage` content. E.g.,
            "this is content" becomes "Reviewer said: this is content."
            This can be useful for models that do not support the `name` field in
            message. Defaults to False.
        include_name_in_message (optional, bool): Whether to include the `name` field
            in user message parameters sent to the OpenAI API. Defaults to True. Set to False
            for model providers that don't support the `name` field (e.g., Groq).
        stream_options (optional, dict): Additional options for streaming. Currently only `include_usage` is supported.

    Examples:

        The following code snippet shows how to use the client with an OpenAI model:

        .. code-block:: python

            from autogen_ext.models.openai import OpenAIChatCompletionClient
            from autogen_core.models import UserMessage

            openai_client = OpenAIChatCompletionClient(
                model="gpt-4o-2024-08-06",
                # api_key="sk-...", # Optional if you have an OPENAI_API_KEY environment variable set.
            )

            result = await openai_client.create([UserMessage(content="What is the capital of France?", source="user")])  # type: ignore
            print(result)

            # Close the client when done.
            # await openai_client.close()

        To use the client with a non-OpenAI model, you need to provide the base URL of the model and the model info.
        For example, to use Ollama, you can use the following code snippet:

        .. code-block:: python

            from autogen_ext.models.openai import OpenAIChatCompletionClient
            from autogen_core.models import ModelFamily

            custom_model_client = OpenAIChatCompletionClient(
                model="deepseek-r1:1.5b",
                base_url="http://localhost:11434/v1",
                api_key="placeholder",
                model_info={
                    "vision": False,
                    "function_calling": False,
                    "json_output": False,
                    "family": ModelFamily.R1,
                    "structured_output": True,
                },
            )

            # Close the client when done.
            # await custom_model_client.close()

        To use streaming mode, you can use the following code snippet:

        .. code-block:: python

            import asyncio
            from autogen_core.models import UserMessage
            from autogen_ext.models.openai import OpenAIChatCompletionClient


            async def main() -> None:
                # Similar for AzureOpenAIChatCompletionClient.
                model_client = OpenAIChatCompletionClient(model="gpt-4o")  # assuming OPENAI_API_KEY is set in the environment.

                messages = [UserMessage(content="Write a very short story about a dragon.", source="user")]

                # Create a stream.
                stream = model_client.create_stream(messages=messages)

                # Iterate over the stream and print the responses.
                print("Streamed responses:")
                async for response in stream:
                    if isinstance(response, str):
                        # A partial response is a string.
                        print(response, flush=True, end="")
                    else:
                        # The last response is a CreateResult object with the complete message.
                        print("\\n\\n------------\\n")
                        print("The complete response:", flush=True)
                        print(response.content, flush=True)

                # Close the client when done.
                await model_client.close()


            asyncio.run(main())

        To use structured output as well as function calling, you can use the following code snippet:

        .. code-block:: python

            import asyncio
            from typing import Literal

            from autogen_core.models import (
                AssistantMessage,
                FunctionExecutionResult,
                FunctionExecutionResultMessage,
                SystemMessage,
                UserMessage,
            )
            from autogen_core.tools import FunctionTool
            from autogen_ext.models.openai import OpenAIChatCompletionClient
            from pydantic import BaseModel


            # Define the structured output format.
            class AgentResponse(BaseModel):
                thoughts: str
                response: Literal["happy", "sad", "neutral"]


            # Define the function to be called as a tool.
            def sentiment_analysis(text: str) -> str:
                \"\"\"Given a text, return the sentiment.\"\"\"
                return "happy" if "happy" in text else "sad" if "sad" in text else "neutral"


            # Create a FunctionTool instance with `strict=True`,
            # which is required for structured output mode.
            tool = FunctionTool(sentiment_analysis, description="Sentiment Analysis", strict=True)


            async def main() -> None:
                # Create an OpenAIChatCompletionClient instance.
                model_client = OpenAIChatCompletionClient(model="gpt-4o-mini")

                # Generate a response using the tool.
                response1 = await model_client.create(
                    messages=[
                        SystemMessage(content="Analyze input text sentiment using the tool provided."),
                        UserMessage(content="I am happy.", source="user"),
                    ],
                    tools=[tool],
                )
                print(response1.content)
                # Should be a list of tool calls.
                # [FunctionCall(name="sentiment_analysis", arguments={"text": "I am happy."}, ...)]

                assert isinstance(response1.content, list)
                response2 = await model_client.create(
                    messages=[
                        SystemMessage(content="Analyze input text sentiment using the tool provided."),
                        UserMessage(content="I am happy.", source="user"),
                        AssistantMessage(content=response1.content, source="assistant"),
                        FunctionExecutionResultMessage(
                            content=[FunctionExecutionResult(content="happy", call_id=response1.content[0].id, is_error=False, name="sentiment_analysis")]
                        ),
                    ],
                    # Use the structured output format.
                    json_output=AgentResponse,
                )
                print(response2.content)
                # Should be a structured output.
                # {"thoughts": "The user is happy.", "response": "happy"}

                # Close the client when done.
                await model_client.close()

            asyncio.run(main())


        To load the client from a configuration, you can use the `load_component` method:

        .. code-block:: python

            from autogen_core.models import ChatCompletionClient

            config = {
                "provider": "OpenAIChatCompletionClient",
                "config": {"model": "gpt-4o", "api_key": "REPLACE_WITH_YOUR_API_KEY"},
            }

            client = ChatCompletionClient.load_component(config)

        To view the full list of available configuration options, see the :py:class:`OpenAIClientConfigurationConfigModel` class.

    """

    component_type = "model"
    component_config_schema = OpenAIClientConfigurationConfigModel
    component_provider_override = "autogen_ext.models.openai.OpenAIChatCompletionClient"

    def __init__(self, **kwargs: Unpack[OpenAIClientConfiguration]):
        if "model" not in kwargs:
            raise ValueError("model is required for OpenAIChatCompletionClient")

        model_capabilities: Optional[ModelCapabilities] = None  # type: ignore
        self._raw_config: Dict[str, Any] = dict(kwargs).copy()
        copied_args = dict(kwargs).copy()

        if "model_capabilities" in kwargs:
            model_capabilities = kwargs["model_capabilities"]
            del copied_args["model_capabilities"]

        model_info: Optional[ModelInfo] = None
        if "model_info" in kwargs:
            model_info = kwargs["model_info"]
            del copied_args["model_info"]

        add_name_prefixes: bool = False
        if "add_name_prefixes" in kwargs:
            add_name_prefixes = kwargs["add_name_prefixes"]

        include_name_in_message: bool = True
        if "include_name_in_message" in kwargs:
            include_name_in_message = kwargs["include_name_in_message"]

        # Special handling for Gemini model.
        assert "model" in copied_args and isinstance(copied_args["model"], str)
        if copied_args["model"].startswith("gemini-"):
            if "base_url" not in copied_args:
                copied_args["base_url"] = _model_info.GEMINI_OPENAI_BASE_URL
            if "api_key" not in copied_args and "GEMINI_API_KEY" in os.environ:
                copied_args["api_key"] = os.environ["GEMINI_API_KEY"]
        if copied_args["model"].startswith("claude-"):
            if "base_url" not in copied_args:
                copied_args["base_url"] = _model_info.ANTHROPIC_OPENAI_BASE_URL
            if "api_key" not in copied_args and "ANTHROPIC_API_KEY" in os.environ:
                copied_args["api_key"] = os.environ["ANTHROPIC_API_KEY"]
        if copied_args["model"].startswith("Llama-"):
            if "base_url" not in copied_args:
                copied_args["base_url"] = _model_info.LLAMA_API_BASE_URL
            if "api_key" not in copied_args and "LLAMA_API_KEY" in os.environ:
                copied_args["api_key"] = os.environ["LLAMA_API_KEY"]

        client = _openai_client_from_config(copied_args)
        create_args = _create_args_from_config(copied_args)

        super().__init__(
            client=client,
            create_args=create_args,
            model_capabilities=model_capabilities,
            model_info=model_info,
            add_name_prefixes=add_name_prefixes,
            include_name_in_message=include_name_in_message,
        )

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_client"] = None
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._client = _openai_client_from_config(state["_raw_config"])

    def _to_config(self) -> OpenAIClientConfigurationConfigModel:
        copied_config = self._raw_config.copy()
        return OpenAIClientConfigurationConfigModel(**copied_config)

    @classmethod
    def _from_config(cls, config: OpenAIClientConfigurationConfigModel) -> Self:
        copied_config = config.model_copy().model_dump(exclude_none=True)

        # Handle api_key as SecretStr
        if "api_key" in copied_config and isinstance(config.api_key, SecretStr):
            copied_config["api_key"] = config.api_key.get_secret_value()

        return cls(**copied_config)


class AzureOpenAIChatCompletionClient(
    BaseOpenAIChatCompletionClient, Component[AzureOpenAIClientConfigurationConfigModel]
):
    """Chat completion client for Azure OpenAI hosted models.

    To use this client, you must install the `azure` and `openai` extensions:

    .. code-block:: bash

        pip install "autogen-ext[openai,azure]"

    Args:

        model (str): Which OpenAI model to use.
        azure_endpoint (str): The endpoint for the Azure model. **Required for Azure models.**
        azure_deployment (str): Deployment name for the Azure model. **Required for Azure models.**
        api_version (str): The API version to use. **Required for Azure models.**
        azure_ad_token (str): The Azure AD token to use. Provide this or `azure_ad_token_provider` for token-based authentication.
        azure_ad_token_provider (optional, Callable[[], Awaitable[str]] | AzureTokenProvider): The Azure AD token provider to use. Provide this or `azure_ad_token` for token-based authentication.
        api_key (optional, str): The API key to use, use this if you are using key based authentication. It is optional if you are using Azure AD token based authentication or `AZURE_OPENAI_API_KEY` environment variable.
        timeout: (optional, float): The timeout for the request in seconds.
        max_retries (optional, int): The maximum number of retries to attempt.
        model_info (optional, ModelInfo): The capabilities of the model. **Required if the model name is not a valid OpenAI model.**
        frequency_penalty (optional, float):
        logit_bias: (optional, dict[str, int]):
        max_tokens (optional, int):
        n (optional, int):
        presence_penalty (optional, float):
        response_format (optional, Dict[str, Any]): the format of the response. Possible options are:

            .. code-block:: text

                # Text response, this is the default.
                {"type": "text"}

            .. code-block:: text

                # JSON response, make sure to instruct the model to return JSON.
                {"type": "json_object"}

            .. code-block:: text

                # Structured output response, with a pre-defined JSON schema.
                {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "name of the schema, must be an identifier.",
                        "description": "description for the model.",
                        # You can convert a Pydantic (v2) model to JSON schema
                        # using the `model_json_schema()` method.
                        "schema": "<the JSON schema itself>",
                        # Whether to enable strict schema adherence when
                        # generating the output. If set to true, the model will
                        # always follow the exact schema defined in the
                        # `schema` field. Only a subset of JSON Schema is
                        # supported when `strict` is `true`.
                        # To learn more, read
                        # https://platform.openai.com/docs/guides/structured-outputs.
                        "strict": False,  # or True
                    },
                }

            It is recommended to use the `json_output` parameter in
            :meth:`~autogen_ext.models.openai.BaseOpenAIChatCompletionClient.create` or
            :meth:`~autogen_ext.models.openai.BaseOpenAIChatCompletionClient.create_stream`
            methods instead of `response_format` for structured output.
            The `json_output` parameter is more flexible and allows you to
            specify a Pydantic model class directly.

        seed (optional, int):
        stop (optional, str | List[str]):
        temperature (optional, float):
        top_p (optional, float):
        parallel_tool_calls (optional, bool): Whether to allow parallel tool calls. When not set, defaults to server behavior.
        user (optional, str):
        default_headers (optional, dict[str, str]):  Custom headers; useful for authentication or other custom requirements.
        add_name_prefixes (optional, bool): Whether to prepend the `source` value
            to each :class:`~autogen_core.models.UserMessage` content. E.g.,
            "this is content" becomes "Reviewer said: this is content."
            This can be useful for models that do not support the `name` field in
            message. Defaults to False.
        include_name_in_message (optional, bool): Whether to include the `name` field
            in user message parameters sent to the OpenAI API. Defaults to True. Set to False
            for model providers that don't support the `name` field (e.g., Groq).
        stream_options (optional, dict): Additional options for streaming. Currently only `include_usage` is supported.


    To use the client, you need to provide your deployment name, Azure Cognitive Services endpoint, and api version.
    For authentication, you can either provide an API key or an Azure Active Directory (AAD) token credential.

    The following code snippet shows how to use AAD authentication.
    The identity used must be assigned the `Cognitive Services OpenAI User <https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/role-based-access-control#cognitive-services-openai-user>`_ role.

    .. code-block:: python

        from autogen_ext.auth.azure import AzureTokenProvider
        from autogen_ext.models.openai import AzureOpenAIChatCompletionClient
        from azure.identity import DefaultAzureCredential

        # Create the token provider
        token_provider = AzureTokenProvider(
            DefaultAzureCredential(),
            "https://cognitiveservices.azure.com/.default",
        )

        az_model_client = AzureOpenAIChatCompletionClient(
            azure_deployment="{your-azure-deployment}",
            model="{model-name, such as gpt-4o}",
            api_version="2024-06-01",
            azure_endpoint="https://{your-custom-endpoint}.openai.azure.com/",
            azure_ad_token_provider=token_provider,  # Optional if you choose key-based authentication.
            # api_key="sk-...", # For key-based authentication.
        )

    See other usage examples in the :class:`OpenAIChatCompletionClient` class.

    To load the client that uses identity based aith from a configuration, you can use the `load_component` method:

    .. code-block:: python

        from autogen_core.models import ChatCompletionClient

        config = {
            "provider": "AzureOpenAIChatCompletionClient",
            "config": {
                "model": "gpt-4o-2024-05-13",
                "azure_endpoint": "https://{your-custom-endpoint}.openai.azure.com/",
                "azure_deployment": "{your-azure-deployment}",
                "api_version": "2024-06-01",
                "azure_ad_token_provider": {
                    "provider": "autogen_ext.auth.azure.AzureTokenProvider",
                    "config": {
                        "provider_kind": "DefaultAzureCredential",
                        "scopes": ["https://cognitiveservices.azure.com/.default"],
                    },
                },
            },
        }

        client = ChatCompletionClient.load_component(config)


    To view the full list of available configuration options, see the :py:class:`AzureOpenAIClientConfigurationConfigModel` class.

    .. note::

        Right now only `DefaultAzureCredential` is supported with no additional args passed to it.

    .. note::

        The Azure OpenAI client by default sets the User-Agent header to `autogen-python/{version}`. To override this, you can set the variable `autogen_ext.models.openai.AZURE_OPENAI_USER_AGENT` environment variable to an empty string.

    See `here <https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/managed-identity#chat-completions>`_ for how to use the Azure client directly or for more info.

    """

    component_type = "model"
    component_config_schema = AzureOpenAIClientConfigurationConfigModel
    component_provider_override = "autogen_ext.models.openai.AzureOpenAIChatCompletionClient"

    def __init__(self, **kwargs: Unpack[AzureOpenAIClientConfiguration]):
        model_capabilities: Optional[ModelCapabilities] = None  # type: ignore
        copied_args = dict(kwargs).copy()
        if "model_capabilities" in kwargs:
            model_capabilities = kwargs["model_capabilities"]
            del copied_args["model_capabilities"]

        model_info: Optional[ModelInfo] = None
        if "model_info" in kwargs:
            model_info = kwargs["model_info"]
            del copied_args["model_info"]

        add_name_prefixes: bool = False
        if "add_name_prefixes" in kwargs:
            add_name_prefixes = kwargs["add_name_prefixes"]

        include_name_in_message: bool = True
        if "include_name_in_message" in kwargs:
            include_name_in_message = kwargs["include_name_in_message"]

        client = _azure_openai_client_from_config(copied_args)
        create_args = _create_args_from_config(copied_args)
        self._raw_config: Dict[str, Any] = copied_args
        super().__init__(
            client=client,
            create_args=create_args,
            model_capabilities=model_capabilities,
            model_info=model_info,
            add_name_prefixes=add_name_prefixes,
            include_name_in_message=include_name_in_message,
        )

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_client"] = None
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._client = _azure_openai_client_from_config(state["_raw_config"])

    def _to_config(self) -> AzureOpenAIClientConfigurationConfigModel:
        from ...auth.azure import AzureTokenProvider

        copied_config = self._raw_config.copy()
        if "azure_ad_token_provider" in copied_config:
            if not isinstance(copied_config["azure_ad_token_provider"], AzureTokenProvider):
                raise ValueError("azure_ad_token_provider must be a AzureTokenProvider to be component serialized")

            copied_config["azure_ad_token_provider"] = (
                copied_config["azure_ad_token_provider"].dump_component().model_dump(exclude_none=True)
            )

        return AzureOpenAIClientConfigurationConfigModel(**copied_config)

    @classmethod
    def _from_config(cls, config: AzureOpenAIClientConfigurationConfigModel) -> Self:
        from ...auth.azure import AzureTokenProvider

        copied_config = config.model_copy().model_dump(exclude_none=True)

        # Handle api_key as SecretStr
        if "api_key" in copied_config and isinstance(config.api_key, SecretStr):
            copied_config["api_key"] = config.api_key.get_secret_value()

        if "azure_ad_token_provider" in copied_config:
            copied_config["azure_ad_token_provider"] = AzureTokenProvider.load_component(
                copied_config["azure_ad_token_provider"]
            )

        return cls(**copied_config)
