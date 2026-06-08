"""Hermes Agent adapter.

Florence owns household policy and transport. Hermes owns open-ended reasoning.
This adapter keeps that boundary explicit.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import re
import shutil
import sys
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

try:
    import fcntl
except ImportError:  # pragma: no cover - Florence deploys on POSIX containers.
    fcntl = None  # type: ignore[assignment]

from florence.config import Settings
from florence.models import (
    Household,
    HouseholdMember,
    HouseholdPrivacy,
    HouseholdReadiness,
    MemoryRecord,
    SourcePreference,
    SourcePreferenceKind,
)
from florence.prompts import PROPOSAL_PROTOCOL, SAAS_BOUNDARY_PROTOCOL, SYSTEM_PERSONA
from florence.tone import fallback_reply
from florence.timekeeper import ensure_utc, format_local


PHONE_LIKE_RE = re.compile(
    r"(?<!\w)(?:"
    r"\+\d[\d\s().-]{7,}\d"
    r"|(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}"
    r")(?!\w)"
)
PHONE_REDACTION = "[phone number]"
EMAIL_LIKE_RE = re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}")
EMAIL_REDACTION = "[email address]"
HERMES_DEFAULT_HOME_NAME = ".hermes"
HERMES_RUNTIME_LOCK = threading.RLock()
PINNED_GIT_REF = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")
HERMES_CHECKOUT_REF_FILE = ".florence-hermes-ref"
HERMES_RUNTIME_LOCK_FILE = ".florence-hermes-runtime.lock"
HERMES_INTERPROCESS_LOCK_MODE = "thread_lock_plus_interprocess_file_lock"
HERMES_THREAD_ONLY_LOCK_MODE = "thread_lock_only_no_interprocess_lock"
HERMES_INTERPROCESS_CONCURRENCY_MODE = "serialized_by_thread_and_file_lock"
HERMES_THREAD_ONLY_CONCURRENCY_MODE = "serialized_by_thread_lock_only"


class HermesSaaSContractError(RuntimeError):
    """Raised when a deployed SaaS Hermes call violates Florence's boundary."""


class AgentBackend(Protocol):
    def complete(
        self,
        *,
        household: Household,
        user_text: str,
        conversation_history: list[dict[str, str]],
        upcoming: list[tuple[str, object]],
        memories: list[MemoryRecord],
        members: list[HouseholdMember],
        actor: HouseholdMember,
        now_utc: datetime,
        source_preferences: list[SourcePreference],
        privacy: HouseholdPrivacy,
        readiness: HouseholdReadiness,
    ) -> str:
        ...


@dataclass(slots=True)
class HermesBackend:
    settings: Settings

    def complete(
        self,
        *,
        household: Household,
        user_text: str,
        conversation_history: list[dict[str, str]],
        upcoming: list[tuple[str, object]],
        memories: list[MemoryRecord],
        members: list[HouseholdMember],
        actor: HouseholdMember,
        now_utc: datetime,
        source_preferences: list[SourcePreference],
        privacy: HouseholdPrivacy,
        readiness: HouseholdReadiness,
    ) -> str:
        try:
            self._reject_enabled_toolsets()
            self._enforce_deployed_saas_contract()
            turn_id = f"florence-turn-{uuid.uuid4()}"
            with hermes_runtime_home_context(self.settings, scope=turn_id, cleanup=True):
                with hermes_checkout_path_context(self.settings):
                    with hermes_checkout_module_context(self.settings):
                        AIAgent = self._load_ai_agent()
                        system_message = self._system_message(
                            household=household,
                            upcoming=upcoming,
                            memories=memories,
                            members=members,
                            actor=actor,
                            now_utc=now_utc,
                            source_preferences=source_preferences,
                            privacy=privacy,
                            readiness=readiness,
                        )
                        agent = AIAgent(
                            base_url=self.settings.hermes_base_url,
                            api_key=self.settings.hermes_api_key,
                            provider=self.settings.hermes_provider,
                            model=self.settings.hermes_model,
                            enabled_toolsets=list(self.settings.hermes_enabled_toolsets),
                            quiet_mode=True,
                            save_trajectories=False,
                            skip_context_files=True,
                            skip_memory=True,
                            platform="florence",
                            session_id=turn_id,
                        )
                        result = agent.run_conversation(
                            user_message=_redact_private_text_for_hermes(user_text),
                            system_message=system_message,
                            conversation_history=_redact_conversation_history_for_hermes(conversation_history),
                        )
            if isinstance(result, dict):
                final = str(result.get("final_response") or "").strip()
            else:
                final = str(result or "").strip()
            if final:
                return final
            raise RuntimeError("Hermes returned an empty response")
        except HermesSaaSContractError:
            raise
        except Exception:
            if self.settings.hermes_strict:
                raise
            return fallback_reply()

    def _reject_enabled_toolsets(self) -> None:
        enabled_toolsets = [toolset for toolset in self.settings.hermes_enabled_toolsets if toolset]
        if enabled_toolsets:
            error_cls = (
                HermesSaaSContractError
                if self.settings.database_backend != "sqlite"
                else RuntimeError
            )
            raise error_cls(
                "Hermes toolsets are disabled for Florence SaaS traffic; "
                "Florence owns external tools and integrations"
            )

    def _enforce_deployed_saas_contract(self) -> None:
        if self.settings.database_backend == "sqlite":
            return
        if not self.settings.hermes_strict:
            raise HermesSaaSContractError(
                "FLORENCE_HERMES_STRICT must be true for deployed SaaS traffic"
            )
        if not self.settings.hermes_agent_path:
            raise HermesSaaSContractError(
                "FLORENCE_HERMES_AGENT_PATH is required for deployed SaaS traffic"
            )
        if not self.settings.hermes_provider:
            raise HermesSaaSContractError(
                "FLORENCE_HERMES_PROVIDER is required for deployed SaaS traffic"
            )
        if not self.settings.hermes_model:
            raise HermesSaaSContractError(
                "FLORENCE_HERMES_MODEL is required for deployed SaaS traffic"
            )
        lock_error = hermes_runtime_lock_error()
        if lock_error:
            raise HermesSaaSContractError(lock_error)
        expected_ref = (self.settings.hermes_agent_ref or "").strip()
        if not PINNED_GIT_REF.fullmatch(expected_ref):
            raise HermesSaaSContractError(
                "HERMES_AGENT_REF must be a full pinned Git commit SHA "
                "(40 or 64 hex characters) for deployed SaaS traffic"
            )
        checkout_ref = hermes_checkout_ref(Path(self.settings.hermes_agent_path).expanduser())
        if not _refs_match(expected_ref, checkout_ref):
            raise HermesSaaSContractError(
                "FLORENCE_HERMES_AGENT_PATH checkout ref does not match HERMES_AGENT_REF"
            )

    def _load_ai_agent(self):
        if self.settings.hermes_agent_path:
            path = Path(self.settings.hermes_agent_path).expanduser().resolve()
            run_agent_path = path / "run_agent.py"
            spec = importlib.util.spec_from_file_location(
                f"_florence_hermes_runtime_{abs(hash(str(run_agent_path)))}",
                run_agent_path,
            )
            if spec is None or spec.loader is None:
                raise ImportError(f"Cannot import Hermes run_agent.py from {run_agent_path}")
            module = importlib.util.module_from_spec(spec)
            with hermes_checkout_path_context(self.settings):
                spec.loader.exec_module(module)
        else:
            if self.settings.database_backend != "sqlite":
                raise HermesSaaSContractError(
                    "FLORENCE_HERMES_AGENT_PATH is required for deployed SaaS "
                    "traffic; ambient run_agent imports are local development only"
                )
            module = importlib.import_module("run_agent")
        return getattr(module, "AIAgent")

    def _system_message(
        self,
        *,
        household: Household,
        upcoming: list[tuple[str, object]],
        memories: list[MemoryRecord],
        members: list[HouseholdMember],
        actor: HouseholdMember,
        now_utc: datetime,
        source_preferences: list[SourcePreference],
        privacy: HouseholdPrivacy,
        readiness: HouseholdReadiness,
    ) -> str:
        now = ensure_utc(now_utc)
        upcoming_lines = []
        for title, due_at in upcoming[:8]:
            try:
                when = format_local(due_at, household.timezone)  # type: ignore[arg-type]
            except Exception:
                when = "time unknown"
            safe_title = _redact_private_text_for_hermes(str(title))
            upcoming_lines.append(f"- {safe_title}: {when}")
        upcoming_text = "\n".join(upcoming_lines) if upcoming_lines else "- Nothing urgent."
        members_by_id = _member_context_labels(members)
        memory_lines = []
        for memory in memories[:20]:
            source = members_by_id.get(memory.asserted_by_member_id or "")
            suffix = f" (from {source})" if source else ""
            safe_memory = _redact_private_text_for_hermes(memory.text)
            memory_lines.append(f"- {memory.kind.value}: {safe_memory}{suffix}")
        memory_text = "\n".join(memory_lines) if memory_lines else "- No durable household memories yet."
        member_lines = [f"- {members_by_id[member.id]}: {member.role.value}" for member in members]
        members_text = "\n".join(member_lines) if member_lines else "- No members recorded yet."
        source_rule_lines = []
        for preference in source_preferences[:20]:
            label = (
                "always surface"
                if preference.preference == SourcePreferenceKind.ALWAYS_SURFACE
                else "mute"
            )
            safe_phrase = _redact_private_text_for_hermes(preference.phrase)
            source_rule_lines.append(f"- {label}: {safe_phrase}")
        source_rules_text = (
            "\n".join(source_rule_lines)
            if source_rule_lines
            else "- No household source rules yet."
        )
        if readiness.ready:
            readiness_text = "- Ready for a pilot."
        else:
            missing = "\n".join(f"- {item}" for item in readiness.missing[:6])
            readiness_text = f"- Not ready yet.\n{missing}"
        memory_status = "on" if privacy.memory_enabled else "paused"
        analytics_status = "on" if privacy.product_analytics_opt_in else "off"
        actor_name = members_by_id.get(actor.id) or _member_context_label(actor)
        local_now = format_local(now, household.timezone)
        return (
            f"{SYSTEM_PERSONA}\n\n"
            f"{SAAS_BOUNDARY_PROTOCOL}\n"
            f"{PROPOSAL_PROTOCOL}\n"
            f"Household timezone: {household.timezone}\n"
            f"Current local time: {local_now}\n\n"
            f"Current sender: {actor_name} ({actor.role.value})\n"
            f"Household members:\n{members_text}\n\n"
            f"Household setup/readiness:\n{readiness_text}\n\n"
            f"Household privacy controls:\n"
            f"- Memory: {memory_status}\n"
            f"- Product analytics: {analytics_status}\n"
            f"- Cross-family memory sharing: off\n\n"
            f"Household connected-source rules:\n{source_rules_text}\n\n"
            f"Durable household memory for this family only:\n{memory_text}\n\n"
            f"Known upcoming household commitments:\n{upcoming_text}\n"
        )


def _redact_conversation_history_for_hermes(
    conversation_history: list[dict[str, str]],
) -> list[dict[str, str]]:
    redacted: list[dict[str, str]] = []
    for message in conversation_history:
        safe_message = dict(message)
        content = safe_message.get("content")
        if isinstance(content, str):
            safe_message["content"] = _redact_private_text_for_hermes(content)
        redacted.append(safe_message)
    return redacted


def _redact_private_text_for_hermes(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = re.sub(r"\D", "", raw)
        if raw.startswith("+") and 8 <= len(digits) <= 15:
            return PHONE_REDACTION
        if len(digits) == 10 or (len(digits) == 11 and digits.startswith("1")):
            return PHONE_REDACTION
        return raw

    return EMAIL_LIKE_RE.sub(EMAIL_REDACTION, PHONE_LIKE_RE.sub(replace, text))


def configure_hermes_runtime_home(settings: Settings, *, scope: str = "preflight") -> str:
    path = _scoped_hermes_runtime_home(settings.hermes_runtime_home, scope=scope)
    path.mkdir(parents=True, exist_ok=True)
    probe = path / ".florence-write-test"
    probe.write_text("ok")
    probe.unlink(missing_ok=True)
    resolved = str(path)
    os.environ["HERMES_HOME"] = resolved
    return resolved


@contextmanager
def hermes_runtime_home_context(
    settings: Settings,
    *,
    scope: str = "preflight",
    cleanup: bool = False,
):
    with HERMES_RUNTIME_LOCK:
        with hermes_runtime_file_lock(settings):
            previous_home = os.environ.get("HERMES_HOME")
            path = Path(configure_hermes_runtime_home(settings, scope=scope))
            try:
                yield path
            finally:
                if previous_home is None:
                    os.environ.pop("HERMES_HOME", None)
                else:
                    os.environ["HERMES_HOME"] = previous_home
                if cleanup:
                    shutil.rmtree(path, ignore_errors=True)


@contextmanager
def hermes_runtime_file_lock(settings: Settings):
    base = _normalized_hermes_runtime_home(settings.hermes_runtime_home)
    base.mkdir(parents=True, exist_ok=True)
    lock_path = base / HERMES_RUNTIME_LOCK_FILE
    with lock_path.open("a+") as lock_file:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield lock_path
        finally:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def hermes_runtime_lock_mode() -> str:
    return HERMES_INTERPROCESS_LOCK_MODE if fcntl is not None else HERMES_THREAD_ONLY_LOCK_MODE


def hermes_runtime_concurrency_mode() -> str:
    if fcntl is not None:
        return HERMES_INTERPROCESS_CONCURRENCY_MODE
    return HERMES_THREAD_ONLY_CONCURRENCY_MODE


def hermes_runtime_lock_error() -> str | None:
    if fcntl is not None:
        return None
    return (
        "POSIX fcntl file locking is required for deployed SaaS Hermes traffic; "
        "HERMES_HOME is process-global and cannot be safely isolated across app "
        "and worker processes without an interprocess lock"
    )


@contextmanager
def hermes_checkout_path_context(settings: Settings):
    if not settings.hermes_agent_path:
        yield None
        return
    path_text = str(Path(settings.hermes_agent_path).expanduser().resolve())
    inserted = path_text not in sys.path
    if inserted:
        sys.path.insert(0, path_text)
    try:
        yield Path(path_text)
    finally:
        if inserted:
            try:
                sys.path.remove(path_text)
            except ValueError:
                pass


@contextmanager
def hermes_checkout_module_context(settings: Settings):
    if not settings.hermes_agent_path:
        yield None
        return
    checkout_path = Path(settings.hermes_agent_path).expanduser().resolve()
    shadowed = _shadow_checkout_module_names(checkout_path)
    before = set(sys.modules)
    try:
        yield checkout_path
    finally:
        _remove_new_modules_from_path(checkout_path, before)
        _restore_shadowed_modules(shadowed)


def _shadow_checkout_module_names(checkout_path: Path) -> dict[str, object]:
    root_names = _checkout_top_level_module_names(checkout_path)
    shadowed: dict[str, object] = {}
    for name, module in list(sys.modules.items()):
        root = name.partition(".")[0]
        if root in root_names:
            shadowed[name] = module
            sys.modules.pop(name, None)
    return shadowed


def _restore_shadowed_modules(shadowed: dict[str, object]) -> None:
    for name, module in shadowed.items():
        sys.modules[name] = module


def _checkout_top_level_module_names(checkout_path: Path) -> set[str]:
    names = set()
    try:
        children = list(checkout_path.iterdir())
    except OSError:
        return names
    for child in children:
        name = child.name
        if name.startswith(".") or name == "__pycache__":
            continue
        if child.is_file() and child.suffix == ".py" and child.stem.isidentifier():
            names.add(child.stem)
        elif child.is_dir() and name.isidentifier():
            names.add(name)
    return names


def _remove_new_modules_from_path(checkout_path: Path, before: set[str]) -> None:
    for name, module in list(sys.modules.items()):
        if name in before:
            continue
        if _module_loaded_from_path(module, checkout_path):
            sys.modules.pop(name, None)


def _module_loaded_from_path(module: object, checkout_path: Path) -> bool:
    candidates: list[object] = [
        getattr(module, "__file__", None),
        getattr(getattr(module, "__spec__", None), "origin", None),
    ]
    candidates.extend(_module_search_paths(module))
    return any(_path_is_under(candidate, checkout_path) for candidate in candidates)


def _module_search_paths(module: object) -> list[object]:
    try:
        module_path = getattr(module, "__path__", None)
    except (AttributeError, KeyError, TypeError, ValueError):
        return []
    if module_path is None:
        return []
    try:
        return list(module_path)
    except (AttributeError, KeyError, TypeError, ValueError):
        return []


def hermes_checkout_ref(path: Path) -> str | None:
    resolved = path.expanduser()
    marker = resolved / HERMES_CHECKOUT_REF_FILE
    if marker.exists():
        value = marker.read_text(errors="ignore").strip()
        return value or None
    return _git_head_ref(resolved)


def _git_head_ref(path: Path) -> str | None:
    git_dir = path / ".git"
    if git_dir.is_file():
        text = git_dir.read_text(errors="ignore").strip()
        if text.startswith("gitdir:"):
            git_dir = (path / text.split(":", 1)[1].strip()).resolve()
    if not git_dir.exists():
        return None
    head = git_dir / "HEAD"
    if not head.exists():
        return None
    value = head.read_text(errors="ignore").strip()
    if not value:
        return None
    if value.startswith("ref:"):
        ref_path = git_dir / value.split(":", 1)[1].strip()
        if ref_path.exists():
            ref_value = ref_path.read_text(errors="ignore").strip()
            return ref_value or None
    return value


def _refs_match(expected: str | None, actual: str | None) -> bool:
    if not expected or not actual:
        return False
    expected_clean = expected.strip().lower()
    actual_clean = actual.strip().lower()
    if not expected_clean or not actual_clean:
        return False
    return actual_clean == expected_clean


def _path_is_under(candidate: object, root: Path) -> bool:
    if not candidate or candidate in {"built-in", "frozen"}:
        return False
    try:
        return Path(str(candidate)).expanduser().resolve().is_relative_to(root)
    except (OSError, ValueError):
        return False


def hermes_runtime_home_error(value: str | None) -> str | None:
    try:
        path = _normalized_hermes_runtime_home(value)
    except ValueError as exc:
        return str(exc)
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".florence-write-test"
        probe.write_text("ok")
        probe.unlink(missing_ok=True)
    except OSError as exc:
        return f"FLORENCE_HERMES_RUNTIME_HOME is not writable: {type(exc).__name__}: {exc}"
    return None


def scoped_hermes_runtime_home(value: str | None, *, scope: str = "preflight") -> str:
    return str(_scoped_hermes_runtime_home(value, scope=scope))


def hermes_preflight_scope() -> str:
    return f"florence-preflight-{os.getpid()}-{uuid.uuid4()}"


def _scoped_hermes_runtime_home(value: str | None, *, scope: str) -> Path:
    safe_scope = str(scope).strip()
    if not safe_scope or "/" in safe_scope or "\\" in safe_scope or safe_scope in {".", ".."}:
        raise ValueError("Hermes runtime scope must be a single path segment")
    return _normalized_hermes_runtime_home(value) / safe_scope


def _normalized_hermes_runtime_home(value: str | None) -> Path:
    if not value or not str(value).strip():
        raise ValueError("FLORENCE_HERMES_RUNTIME_HOME is not set")
    path = Path(str(value).strip()).expanduser()
    if not path.is_absolute():
        raise ValueError("FLORENCE_HERMES_RUNTIME_HOME must be an absolute path")
    resolved = path.resolve()
    default_home = Path.home().joinpath(HERMES_DEFAULT_HOME_NAME).resolve()
    if resolved == default_home:
        raise ValueError(
            "FLORENCE_HERMES_RUNTIME_HOME must not use Hermes default ~/.hermes "
            "for SaaS traffic"
        )
    return resolved


def _member_context_labels(members: list[HouseholdMember]) -> dict[str, str]:
    labels: dict[str, str] = {}
    role_counts: dict[str, int] = {}
    for member in members:
        label = _member_context_label(member)
        if member.display_name:
            labels[member.id] = label
            continue
        role_counts[member.role.value] = role_counts.get(member.role.value, 0) + 1
        count = role_counts[member.role.value]
        labels[member.id] = label if count == 1 else f"{label} {count}"
    return labels


def _member_context_label(member: HouseholdMember) -> str:
    if member.display_name:
        safe_name = _redact_private_text_for_hermes(member.display_name).strip()
        if safe_name and safe_name != PHONE_REDACTION:
            return safe_name
    return f"unnamed {member.role.value}"
