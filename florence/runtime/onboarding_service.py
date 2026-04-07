"""Onboarding session service for Florence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable

from florence.contracts import ChildProfile, HouseholdProfileItem, HouseholdProfileKind
from florence.onboarding import (
    OnboardingPrompt,
    OnboardingStage,
    OnboardingState,
    OnboardingTransition,
    apply_child_names,
    apply_child_profile_updates,
    apply_parent_name,
    build_onboarding_prompt,
    build_onboarding_prompt_message_sequence,
    build_onboarding_ready_message_sequence,
    build_onboarding_transition_message_sequence,
    extract_child_names,
    mark_google_connected,
    split_entries,
    sync_onboarding_stage,
)
from florence.onboarding.intake import FlorenceOnboardingIntakeService
from florence.runtime.candidate_review import FlorenceCandidateReviewService
from florence.runtime.services import (
    _augment_onboarding_prompt,
    _clean_label,
    _grounding_hints_from_settings,
    _index_hint_entries,
    _merge_metadata_list,
    _stable_id,
)
from florence.state import FlorenceStateDB


@dataclass(slots=True)
class _OnboardingProtocolReply:
    reply_text: str | None = None
    reply_messages: tuple[str, ...] = ()


class FlorenceOnboardingSessionService:
    """Persisted deterministic onboarding flow for a parent DM."""

    def __init__(
        self,
        store: FlorenceStateDB,
        *,
        candidate_review_service: FlorenceCandidateReviewService | None = None,
        intake_service: FlorenceOnboardingIntakeService | None = None,
        link_url_builder: Callable[[str, str, str], str | None] | None = None,
    ):
        self.store = store
        self.candidate_review_service = candidate_review_service
        self.intake_service = intake_service or FlorenceOnboardingIntakeService()
        self.link_url_builder = link_url_builder

    def get_or_create_session(self, *, household_id: str, member_id: str, thread_id: str) -> OnboardingState:
        existing = self.store.get_onboarding_session(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        if existing is not None:
            return existing

        previous_sessions = self.store.list_member_onboarding_sessions(
            household_id=household_id,
            member_id=member_id,
        )
        if previous_sessions:
            resumed = replace(previous_sessions[0], thread_id=thread_id)
            self.store.upsert_onboarding_session(resumed)
            return resumed

        state = OnboardingState(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        self.store.upsert_onboarding_session(state)
        return state

    def get_prompt(self, *, household_id: str, member_id: str, thread_id: str) -> OnboardingPrompt | None:
        state = self.get_or_create_session(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        household = self.store.get_household(household_id)
        return _augment_onboarding_prompt(
            build_onboarding_prompt(state),
            settings=household.settings if household is not None else None,
        )

    def get_prompt_messages(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        link_url: str | None = None,
    ) -> tuple[str, ...]:
        return build_onboarding_prompt_message_sequence(
            self.get_prompt(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            ),
            link_url=link_url or self._build_link_url(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            ),
        )

    def get_transition_messages(
        self,
        transition: OnboardingTransition,
        *,
        previous_stage: OnboardingStage,
        household_id: str,
        member_id: str,
        thread_id: str,
        link_url: str | None = None,
    ) -> tuple[str, ...]:
        return build_onboarding_transition_message_sequence(
            transition,
            previous_stage=previous_stage,
            link_url=link_url or self._build_link_url(
                household_id=household_id,
                member_id=member_id,
                thread_id=thread_id,
            ),
        )

    def set_link_url_builder(
        self,
        builder: Callable[[str, str, str], str | None] | None,
    ) -> None:
        self.link_url_builder = builder

    @staticmethod
    def get_ready_messages() -> tuple[str, ...]:
        return build_onboarding_ready_message_sequence()

    def get_google_connect_retry_messages(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
    ) -> tuple[str, ...]:
        link_url = self._build_link_url(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        messages = ["I still don’t see your Google account connected yet."]
        if link_url:
            messages.append(link_url)
            messages.append("Once Google says you're connected, come back here and text done.")
        return tuple(messages)

    def handle_google_done_followup(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        continue_with_household_chat: Callable[[str], tuple[str | None, tuple[str, ...]] | None],
    ) -> _OnboardingProtocolReply:
        member_connections = self.store.list_google_connections(
            household_id=household_id,
            member_id=member_id,
        )
        if member_connections:
            chat_result = continue_with_household_chat(
                "My Google account is connected now. Continue with the inbox or calendar lookup you just offered."
            )
            if chat_result is not None:
                reply_text, reply_messages = chat_result
                return _OnboardingProtocolReply(
                    reply_text=reply_text,
                    reply_messages=reply_messages,
                )
            messages = self.get_ready_messages()
            return _OnboardingProtocolReply(
                reply_text=messages[0] if messages else None,
                reply_messages=messages,
            )

        messages = self.get_google_connect_retry_messages(
            household_id=household_id,
            member_id=member_id,
            thread_id=thread_id,
        )
        return _OnboardingProtocolReply(
            reply_text=messages[0] if messages else None,
            reply_messages=messages,
        )

    def _build_link_url(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
    ) -> str | None:
        if self.link_url_builder is None:
            return None
        return self.link_url_builder(household_id, member_id, thread_id)

    def record_parent_name(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        display_name: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        member = self.store.get_member(member_id)
        if member is not None:
            self.store.upsert_member(replace(member, display_name=display_name.strip() or member.display_name))
        return self._persist_transition(apply_parent_name(state, display_name))

    def record_user_reply(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        text: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        intake = self.intake_service.parse(state=state, text=text)
        next_state = state
        absorbed_fragmented_child_names = False

        if intake.parent_name and not next_state.parent_display_name:
            next_state = apply_parent_name(next_state, intake.parent_name).state
            member = self.store.get_member(member_id)
            if member is not None:
                self.store.upsert_member(replace(member, display_name=intake.parent_name))

        if intake.child_names:
            next_state = apply_child_names(next_state, intake.child_names).state
        elif self._should_absorb_fragmented_child_names(state=state, text=text):
            fragmented_names = extract_child_names(split_entries(text))
            if fragmented_names:
                next_state = apply_child_names(next_state, fragmented_names).state
                absorbed_fragmented_child_names = True

        if intake.child_updates and not absorbed_fragmented_child_names:
            next_state = apply_child_profile_updates(next_state, intake.child_updates).state

        if intake.google_connected and not next_state.google_connected:
            next_state = mark_google_connected(next_state).state

        next_state = sync_onboarding_stage(next_state)
        changed = next_state != state
        return self._persist_transition(
            OnboardingTransition(
                state=next_state,
                prompt=build_onboarding_prompt(next_state),
                changed=changed and not intake.ignore_message,
            )
        )

    @staticmethod
    def _should_absorb_fragmented_child_names(*, state: OnboardingState, text: str) -> bool:
        if state.stage != OnboardingStage.COLLECT_CHILD_AGE:
            return False
        cleaned = " ".join(str(text or "").split()).strip()
        if not cleaned or any(char.isdigit() for char in cleaned):
            return False
        lowered = cleaned.lower()
        disallowed_markers = (
            "school",
            "elementary",
            "middle",
            "high",
            "preschool",
            "pre school",
            "pre-school",
            "kindergarten",
            "daycare",
            "grade",
            "years old",
            "year old",
            " years",
            " year",
            " old",
            "turning",
            "turns",
            "soccer",
            "baseball",
            "basketball",
            "dance",
            "music",
            "piano",
            "violin",
            "swim",
            "camp",
            "class",
            "not yet",
            "none",
        )
        if any(marker in lowered for marker in disallowed_markers):
            return False
        candidate_names = extract_child_names(split_entries(cleaned))
        if not candidate_names:
            return False
        existing = {name.strip().lower() for name in state.child_names if name.strip()}
        return any(name.strip().lower() not in existing for name in candidate_names)

    def record_google_connected(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(mark_google_connected(state))

    def record_child_names(
        self,
        *,
        household_id: str,
        member_id: str,
        thread_id: str,
        child_names: list[str],
        child_details: list[str] | None = None,
    ) -> OnboardingTransition:
        state = self.get_or_create_session(household_id=household_id, member_id=member_id, thread_id=thread_id)
        return self._persist_transition(apply_child_names(state, child_names, child_details=child_details))

    def _persist_transition(self, transition: OnboardingTransition) -> OnboardingTransition:
        self.store.upsert_onboarding_session(transition.state)
        self._sync_household_grounding(transition.state)
        household = self.store.get_household(transition.state.household_id)
        prompt = _augment_onboarding_prompt(
            transition.prompt,
            settings=household.settings if household is not None else None,
        )
        if transition.state.is_grounded_for_google_matching and self.candidate_review_service is not None:
            self.candidate_review_service.release_quarantined_candidates(
                household_id=transition.state.household_id,
                member_id=transition.state.member_id,
            )
        return replace(transition, prompt=prompt)

    def _sync_household_grounding(self, state: OnboardingState) -> None:
        household = self.store.get_household(state.household_id)
        grounding_hints = _grounding_hints_from_settings(household.settings if household is not None else None)
        school_hints = _index_hint_entries(
            grounding_hints,
            key="schools",
            detail_fields=("domains", "platforms", "contacts"),
        )
        activity_hints = _index_hint_entries(
            grounding_hints,
            key="activities",
            detail_fields=("locations", "contacts"),
        )
        if state.child_names or state.stage not in {
            OnboardingStage.COLLECT_PARENT_NAME,
            OnboardingStage.CONNECT_GOOGLE,
            OnboardingStage.COLLECT_CHILD_NAMES,
        }:
            child_profile_map = {
                str(profile.get("name")).strip().lower(): profile
                for profile in state.child_profiles
                if isinstance(profile, dict) and str(profile.get("name") or "").strip()
            }
            existing_children = {
                child.full_name.strip().lower(): child
                for child in self.store.list_child_profiles(household_id=state.household_id)
            }
            children: list[ChildProfile] = []
            for child_name in state.child_names:
                cleaned_name = child_name.strip()
                if not cleaned_name:
                    continue
                existing_child = existing_children.get(cleaned_name.lower())
                metadata = dict(existing_child.metadata) if existing_child is not None else {}
                first_name = _clean_label(cleaned_name.split()[0] if cleaned_name else None)
                if first_name is not None and first_name.lower() != cleaned_name.lower():
                    _merge_metadata_list(metadata, "aliases", [first_name])
                parsed_profile = child_profile_map.get(cleaned_name.lower())
                parsed_age = _clean_label(parsed_profile.get("age")) if isinstance(parsed_profile, dict) else None
                if parsed_age is not None:
                    metadata["age"] = parsed_age
                children.append(
                    ChildProfile(
                        id=_stable_id("child", state.household_id, cleaned_name.lower()),
                        household_id=state.household_id,
                        full_name=cleaned_name,
                        metadata=metadata,
                    )
                )
            self.store.replace_child_profiles(household_id=state.household_id, children=children)

        if state.school_labels:
            existing_schools = {
                item.label.strip().lower(): item
                for item in self.store.list_household_profile_items(
                    household_id=state.household_id,
                    kind=HouseholdProfileKind.SCHOOL,
                )
            }
            schools = [
                self._build_school_profile_item(
                    state=state,
                    label=label.strip(),
                    existing=existing_schools.get(label.strip().lower()),
                    hint=school_hints.get(label.strip().lower()),
                )
                for label in state.school_labels
                if label.strip()
            ]
            self.store.replace_household_profile_items(
                household_id=state.household_id,
                kind=HouseholdProfileKind.SCHOOL,
                items=schools,
            )

        if state.activity_labels:
            existing_activities = {
                item.label.strip().lower(): item
                for item in self.store.list_household_profile_items(
                    household_id=state.household_id,
                    kind=HouseholdProfileKind.ACTIVITY,
                )
            }
            activities = [
                self._build_activity_profile_item(
                    state=state,
                    label=label.strip(),
                    existing=existing_activities.get(label.strip().lower()),
                    hint=activity_hints.get(label.strip().lower()),
                )
                for label in state.activity_labels
                if label.strip()
            ]
            self.store.replace_household_profile_items(
                household_id=state.household_id,
                kind=HouseholdProfileKind.ACTIVITY,
                items=activities,
            )

    def _build_school_profile_item(
        self,
        *,
        state: OnboardingState,
        label: str,
        existing: HouseholdProfileItem | None,
        hint: dict[str, list[str]] | None,
    ) -> HouseholdProfileItem:
        metadata = dict(existing.metadata) if existing is not None else {}
        if hint is not None:
            _merge_metadata_list(metadata, "domains", list(hint.get("domains", [])))
            _merge_metadata_list(metadata, "platforms", list(hint.get("platforms", [])))
            _merge_metadata_list(metadata, "contacts", list(hint.get("contacts", [])))
        return HouseholdProfileItem(
            id=_stable_id("school", state.household_id, label.lower()),
            household_id=state.household_id,
            kind=HouseholdProfileKind.SCHOOL,
            label=label,
            member_id=state.member_id,
            child_id=existing.child_id if existing is not None else None,
            metadata=metadata,
        )

    def _build_activity_profile_item(
        self,
        *,
        state: OnboardingState,
        label: str,
        existing: HouseholdProfileItem | None,
        hint: dict[str, list[str]] | None,
    ) -> HouseholdProfileItem:
        metadata = dict(existing.metadata) if existing is not None else {}
        if hint is not None:
            _merge_metadata_list(metadata, "locations", list(hint.get("locations", [])))
            _merge_metadata_list(metadata, "contacts", list(hint.get("contacts", [])))
        return HouseholdProfileItem(
            id=_stable_id("activity", state.household_id, label.lower()),
            household_id=state.household_id,
            kind=HouseholdProfileKind.ACTIVITY,
            label=label,
            member_id=state.member_id,
            child_id=existing.child_id if existing is not None else None,
            metadata=metadata,
        )
