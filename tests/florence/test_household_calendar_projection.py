from florence.contracts import (
    GoogleConnection,
    GoogleSourceKind,
    Household,
    HouseholdEvent,
    HouseholdEventStatus,
    Member,
    MemberRole,
)
from florence.google.types import GoogleCalendarMetadata
from florence.runtime.household_calendar_projection import (
    HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY,
    HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY,
    HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY,
    FlorenceHouseholdCalendarProjectionService,
)
from florence.runtime.household_merge import FlorenceHouseholdMergeService
from florence.state import FlorenceStateDB


def test_household_calendar_projection_ensures_shared_calendar_and_marks_connection(monkeypatch, tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_123", name="Maya household", timezone="America/Los_Angeles"))
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={},
        )
    )

    monkeypatch.setattr(
        "florence.runtime.household_calendar_projection.create_google_calendar",
        lambda **_: GoogleCalendarMetadata(
            id="cal_shared_123",
            summary="Florence - Maya household",
            timezone="America/Los_Angeles",
        ),
    )

    service = FlorenceHouseholdCalendarProjectionService(store)
    projection = service.ensure_projection(household_id="hh_123", preferred_connection_id="gconn_123")

    household = store.get_household("hh_123")
    connection = store.get_google_connection("gconn_123")
    assert projection is not None
    assert projection["calendar_id"] == "cal_shared_123"
    assert projection["host_email"] == "maya@example.com"
    assert projection["calendar_web_url"] == "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123"
    assert household is not None
    assert household.settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY]["calendar_id"] == "cal_shared_123"
    assert connection is not None
    assert connection.metadata["florence_projection_calendar_id"] == "cal_shared_123"
    assert connection.metadata[HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY] == ["cal_shared_123"]
    store.close()


def test_household_calendar_projection_reuses_existing_calendar_and_shares_to_later_connected_parent(monkeypatch, tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya household",
            timezone="America/Los_Angeles",
            settings={
                HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY: {
                    "host_connection_id": "gconn_host",
                    "host_email": "maya@example.com",
                    "calendar_id": "cal_shared_123",
                    "calendar_summary": "Florence - Maya household",
                    "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_shared_123",
                    "timezone": "America/Los_Angeles",
                    "status": "active",
                    "shared_with_connection_ids": [],
                    "shared_with_emails": [],
                }
            },
        )
    )
    store.upsert_member(
        Member(
            id="mem_host",
            household_id="hh_123",
            display_name="Maya",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_member(
        Member(
            id="mem_viewer",
            household_id="hh_123",
            display_name="Kendall",
            role=MemberRole.PARENT,
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_host",
            household_id="hh_123",
            member_id="mem_host",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="host-access-token",
            metadata={"florence_projection_calendar_id": "cal_shared_123"},
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_viewer",
            household_id="hh_123",
            member_id="mem_viewer",
            email="kendall@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="viewer-access-token",
            metadata={},
        )
    )

    shared: list[tuple[str, str]] = []
    inserted: list[tuple[str, str]] = []

    monkeypatch.setattr(
        "florence.runtime.household_calendar_projection.create_google_calendar",
        lambda **_: (_ for _ in ()).throw(AssertionError("should reuse existing calendar")),
    )
    monkeypatch.setattr(
        "florence.runtime.household_calendar_projection.share_google_calendar_with_user",
        lambda **kwargs: shared.append((kwargs["calendar_id"], kwargs["user_email"])) or {"id": "rule_123"},
    )
    monkeypatch.setattr(
        "florence.runtime.household_calendar_projection.add_google_calendar_to_calendar_list",
        lambda **kwargs: inserted.append((kwargs["calendar_id"], kwargs["access_token"])) or {"id": kwargs["calendar_id"]},
    )

    service = FlorenceHouseholdCalendarProjectionService(store)
    projection = service.ensure_projection(household_id="hh_123", preferred_connection_id="gconn_viewer")

    household = store.get_household("hh_123")
    host_connection = store.get_google_connection("gconn_host")
    viewer_connection = store.get_google_connection("gconn_viewer")
    assert projection is not None
    assert projection["calendar_id"] == "cal_shared_123"
    assert projection["shared_with_connection_ids"] == ["gconn_viewer"]
    assert projection["shared_with_emails"] == ["kendall@example.com"]
    assert shared == [("cal_shared_123", "kendall@example.com")]
    assert inserted == [("cal_shared_123", "viewer-access-token")]
    assert household is not None
    assert household.settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY]["calendar_id"] == "cal_shared_123"
    assert host_connection is not None
    assert viewer_connection is not None
    assert host_connection.metadata[HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY] == ["cal_shared_123"]
    assert viewer_connection.metadata[HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY] == ["cal_shared_123"]
    store.close()


def test_household_calendar_projection_syncs_confirmed_events_and_removes_non_confirmed(monkeypatch, tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_123",
            name="Maya household",
            timezone="America/Los_Angeles",
            settings={
                HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY: {
                    "host_connection_id": "gconn_123",
                    "calendar_id": "cal_shared_123",
                    "calendar_summary": "Florence - Maya household",
                    "timezone": "America/Los_Angeles",
                    "status": "active",
                }
            },
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_123",
            household_id="hh_123",
            member_id="mem_123",
            email="maya@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="access-token",
            metadata={"florence_projection_calendar_id": "cal_shared_123"},
        )
    )
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_confirmed_123",
            household_id="hh_123",
            title="Theo music class",
            starts_at="2026-06-10T15:30:00-07:00",
            ends_at="2026-06-10T16:15:00-07:00",
            timezone="America/Los_Angeles",
            status=HouseholdEventStatus.CONFIRMED,
        )
    )
    store.upsert_household_event(
        HouseholdEvent(
            id="evt_tentative_123",
            household_id="hh_123",
            title="Possible camp carpool",
            starts_at="2026-06-12T15:00:00-07:00",
            ends_at="2026-06-12T16:00:00-07:00",
            timezone="America/Los_Angeles",
            status=HouseholdEventStatus.TENTATIVE,
            metadata={HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY: "gcal_old_123"},
        )
    )

    projected: list[tuple[str, str | None]] = []
    deleted: list[str] = []

    def _upsert_google_calendar_event(**kwargs):
        projected.append((kwargs["event"].id, kwargs.get("google_event_id")))
        return {"id": f'gcal_{kwargs["event"].id}'}

    monkeypatch.setattr(
        "florence.runtime.household_calendar_projection.upsert_google_calendar_event",
        _upsert_google_calendar_event,
    )
    monkeypatch.setattr(
        "florence.runtime.household_calendar_projection.delete_google_calendar_event",
        lambda **kwargs: deleted.append(kwargs["google_event_id"]),
    )

    service = FlorenceHouseholdCalendarProjectionService(store)
    projection = service.sync_household(household_id="hh_123")

    confirmed = next(event for event in store.list_household_events(household_id="hh_123") if event.id == "evt_confirmed_123")
    tentative = next(event for event in store.list_household_events(household_id="hh_123") if event.id == "evt_tentative_123")
    assert projection is not None
    assert projection["last_projected_event_count"] == 1
    assert projected == [("evt_confirmed_123", None)]
    assert confirmed.metadata[HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY] == "gcal_evt_confirmed_123"
    assert deleted == ["gcal_old_123"]
    assert HOUSEHOLD_CALENDAR_PROJECTION_EVENT_ID_KEY not in tentative.metadata
    store.close()


def test_household_merge_service_keeps_target_projection_and_retires_source_projection(monkeypatch, tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(
        Household(
            id="hh_primary",
            name="Jackson household",
            timezone="America/Los_Angeles",
            settings={
                HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY: {
                    "host_connection_id": "gconn_primary",
                    "host_email": "jackson@example.com",
                    "calendar_id": "cal_primary",
                    "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_primary",
                    "status": "active",
                }
            },
        )
    )
    store.upsert_household(
        Household(
            id="hh_secondary",
            name="Kendall household",
            timezone="America/Los_Angeles",
            settings={
                HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY: {
                    "host_connection_id": "gconn_secondary",
                    "host_email": "kendall@example.com",
                    "calendar_id": "cal_secondary",
                    "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_secondary",
                    "status": "active",
                }
            },
        )
    )
    primary_member = store.upsert_member(
        Member(
            id="mem_primary",
            household_id="hh_primary",
            display_name="Jackson",
            role=MemberRole.ADMIN,
        )
    )
    secondary_member = store.upsert_member(
        Member(
            id="mem_secondary",
            household_id="hh_secondary",
            display_name="Kendall",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_primary",
            household_id="hh_primary",
            member_id="mem_primary",
            email="jackson@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="primary-token",
            metadata={"florence_projection_calendar_id": "cal_primary"},
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_secondary",
            household_id="hh_secondary",
            member_id="mem_secondary",
            email="kendall@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="secondary-token",
            metadata={"florence_projection_calendar_id": "cal_secondary"},
        )
    )

    projection_service = FlorenceHouseholdCalendarProjectionService(store)
    ensured: list[tuple[str, str | None]] = []
    deleted: list[str] = []
    monkeypatch.setattr(
        projection_service,
        "ensure_projection",
        lambda **kwargs: ensured.append((kwargs["household_id"], kwargs.get("preferred_connection_id"))) or kwargs,
    )
    monkeypatch.setattr(
        "florence.runtime.household_merge.delete_google_calendar",
        lambda **kwargs: deleted.append(kwargs["calendar_id"]),
    )

    service = FlorenceHouseholdMergeService(
        store,
        household_calendar_projection_service=projection_service,
    )
    merged_household_id = service.merge_group_households(
        target_household_id="hh_primary",
        matching_households={
            "hh_primary": [primary_member],
            "hh_secondary": [secondary_member],
        },
    )

    merged_household = store.get_household("hh_primary")
    moved_connection = store.get_google_connection("gconn_secondary")
    assert merged_household_id == "hh_primary"
    assert store.get_household("hh_secondary") is None
    assert merged_household is not None
    assert merged_household.settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY]["calendar_id"] == "cal_primary"
    assert deleted == ["cal_secondary"]
    assert ensured == [("hh_primary", "gconn_primary")]
    assert moved_connection is not None
    assert moved_connection.household_id == "hh_primary"
    assert moved_connection.metadata[HOUSEHOLD_CALENDAR_MANAGED_IDS_METADATA_KEY] == ["cal_secondary"]
    assert "florence_projection_calendar_id" not in moved_connection.metadata
    store.close()


def test_household_merge_service_moves_source_projection_when_target_has_none(monkeypatch, tmp_path):
    store = FlorenceStateDB(tmp_path / "florence.db")
    store.upsert_household(Household(id="hh_primary", name="Jackson household", timezone="America/Los_Angeles"))
    store.upsert_household(
        Household(
            id="hh_secondary",
            name="Kendall household",
            timezone="America/Los_Angeles",
            settings={
                HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY: {
                    "host_connection_id": "gconn_secondary",
                    "host_email": "kendall@example.com",
                    "calendar_id": "cal_secondary",
                    "calendar_web_url": "https://calendar.google.com/calendar/u/0/r?cid=cal_secondary",
                    "status": "active",
                }
            },
        )
    )
    primary_member = store.upsert_member(
        Member(
            id="mem_primary",
            household_id="hh_primary",
            display_name="Jackson",
            role=MemberRole.ADMIN,
        )
    )
    secondary_member = store.upsert_member(
        Member(
            id="mem_secondary",
            household_id="hh_secondary",
            display_name="Kendall",
            role=MemberRole.ADMIN,
        )
    )
    store.upsert_google_connection(
        GoogleConnection(
            id="gconn_secondary",
            household_id="hh_secondary",
            member_id="mem_secondary",
            email="kendall@example.com",
            connected_scopes=(GoogleSourceKind.GMAIL, GoogleSourceKind.GOOGLE_CALENDAR),
            access_token="secondary-token",
            metadata={"florence_projection_calendar_id": "cal_secondary"},
        )
    )

    projection_service = FlorenceHouseholdCalendarProjectionService(store)
    ensured: list[tuple[str, str | None]] = []
    deleted: list[str] = []
    monkeypatch.setattr(
        projection_service,
        "ensure_projection",
        lambda **kwargs: ensured.append((kwargs["household_id"], kwargs.get("preferred_connection_id"))) or kwargs,
    )
    monkeypatch.setattr(
        "florence.runtime.household_merge.delete_google_calendar",
        lambda **kwargs: deleted.append(kwargs["calendar_id"]),
    )

    service = FlorenceHouseholdMergeService(
        store,
        household_calendar_projection_service=projection_service,
    )
    merged_household_id = service.merge_group_households(
        target_household_id="hh_primary",
        matching_households={
            "hh_primary": [primary_member],
            "hh_secondary": [secondary_member],
        },
    )

    merged_household = store.get_household("hh_primary")
    assert merged_household_id == "hh_primary"
    assert merged_household is not None
    assert merged_household.settings[HOUSEHOLD_CALENDAR_PROJECTION_SETTINGS_KEY]["calendar_id"] == "cal_secondary"
    assert ensured == [("hh_primary", "gconn_secondary")]
    assert deleted == []
    store.close()
