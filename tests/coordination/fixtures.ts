import {
  type DestinationEpoch,
  DestinationEpochSchema,
  type ResolvedTimePlan,
  type Routine,
  type RoutineRevision,
  RoutineRevisionSchema,
  RoutineSchema,
  resolveSemanticTime,
} from "../../src/modules/coordination/index.js";

export const IDS = {
  household: "11111111-1111-4111-8111-111111111111",
  actor: "22222222-2222-4222-8222-222222222222",
  holder: "33333333-3333-4333-8333-333333333333",
  other: "44444444-4444-4444-8444-444444444444",
  conversation: "55555555-5555-4555-8555-555555555555",
  epoch: "66666666-6666-4666-8666-666666666666",
  nextEpoch: "77777777-7777-4777-8777-777777777777",
  routine: "88888888-8888-4888-8888-888888888888",
  occurrence: "99999999-9999-4999-8999-999999999999",
  loop: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  transition1: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  transition2: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  transition3: "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
  transition4: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
  timer: "ffffffff-ffff-4fff-8fff-ffffffffffff",
  notification1: "10101010-1010-4010-8010-101010101010",
  notification2: "20202020-2020-4020-8020-202020202020",
} as const;

export const destination: DestinationEpoch = DestinationEpochSchema.parse({
  conversationId: IDS.conversation,
  participantEpochId: IDS.epoch,
  participantSetDigest: "a".repeat(64),
  audience: "group",
});

export function timing(localDate = "2026-08-06"): ResolvedTimePlan {
  return resolveSemanticTime(
    {
      timeZone: "America/Los_Angeles",
      event: { kind: "local_clock", time: "15:00" },
      deadline: null,
      preparationMinutes: 30,
      travelMinutes: 15,
      earliestUseful: { kind: "relative", anchor: "event", offsetMinutes: -180 },
      lastResponsible: { kind: "relative", anchor: "event", offsetMinutes: -30 },
    },
    localDate,
  );
}

export function routine(): Routine {
  return RoutineSchema.parse({
    routineId: IDS.routine,
    householdId: IDS.household,
    version: 1,
    currentRevision: 1,
    status: "active",
  });
}

export function routineRevision(overrides: Partial<RoutineRevision> = {}): RoutineRevision {
  return RoutineRevisionSchema.parse({
    routineId: IDS.routine,
    revision: 1,
    title: "Wednesday pickup",
    minimumSharedMeaning: "Wednesday pickup",
    recurrence: {
      kind: "weekly",
      weekdays: [3],
      intervalWeeks: 1,
      startsOn: "2026-08-05",
      endsOn: null,
      excludedDates: [],
    },
    timePlan: {
      timeZone: "America/Los_Angeles",
      event: { kind: "local_clock", time: "15:00", dayOffset: 0 },
      deadline: null,
      preparationMinutes: 30,
      travelMinutes: 15,
      earliestUseful: { kind: "relative", anchor: "event", offsetMinutes: -180 },
      lastResponsible: { kind: "relative", anchor: "event", offsetMinutes: -30 },
    },
    notificationMode: "exceptions_only",
    destination,
    proposedHolderPersonId: IDS.holder,
    standingCoverage: null,
    sourceRevisionRefs: ["source-revision:pickup"],
    effectiveFrom: "2026-08-05",
    effectiveThrough: null,
    createdAt: "2026-08-05T16:00:00Z",
    createdByPersonId: IDS.actor,
    ...overrides,
  });
}
