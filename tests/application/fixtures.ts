import {
  createApplicationProjection,
  createOnboardingProjection,
  FakeApplicationEffectExecutor,
  FakeApplicationInterpreter,
  FakeHouseholdCalendarActions,
  FakeWorkerContext,
  InMemoryApplicationRepository,
  type WorkerRoutes,
} from "../../src/application/index.js";
import {
  AdultIdSchema,
  type HouseholdAggregate,
  HouseholdAggregateSchema,
  HouseholdIdSchema,
} from "../../src/domain/index.js";
import { FakeWorkerRuntime, type WorkerResultPayload } from "../../src/runtime/index.js";

export const HOUSEHOLD_ID = HouseholdIdSchema.parse("household_family");
export const ADULT_A = AdultIdSchema.parse("adult_alex");
export const ADULT_B = AdultIdSchema.parse("adult_bailey");
export const GROUP_CHANNEL = "conversation_household";

export function aggregate(overrides: Partial<HouseholdAggregate> = {}): HouseholdAggregate {
  return HouseholdAggregateSchema.parse({
    schemaVersion: 1,
    householdId: HOUSEHOLD_ID,
    version: 0,
    policyVersion: 0,
    lastProcessedSequence: 0,
    timeZone: "America/Los_Angeles",
    verifiedAdultIds: [ADULT_A, ADULT_B],
    routineAnchors: [],
    episodes: [],
    policies: [],
    policyCandidates: [],
    approvals: [],
    memoryCandidates: [],
    memories: [],
    pendingActions: [],
    ...overrides,
  });
}

export const TEST_WORKER_ROUTES: WorkerRoutes = {
  family_research: {
    modelRouteId: "route.test.research",
    outputContractRef: "contract.test.research",
    capabilityIds: [],
    allowedToolNames: [],
    maxDurationMs: 60_000,
    maxModelCalls: 5,
    maxToolCalls: 0,
    modelCapabilityProfile: "long_context_research",
  },
  meal_plan: {
    modelRouteId: "route.test.meals",
    outputContractRef: "contract.test.meals",
    capabilityIds: [],
    allowedToolNames: [],
    maxDurationMs: 60_000,
    maxModelCalls: 5,
    maxToolCalls: 0,
    modelCapabilityProfile: "tool_planning",
  },
};

export function setup(input?: {
  onboarding?: "active" | "new";
  aggregate?: HouseholdAggregate;
  workerResponse?: WorkerResultPayload | ConstructorParameters<typeof FakeWorkerRuntime>[0];
}) {
  const repository = new InMemoryApplicationRepository();
  const household = input?.aggregate ?? aggregate();
  const onboarding =
    input?.onboarding === "new"
      ? createOnboardingProjection({ initiatorAdultId: ADULT_A })
      : createOnboardingProjection({
          initiatorAdultId: ADULT_A,
          invitedAdultId: ADULT_B,
          groupChannelId: GROUP_CHANNEL,
          phase: "active",
        });
  repository.seed({
    revision: 0,
    aggregate: household,
    projection: createApplicationProjection(onboarding),
  });
  const interpreter = new FakeApplicationInterpreter();
  const workerRuntime = new FakeWorkerRuntime(
    input?.workerResponse ?? {
      summary: "No changes proposed.",
      evidenceRefs: [],
      questions: [],
      warnings: [],
      proposedCommands: [],
      confidence: 1,
    },
  );
  const workerContext = new FakeWorkerContext();
  const effectExecutor = new FakeApplicationEffectExecutor();
  const calendarActions = new FakeHouseholdCalendarActions();
  return {
    repository,
    interpreter,
    workerRuntime,
    workerContext,
    effectExecutor,
    calendarActions,
    dependencies: {
      repository,
      interpreter,
      workerRuntime,
      workerContext,
      effectExecutor,
      calendarActions,
      workerRoutes: TEST_WORKER_ROUTES,
    },
  };
}

export function groupMessage(key: string, text: string, occurredAt: string) {
  return {
    kind: "conversation_message" as const,
    householdId: HOUSEHOLD_ID,
    idempotencyKey: key,
    occurredAt,
    channel: { channelId: GROUP_CHANNEL, scope: "household" as const },
    senderAdultId: ADULT_A,
    messageRef: `message_${key}`,
    text,
    attachmentRefs: [],
    attachmentContents: [],
  };
}

export function directMessage(
  key: string,
  text: string,
  adultId: typeof ADULT_A | typeof ADULT_B,
  occurredAt: string,
) {
  return {
    kind: "conversation_message" as const,
    householdId: HOUSEHOLD_ID,
    idempotencyKey: key,
    occurredAt,
    channel: { channelId: `dm_${adultId}`, scope: "personal" as const, adultId },
    senderAdultId: adultId,
    messageRef: `message_${key}`,
    text,
    attachmentRefs: [],
    attachmentContents: [],
  };
}

export const classificationBase = {
  confidence: 0.98,
  rationale: "The request has clear household meaning.",
} as const;
