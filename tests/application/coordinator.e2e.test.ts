import { describe, expect, it } from "vitest";
import { type ApplicationOutboxIntent, createFlorenceApplication } from "../../src/application/index.js";
import type { WorkerJob, WorkerResultPayload } from "../../src/runtime/index.js";
import {
  ADULT_A,
  ADULT_B,
  classificationBase,
  directMessage,
  GROUP_CHANNEL,
  groupMessage,
  HOUSEHOLD_ID,
  setup,
} from "./fixtures.js";

function domainEffects(intents: readonly ApplicationOutboxIntent[]) {
  return intents.flatMap((intent) => (intent.kind === "domain.effect" ? [intent.effect] : []));
}

describe("Florence application coordinator", () => {
  it("keeps Gmail private and promotes only approved minimum household meaning", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const privateText = "PRIVATE BODY: the access code is 9917 and must remain in Alex's mailbox.";
    const gmailKey = "gmail-private-1";
    harness.interpreter.respondToGmail(gmailKey, {
      decision: "propose_family_episode",
      confidence: 0.97,
      sourceClass: "school.notice",
      sensitivity: "sensitive",
      familyImpact: true,
      rationale: "A school form has a current household consequence.",
      privateSummary: `Private review: ${privateText}`,
      minimumHouseholdMeaning: "A field-trip form is due Friday.",
      title: "Field-trip form details",
      requiredOutcome: "The form is returned with its private access details.",
      proposedOwnerAdultId: ADULT_A,
    });

    await app.process({
      kind: "gmail_message",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: gmailKey,
      occurredAt: "2027-01-01T08:00:00Z",
      ownerAdultId: ADULT_A,
      accountRef: "gmail_alex_personal",
      messageRef: "gmail_message_school_1",
      revision: 1,
      labels: ["INBOX"],
      sender: "School office",
      subject: "Field trip form and private access code",
      snippet: "A form is due Friday.",
      bodyText: privateText,
      attachmentRefs: ["attachment_permission_slip"],
    });

    const beforeApproval = await harness.repository.load(HOUSEHOLD_ID);
    expect(beforeApproval?.aggregate.episodes).toEqual([]);
    expect(beforeApproval?.projection.pendingPromotions).toHaveLength(1);
    expect(beforeApproval?.projection.gmailTriage[0]).not.toHaveProperty("privateSummary");
    const initialHouseholdMessages = harness.repository
      .intents("conversation.send")
      .filter((intent) => intent.kind === "conversation.send" && intent.targetScope.kind === "household");
    expect(initialHouseholdMessages).toEqual([]);

    const promotionId = beforeApproval?.projection.pendingPromotions[0]?.promotionId;
    expect(promotionId).toBeTruthy();
    const approvalKey = "dm-approve-private-1";
    harness.interpreter.respondToConversation(approvalKey, {
      ...classificationBase,
      intent: "approve_promotion",
      promotionId,
    });
    await app.process(
      directMessage(
        approvalKey,
        `Share the minimum meaning for ${promotionId}`,
        ADULT_A,
        "2027-01-01T08:05:00Z",
      ),
    );

    const afterApproval = await harness.repository.load(HOUSEHOLD_ID);
    expect(afterApproval?.projection.pendingPromotions).toEqual([]);
    expect(afterApproval?.aggregate.episodes).toHaveLength(1);
    expect(afterApproval?.aggregate.episodes[0]).toMatchObject({
      scope: { kind: "household" },
      title: "A field-trip form is due Friday.",
      requiredOutcome: "A field-trip form is due Friday.",
      promotionAuthority: { kind: "approval" },
    });
    expect(JSON.stringify(afterApproval)).not.toContain(privateText);

    const householdBodies = harness.repository
      .intents("conversation.send")
      .flatMap((intent) =>
        intent.kind === "conversation.send" && intent.targetScope.kind === "household" ? [intent.body] : [],
      );
    expect(householdBodies).toEqual(["A field-trip form is due Friday."]);
    expect(householdBodies.join(" ")).not.toContain("9917");
    expect(householdBodies.join(" ")).not.toContain("access code");
  });

  it("learns and revokes an explicitly approved minimum-meaning sharing rule", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const gmail = async (key: string, meaning: string, minute: number) => {
      harness.interpreter.respondToGmail(key, {
        decision: "propose_family_episode",
        confidence: 0.97,
        sourceClass: "school.notice",
        sensitivity: "ordinary",
        familyImpact: true,
        rationale: "A school notice requires household coordination.",
        privateSummary: "A school notice needs coordination.",
        minimumHouseholdMeaning: meaning,
        title: meaning,
        requiredOutcome: meaning,
      });
      await app.process({
        kind: "gmail_message",
        householdId: HOUSEHOLD_ID,
        idempotencyKey: key,
        occurredAt: `2027-01-01T08:${String(minute).padStart(2, "0")}:00Z`,
        ownerAdultId: ADULT_A,
        accountRef: "gmail_alex_personal",
        messageRef: `gmail_${key}`,
        revision: 1,
        labels: ["INBOX"],
        sender: "School office",
        subject: "School notice",
        bodyText: "A school notice has a household consequence.",
        attachmentRefs: [],
      });
    };

    await gmail("sharing-rule-first", "School closes early Friday.", 0);
    const promotionId = (await harness.repository.load(HOUSEHOLD_ID))?.projection.pendingPromotions[0]
      ?.promotionId;
    expect(promotionId).toBeTruthy();
    const approveKey = "sharing-rule-approve";
    harness.interpreter.respondToConversation(approveKey, {
      ...classificationBase,
      intent: "approve_promotion",
      promotionId,
      rememberForMatchingSource: true,
    });
    await app.process(
      directMessage(approveKey, `Always share ${promotionId}`, ADULT_A, "2027-01-01T08:01:00Z"),
    );

    let snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.policies).toEqual([
      expect.objectContaining({
        status: "active",
        version: 1,
        rule: expect.objectContaining({
          kind: "sharing",
          sourceClass: "school.notice",
          maximumSensitivity: "ordinary",
        }),
      }),
    ]);
    await gmail("sharing-rule-second", "School starts late Monday.", 2);
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.pendingPromotions).toEqual([]);
    expect(snapshot?.aggregate.episodes).toHaveLength(2);

    const policy = snapshot?.aggregate.policies[0];
    expect(policy).toBeDefined();
    if (policy === undefined) throw new Error("Expected an active sharing policy");
    const revokeKey = "sharing-rule-revoke";
    harness.interpreter.respondToConversation(revokeKey, {
      ...classificationBase,
      intent: "revoke_policy",
      policyId: policy.policyId,
      expectedPolicyVersion: 1,
    });
    await app.process(
      directMessage(revokeKey, "Stop always sharing school notices", ADULT_A, "2027-01-01T08:03:00Z"),
    );
    expect((await harness.repository.load(HOUSEHOLD_ID))?.aggregate.policies[0]?.status).toBe("revoked");

    await gmail("sharing-rule-third", "School dismisses at noon Wednesday.", 4);
    expect((await harness.repository.load(HOUSEHOLD_ID))?.projection.pendingPromotions).toHaveLength(1);
  });

  it("requires private invite consent before activating one shared group", async () => {
    const harness = setup({ onboarding: "new" });
    const app = createFlorenceApplication(harness.dependencies);
    const steps = [
      {
        key: "onboard-consent-a",
        input: directMessage("onboard-consent-a", "I consent", ADULT_A, "2027-01-01T08:00:00Z"),
        response: { ...classificationBase, intent: "onboarding", action: "consent" },
        phase: "awaiting_invitation",
      },
      {
        key: "onboard-invite-b",
        input: directMessage("onboard-invite-b", "Invite Bailey", ADULT_A, "2027-01-01T08:01:00Z"),
        response: {
          ...classificationBase,
          intent: "onboarding",
          action: "invite_adult",
          invitedAdultId: ADULT_B,
        },
        phase: "awaiting_invitee_consent",
      },
      {
        key: "onboard-consent-b",
        input: directMessage("onboard-consent-b", "I accept", ADULT_B, "2027-01-01T08:02:00Z"),
        response: { ...classificationBase, intent: "onboarding", action: "accept_invite" },
        phase: "awaiting_group",
      },
      {
        key: "onboard-group",
        input: groupMessage("onboard-group", "This is our household group", "2027-01-01T08:03:00Z"),
        response: { ...classificationBase, intent: "onboarding", action: "register_group" },
        phase: "building_profile",
      },
      {
        key: "onboard-profile",
        input: groupMessage(
          "onboard-profile",
          "Maya goes to Lakeside School, has soccer Tuesdays, and is allergic to peanuts.",
          "2027-01-01T08:03:30Z",
        ),
        response: {
          ...classificationBase,
          intent: "onboarding",
          action: "update_profile",
          profileFacts: [
            { category: "dependent", subject: "Maya", detail: "Maya is a child in the household." },
            {
              category: "school_childcare",
              subject: "Maya",
              detail: "Maya attends Lakeside School.",
            },
            {
              category: "recurring_activity",
              subject: "Maya soccer",
              detail: "Soccer is on Tuesdays.",
            },
            {
              category: "dietary_constraint",
              subject: "Maya",
              detail: "Maya has a peanut allergy.",
            },
          ],
        },
        phase: "building_profile",
      },
      {
        key: "onboard-confirm-a",
        input: groupMessage("onboard-confirm-a", "Profile looks right", "2027-01-01T08:04:00Z"),
        response: { ...classificationBase, intent: "onboarding", action: "confirm_profile" },
        phase: "building_profile",
      },
      {
        key: "onboard-confirm-b",
        input: {
          ...groupMessage("onboard-confirm-b", "I confirm too", "2027-01-01T08:05:00Z"),
          senderAdultId: ADULT_B,
        },
        response: { ...classificationBase, intent: "onboarding", action: "confirm_profile" },
        phase: "active",
      },
    ] as const;

    for (const step of steps) {
      harness.interpreter.respondToConversation(step.key, step.response);
      await app.process(step.input);
      expect((await harness.repository.load(HOUSEHOLD_ID))?.projection.onboarding.phase).toBe(step.phase);
      if (step.key === "onboard-invite-b") {
        const contactedBeforeInbound = harness.repository
          .intents("conversation.send")
          .some(
            (intent) =>
              intent.kind === "conversation.send" &&
              intent.targetScope.kind === "personal" &&
              intent.targetScope.adultId === ADULT_B,
          );
        expect(contactedBeforeInbound).toBe(false);
      }
    }

    const snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.onboarding).toMatchObject({
      phase: "active",
      invitedAdultId: ADULT_B,
      consentedAdultIds: [ADULT_A, ADULT_B],
      privateDmAdultIds: [ADULT_A, ADULT_B],
      groupChannelId: GROUP_CHANNEL,
      profileConfirmedAdultIds: [ADULT_A, ADULT_B],
    });
    expect(snapshot?.aggregate.version).toBe(0);
    expect(snapshot?.aggregate.lastProcessedSequence).toBe(0);
    expect(snapshot?.projection.sharedProfile.facts).toHaveLength(4);
    expect(snapshot?.projection.sharedProfile.facts[1]).toMatchObject({
      sourceRef: "message_onboard-profile",
      recordedByAdultId: ADULT_A,
    });
  });

  it("keeps profile updates in the group and invalidates stale confirmations", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const privateKey = "private-profile-update";
    harness.interpreter.respondToConversation(privateKey, {
      ...classificationBase,
      intent: "onboarding",
      action: "update_profile",
      profileFacts: [
        {
          category: "dietary_constraint",
          subject: "Private detail",
          detail: "This must not become household data.",
        },
      ],
    });
    await app.process(
      directMessage(privateKey, "Add this private detail to the profile", ADULT_A, "2027-01-02T08:00:00Z"),
    );
    expect((await harness.repository.load(HOUSEHOLD_ID))?.projection.sharedProfile.facts).toEqual([]);

    const groupKey = "shared-profile-update";
    harness.interpreter.respondToConversation(groupKey, {
      ...classificationBase,
      intent: "onboarding",
      action: "update_profile",
      profileFacts: [
        {
          category: "routine_anchor",
          subject: "School pickup",
          detail: "School pickup is normally at 3:15 PM on weekdays.",
        },
      ],
    });
    await app.process(
      groupMessage(groupKey, "School pickup is normally 3:15 on weekdays", "2027-01-02T08:01:00Z"),
    );
    const snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.onboarding).toMatchObject({
      phase: "active",
      profileConfirmedAdultIds: [],
    });
    expect(snapshot?.projection.sharedProfile.facts).toEqual([
      expect.objectContaining({
        category: "routine_anchor",
        subject: "School pickup",
        sourceRef: "message_shared-profile-update",
        recordedByAdultId: ADULT_A,
      }),
    ]);

    const correctionKey = "shared-profile-correction";
    harness.interpreter.respondToConversation(correctionKey, {
      ...classificationBase,
      intent: "onboarding",
      action: "update_profile",
      profileFacts: [
        {
          category: "routine_anchor",
          subject: "School pickup",
          detail: "School pickup is normally at 3:30 PM on weekdays.",
        },
      ],
    });
    await app.process(
      groupMessage(correctionKey, "Correction: school pickup is 3:30", "2027-01-02T08:02:00Z"),
    );
    expect((await harness.repository.load(HOUSEHOLD_ID))?.projection.sharedProfile.facts).toEqual([
      expect.objectContaining({
        subject: "School pickup",
        detail: "School pickup is normally at 3:30 PM on weekdays.",
        sourceRef: "message_shared-profile-correction",
      }),
    ]);
  });

  it("drives a group obligation through owner acknowledgement, reminder, and closure", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const proposalKey = "group-obligation";
    harness.interpreter.respondToConversation(proposalKey, {
      ...classificationBase,
      intent: "propose_commitment",
      title: "Return the field-trip form",
      requiredOutcome: "The signed field-trip form is returned",
      proposedOwnerAdultId: ADULT_A,
      sourceClass: "school.form",
      sensitivity: "ordinary",
      temporalPlan: {
        planId: "plan_field_trip",
        version: 1,
        timeZone: "America/Los_Angeles",
        deadline: { kind: "instant", at: "2027-01-03T17:00:00Z" },
        usefulLeadMinutes: 1_440,
        preparationMinutes: 30,
        finalBufferMinutes: 30,
        triggers: [
          {
            triggerId: "trigger_field_trip_first",
            timerId: "timer_field_trip_first",
            kind: "reminder",
            at: { kind: "instant", at: "2027-01-02T17:00:00Z" },
          },
          {
            triggerId: "trigger_field_trip_final",
            timerId: "timer_field_trip_final",
            kind: "reminder",
            at: { kind: "instant", at: "2027-01-03T16:00:00Z" },
          },
        ],
      },
    });
    await app.process(
      groupMessage(proposalKey, "Alex can return the field-trip form", "2027-01-01T08:00:00Z"),
    );
    let snapshot = await harness.repository.load(HOUSEHOLD_ID);
    const episodeId = snapshot?.aggregate.episodes[0]?.episodeId;
    expect(snapshot?.aggregate.episodes[0]).toMatchObject({
      episodeId,
      state: "awaiting_acknowledgement",
      owner: { status: "proposed", adultId: ADULT_A },
    });
    expect(
      domainEffects(harness.repository.outbox).filter((effect) => effect.kind === "schedule_timer"),
    ).toHaveLength(2);

    const ackKey = "group-obligation-ack";
    harness.interpreter.respondToConversation(ackKey, {
      ...classificationBase,
      intent: "acknowledge_owner",
      episodeId,
      baseEpisodeVersion: 1,
    });
    await app.process(groupMessage(ackKey, "I will handle it", "2027-01-01T08:01:00Z"));
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.episodes[0]?.state).toBe("active");

    const timerResult = await app.process({
      kind: "timer_fired",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "timer-fire-first",
      timerId: "timer_field_trip_first",
      episodeId,
      temporalPlanVersion: 1,
      triggerId: "trigger_field_trip_first",
      firedAt: "2027-01-02T17:00:00Z",
    });
    expect(timerResult.outcome.classification).toBe("timer:accepted");
    const duplicate = await app.process({
      kind: "timer_fired",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "timer-fire-first",
      timerId: "timer_field_trip_first",
      episodeId,
      temporalPlanVersion: 1,
      triggerId: "trigger_field_trip_first",
      firedAt: "2027-01-02T17:00:00Z",
    });
    expect(duplicate.disposition).toBe("duplicate");

    const reminders = domainEffects(harness.repository.outbox).filter(
      (effect) => effect.kind === "send_message" && effect.messageClass === "reminder",
    );
    expect(reminders).toHaveLength(1);
    expect(reminders[0]).toMatchObject({ targetScope: { kind: "household" } });
    if (reminders[0]?.kind === "send_message") {
      expect(reminders[0].body).not.toMatch(/forgot|fault|blame|careless|irresponsible/i);
    }

    const closeKey = "group-obligation-close";
    harness.interpreter.respondToConversation(closeKey, {
      ...classificationBase,
      intent: "close_episode",
      episodeId,
      baseEpisodeVersion: 3,
      outcome: "completed",
      summary: "The signed field-trip form was returned.",
    });
    await app.process(groupMessage(closeKey, "The form is returned", "2027-01-02T18:00:00Z"));
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.episodes[0]?.state).toBe("completed");
    expect(
      domainEffects(harness.repository.outbox).filter((effect) => effect.kind === "cancel_timer"),
    ).toEqual([
      expect.objectContaining({
        timerId: "timer_field_trip_final",
        temporalPlanVersion: 1,
      }),
    ]);
  });

  it("rejects a validly shaped worker result when its household base version is stale", async () => {
    const harness = setup();
    const app = createFlorenceApplication(harness.dependencies);
    const researchKey = "research-stale";
    harness.interpreter.respondToConversation(researchKey, {
      ...classificationBase,
      intent: "research_request",
      title: "Compare summer camps",
      requiredOutcome: "A sourced camp comparison is ready",
      constraints: ["Fit the household calendar"],
      scopeAssessment: {
        decision: "in_scope",
        reason: "The choice affects childcare and the household calendar.",
      },
    });
    await app.process(groupMessage(researchKey, "Compare summer camps for us", "2027-02-01T08:00:00Z"));
    let snapshot = await harness.repository.load(HOUSEHOLD_ID);
    const job = snapshot?.projection.workers[0]?.job;
    expect(job?.baseHouseholdVersion).toBe(snapshot?.aggregate.version);

    const changeKey = "research-stale-change";
    harness.interpreter.respondToConversation(changeKey, {
      ...classificationBase,
      intent: "propose_commitment",
      title: "Confirm Friday pickup",
      requiredOutcome: "Friday pickup coverage is confirmed",
      sourceClass: "household.pickup",
      sensitivity: "ordinary",
    });
    await app.process(groupMessage(changeKey, "We also need Friday pickup coverage", "2027-02-01T08:01:00Z"));
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.aggregate.version).toBe((job?.baseHouseholdVersion ?? 0) + 1);
    if (job === undefined) {
      throw new Error("Expected a queued worker job");
    }
    const resultEvidenceId = "evidence_worker_stale_result";
    const beforeResultEffects = harness.repository.outbox.length;
    const workerResult = {
      jobId: job.jobId,
      attemptId: job.attemptId,
      householdId: job.householdId,
      baseHouseholdVersion: job.baseHouseholdVersion,
      policyVersion: job.policyVersion,
      modelRouteId: job.modelRouteId,
      modelCapabilityProfile: job.modelCapabilityProfile,
      outputContractRef: job.outputContractRef,
      summary: "A camp comparison is ready.",
      evidenceRefs: [resultEvidenceId],
      questions: [],
      warnings: [],
      proposedCommands: [
        {
          kind: "message.propose",
          payload: {
            proposalId: "message_proposal_stale",
            targetScope: { kind: "household" },
            purpose: "status_update",
            body: "A sourced camp comparison is ready for review.",
            evidence: [
              {
                evidenceId: resultEvidenceId,
                source: "worker",
                sourceRef: "worker:camp-comparison",
                scope: { kind: "household" },
                observedAt: "2027-02-01T08:02:00Z",
                revision: 1,
              },
            ],
            sourceClass: "worker.research",
            sensitivity: "ordinary",
          },
        },
      ],
      confidence: 0.94,
      diagnostics: {
        durationMs: 100,
        modelCalls: 1,
        toolCalls: 1,
        usage: {},
        traceReferences: [],
      },
    };
    const reconciled = await app.process({
      kind: "worker_result",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "worker-result-stale",
      receivedAt: "2027-02-01T08:02:00Z",
      result: workerResult,
    });

    expect(reconciled.outcome.status).toBe("rejected");
    expect(reconciled.outcome.domainReceipts[0]).toMatchObject({
      disposition: "rejected",
      reason: "stale_household_version",
    });
    snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.workers[0]?.status).toBe("rejected");
    expect(harness.repository.outbox).toHaveLength(beforeResultEffects);
  });

  it("runs research and meal workers only after in-scope adult requests", async () => {
    const observedAt = new Date(Date.now() + 60_000).toISOString();
    const workerResponse = (job: WorkerJob): WorkerResultPayload => {
      const meal = job.modelCapabilityProfile === "tool_planning";
      const evidenceId = `evidence_${job.jobId}`;
      return {
        summary: meal ? "A meal plan and grocery list are ready." : "A sourced comparison is ready.",
        evidenceRefs: [evidenceId],
        questions: [],
        warnings: [],
        proposedCommands: [
          {
            kind: "message.propose",
            payload: {
              proposalId: meal ? "message_meal_result" : "message_research_result",
              targetScope: { kind: "household" },
              purpose: "status_update",
              body: meal
                ? "Meal plan: pasta, tacos, and soup. Grocery list: produce, tortillas, beans, pasta, and broth."
                : "Research comparison: Camp North and Camp West fit the stated calendar; source checks are current.",
              evidence: [
                {
                  evidenceId,
                  source: "worker",
                  sourceRef: meal ? "worker:meal-plan" : "worker:research-comparison",
                  scope: { kind: "household" },
                  observedAt,
                  revision: 1,
                },
              ],
              sourceClass: meal ? "worker.meal_plan" : "worker.research",
              sensitivity: "ordinary",
            },
          },
        ],
        confidence: 0.93,
      };
    };
    const harness = setup({ workerResponse });
    const app = createFlorenceApplication(harness.dependencies);
    const requestAt = new Date(Date.now() + 30_000).toISOString();

    const researchKey = "research-requested";
    harness.interpreter.respondToConversation(researchKey, {
      ...classificationBase,
      intent: "research_request",
      title: "Compare two summer camps",
      requiredOutcome: "A sourced comparison covers cost, location, policy, and calendar fit",
      constraints: ["Include cancellation policy", "Check household calendar fit"],
      scopeAssessment: {
        decision: "in_scope",
        reason: "The decision affects childcare and the household calendar.",
      },
    });
    await app.process(groupMessage(researchKey, "Compare two summer camps", requestAt));
    const researchIntent = harness.repository
      .intents("worker.run")
      .find((intent) => intent.kind === "worker.run");
    expect(researchIntent).toMatchObject({
      kind: "worker.run",
      job: { modelCapabilityProfile: "long_context_research" },
    });
    if (researchIntent?.kind !== "worker.run") {
      throw new Error("Expected a research worker intent");
    }
    await app.executeOutbox(researchIntent, new Date(Date.now() + 45_000).toISOString());

    const mealKey = "meal-requested";
    harness.interpreter.respondToConversation(mealKey, {
      ...classificationBase,
      intent: "meal_plan_request",
      title: "Plan three weeknight dinners",
      requiredOutcome: "Three practical dinners and one grouped grocery list are ready",
      horizon: "Monday through Wednesday",
      constraints: ["Use the household schedule", "Prefer one leftovers night"],
      scopeAssessment: {
        decision: "in_scope",
        reason: "The plan covers shared household meals and groceries.",
      },
    });
    await app.process(
      groupMessage(
        mealKey,
        "Plan three weeknight dinners and a grocery list",
        new Date(Date.now() + 50_000).toISOString(),
      ),
    );
    const mealIntent = harness.repository
      .intents("worker.run")
      .filter((intent) => intent.kind === "worker.run")
      .at(-1);
    expect(mealIntent).toMatchObject({
      kind: "worker.run",
      job: { modelCapabilityProfile: "tool_planning" },
    });
    if (mealIntent?.kind !== "worker.run") {
      throw new Error("Expected a meal worker intent");
    }
    await app.executeOutbox(mealIntent, new Date(Date.now() + 55_000).toISOString());

    const workerCount = harness.repository.intents("worker.run").length;
    const declinedKey = "research-out-of-scope";
    harness.interpreter.respondToConversation(declinedKey, {
      ...classificationBase,
      intent: "research_request",
      title: "Prepare a work presentation",
      requiredOutcome: "A professional presentation is ready",
      constraints: [],
      scopeAssessment: {
        decision: "out_of_scope",
        reason: "The request is a professional deliverable without a household consequence.",
      },
    });
    const declined = await app.process(
      directMessage(
        declinedKey,
        "Prepare my work presentation",
        ADULT_A,
        new Date(Date.now() + 57_000).toISOString(),
      ),
    );
    expect(declined.outcome.status).toBe("rejected");
    expect(harness.repository.intents("worker.run")).toHaveLength(workerCount);

    await app.process({
      kind: "daily_brief",
      householdId: HOUSEHOLD_ID,
      idempotencyKey: "daily-brief-scheduled",
      occurredAt: new Date(Date.now() + 58_000).toISOString(),
      reason: "scheduled",
    });
    expect(harness.repository.intents("worker.run")).toHaveLength(workerCount);
    expect(harness.workerRuntime.calls).toHaveLength(2);
    const snapshot = await harness.repository.load(HOUSEHOLD_ID);
    expect(snapshot?.projection.workers.map((worker) => worker.status)).toEqual(["reconciled", "reconciled"]);

    const resultBodies = domainEffects(harness.repository.outbox).flatMap((effect) =>
      effect.kind === "send_message" && effect.messageClass === "status" ? [effect.body] : [],
    );
    expect(resultBodies.some((body) => body.includes("Research comparison"))).toBe(true);
    expect(resultBodies.some((body) => body.includes("Grocery list"))).toBe(true);
    expect(
      domainEffects(harness.repository.outbox).some((effect) => effect.kind === "execute_external_action"),
    ).toBe(false);
  });
});
