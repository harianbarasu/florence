import { randomUUID } from "node:crypto";

export type Brand<T, Name extends string> = T & { readonly __brand: Name };

export type PersonId = Brand<string, "PersonId">;
export type HouseholdId = Brand<string, "HouseholdId">;
export type ConversationId = Brand<string, "ConversationId">;
export type ParticipantEpochId = Brand<string, "ParticipantEpochId">;
export type SourceRevisionId = Brand<string, "SourceRevisionId">;
export type CoverageLoopId = Brand<string, "CoverageLoopId">;
export type JobId = Brand<string, "JobId">;

export function newId<T extends string>(): Brand<string, T> {
  return randomUUID() as Brand<string, T>;
}
