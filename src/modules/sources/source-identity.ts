import { canonicalDigest } from "../../shared/canonical-json.js";
import type { SourceOrigin, SourceScope } from "./contracts.js";

/** Canonical database identity for one app-owned normalized source object. */
export function sourceExternalObjectId(input: {
  readonly integrationId: string | null;
  readonly scope: SourceScope;
  readonly artifactKind: string;
  readonly origin: Pick<SourceOrigin, "system" | "remoteObjectId">;
}): string {
  return `${input.artifactKind}:${canonicalDigest({
    integrationId: input.integrationId,
    scope: input.scope,
    artifactKind: input.artifactKind,
    system: input.origin.system,
    remoteObjectId: input.origin.remoteObjectId,
  })}`;
}
