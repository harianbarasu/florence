import { createHash } from "node:crypto";
import { decodeImageVaultKey, EncryptedImageVault, ImageVaultError } from "@florence/artifacts";
import {
  FlorenceStoreConflict,
  PostgresFlorenceStore,
  type ProductionResetSnapshot,
} from "@florence/database";
import { GoogleConnection, GoogleProductionResetError } from "@florence/google";
import {
  ProductionRuntimeError,
  requireProductionResetMaintenanceRuntime,
  requireProductionResetVaultDirectory,
} from "./production-runtime.js";

const productionConfirmation = "RESET FLORENCE PRODUCTION";

class ResetCliError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "ResetCliError";
  }
}

let phase:
  | "configuration"
  | "snapshot"
  | "artifact_preflight"
  | "calendar_preflight"
  | "calendar_recovery"
  | "calendar_deletion"
  | "artifact_deletion"
  | "credential_preparation"
  | "credential_revocation"
  | "database_truncation" = "configuration";

try {
  await resetProduction();
} catch (error) {
  process.stderr.write(
    `${JSON.stringify({
      ok: false,
      event: "production_reset_failed",
      phase,
      errorCode: productionResetErrorCode(error),
    })}\n`,
  );
  process.exitCode = 1;
}

async function resetProduction(): Promise<void> {
  if (process.env.NODE_ENV !== "production") throw new ResetCliError("node_environment_not_production");
  const command = resetCommand(process.argv.slice(2));
  await requireProductionResetMaintenanceRuntime();
  const imageVault = productionImageVault(await requireProductionResetVaultDirectory());
  const store = new PostgresFlorenceStore({
    connectionString: requiredEnvironment("FLORENCE_DATABASE_URL"),
    maxConnections: 1,
  });
  try {
    await store.ready();
    phase = "snapshot";
    const snapshot = await store.readProductionResetSnapshot();
    phase = "artifact_preflight";
    const imageVaultSnapshot = await imageVault.inspectForProductionReset();
    const snapshotGuard = productionResetGuard(snapshot.guard, imageVaultSnapshot.guard);
    if (command.mode === "execute" && command.snapshotGuard !== snapshotGuard) {
      throw new ResetCliError("snapshot_guard_mismatch");
    }
    const ambiguousCalendarCreates = snapshot.calendars.filter(
      (calendar) => calendar.calendarId === null,
    ).length;
    const missingCreatorCredentials = snapshot.calendars.filter(
      (calendar) => calendar.creator === null,
    ).length;
    if (missingCreatorCredentials > 0) {
      writeResetResult({
        ok: false,
        event: "production_reset_blocked",
        mode: command.mode,
        snapshotGuard,
        households: snapshot.householdCount,
        calendarTargets: snapshot.calendars.length,
        ambiguousCalendarCreates,
        activeGoogleCredentials: snapshot.activeGoogleCredentials.length,
        encryptedImageArtifacts: imageVaultSnapshot.encryptedImageArtifacts,
        encryptedImageTemporaryArtifacts: imageVaultSnapshot.encryptedImageTemporaryArtifacts,
        missingCreatorCredentials,
      });
      process.exitCode = 1;
      return;
    }

    const google =
      snapshot.calendars.length > 0 ||
      (command.mode === "execute" && snapshot.activeGoogleCredentials.length > 0)
        ? productionGoogleConnection(store)
        : null;
    phase = "calendar_preflight";
    let presentCalendars = 0;
    let absentCalendars = 0;
    const recoveredCalendarIds: { householdId: string; calendarId: string }[] = [];
    for (const calendar of snapshot.calendars) {
      const creator = requiredCreator(calendar.creator);
      const result = await requiredGoogle(google).reconcileFamilyCalendarForProductionReset({
        householdId: calendar.householdId,
        creatorAdultId: creator.adultId,
        creatorConnectionId: creator.connectionId,
        calendarId: calendar.calendarId,
        mode: "inspect",
      });
      if (result.state === "present") presentCalendars += 1;
      else absentCalendars += 1;
      if (calendar.calendarId === null && result.calendarId !== null) {
        recoveredCalendarIds.push({
          householdId: calendar.householdId,
          calendarId: result.calendarId,
        });
      }
    }
    if (command.mode === "dry-run") {
      writeResetResult({
        ok: true,
        event: "production_reset_dry_run_complete",
        mode: "dry-run",
        snapshotGuard,
        households: snapshot.householdCount,
        calendarTargets: snapshot.calendars.length,
        ambiguousCalendarCreates,
        calendarsPresent: presentCalendars,
        calendarsAlreadyAbsent: absentCalendars,
        activeGoogleCredentials: snapshot.activeGoogleCredentials.length,
        encryptedImageArtifacts: imageVaultSnapshot.encryptedImageArtifacts,
        encryptedImageTemporaryArtifacts: imageVaultSnapshot.encryptedImageTemporaryArtifacts,
      });
      return;
    }

    let resetSnapshot = snapshot;
    if (recoveredCalendarIds.length > 0) {
      phase = "calendar_recovery";
      resetSnapshot = await store.recoverProductionResetCalendarIds(snapshot, recoveredCalendarIds);
    }
    phase = "calendar_deletion";
    for (const calendar of resetSnapshot.calendars) {
      const creator = requiredCreator(calendar.creator);
      const result = await requiredGoogle(google).reconcileFamilyCalendarForProductionReset({
        householdId: calendar.householdId,
        creatorAdultId: creator.adultId,
        creatorConnectionId: creator.connectionId,
        calendarId: calendar.calendarId,
        mode: "delete",
      });
      if (result.state !== "absent") throw new ResetCliError("calendar_deletion_unconfirmed");
    }

    phase = "artifact_deletion";
    const deletedImageArtifacts = await imageVault.purgeForProductionReset(imageVaultSnapshot);

    phase = "credential_preparation";
    const revokeGoogleCredentials =
      resetSnapshot.activeGoogleCredentials.length === 0
        ? async () => Object.freeze({ confirmed: 0, unconfirmed: 0 })
        : await requiredGoogle(google).prepareGoogleCredentialRevocationsForProductionReset(
            resetSnapshot.activeGoogleCredentials,
          );

    phase = "database_truncation";
    const deleted = await store.truncateProductionHouseholdData(resetSnapshot);
    phase = "credential_revocation";
    const revocations = await revokeGoogleCredentials();
    writeResetResult({
      ok: true,
      event: "production_reset_complete",
      mode: "execute",
      householdsDeleted: deleted.deletedHouseholds,
      calendarsConfirmedAbsent: deleted.deletedCalendars,
      activeGoogleCredentialsDeleted: deleted.deletedActiveGoogleCredentials,
      encryptedImageArtifactsDeleted: deletedImageArtifacts.encryptedImageArtifactsDeleted,
      encryptedImageTemporaryArtifactsDeleted: deletedImageArtifacts.encryptedImageTemporaryArtifactsDeleted,
      unconfirmedProviderRevocations: revocations.unconfirmed,
      migrationHistoryRetained: true,
    });
  } finally {
    await store.close();
  }
}

function productionImageVault(rootDirectory: string): EncryptedImageVault {
  return new EncryptedImageVault({
    rootDirectory,
    encryptionKey: decodeImageVaultKey(requiredEnvironment("FLORENCE_IMAGE_VAULT_KEY")),
  });
}

function productionResetGuard(databaseGuard: string, imageVaultGuard: string): string {
  return createHash("sha256")
    .update("florence-production-reset-v2\0")
    .update(databaseGuard)
    .update("\0")
    .update(imageVaultGuard)
    .digest("hex");
}

type ResetCommand = Readonly<{ mode: "dry-run" }> | Readonly<{ mode: "execute"; snapshotGuard: string }>;

function resetCommand(rawArguments: readonly string[]): ResetCommand {
  const arguments_ = rawArguments[0] === "--" ? rawArguments.slice(1) : rawArguments;
  if (arguments_.length === 1 && arguments_[0] === "--dry-run") {
    return Object.freeze({ mode: "dry-run" });
  }
  if (
    arguments_.length === 5 &&
    arguments_[0] === "--confirm-production-reset" &&
    arguments_[1] === productionConfirmation &&
    arguments_[2] === "--snapshot" &&
    arguments_[4] === "--api-stopped"
  ) {
    const snapshotGuard = arguments_[3];
    if (snapshotGuard && /^[0-9a-f]{64}$/u.test(snapshotGuard)) {
      return Object.freeze({ mode: "execute", snapshotGuard });
    }
  }
  throw new ResetCliError("invalid_confirmation_or_usage");
}

function productionGoogleConnection(store: PostgresFlorenceStore): GoogleConnection {
  const encodedKey = requiredEnvironment("GOOGLE_CREDENTIAL_KEY");
  const encryptionKey = Buffer.from(encodedKey, "base64");
  if (encryptionKey.byteLength !== 32 || encryptionKey.toString("base64") !== encodedKey) {
    throw new ResetCliError("google_credential_key_invalid");
  }
  return new GoogleConnection({
    store,
    clientId: requiredEnvironment("GOOGLE_OAUTH_CLIENT_ID"),
    clientSecret: requiredEnvironment("GOOGLE_OAUTH_CLIENT_SECRET"),
    redirectUri: requiredEnvironment("GOOGLE_OAUTH_REDIRECT_URI"),
    encryptionKey,
  });
}

function requiredCreator(
  creator: ProductionResetSnapshot["calendars"][number]["creator"],
): NonNullable<ProductionResetSnapshot["calendars"][number]["creator"]> {
  if (!creator) throw new ResetCliError("missing_creator_credential");
  return creator;
}

function requiredGoogle(google: GoogleConnection | null): GoogleConnection {
  if (!google) throw new ResetCliError("google_reset_not_configured");
  return google;
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new ResetCliError(`${name.toLowerCase()}_missing`);
  return value;
}

function writeResetResult(value: Readonly<Record<string, unknown>>): void {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function productionResetErrorCode(error: unknown): string {
  if (error instanceof ImageVaultError) return `image_vault_${error.code}`;
  if (error instanceof GoogleProductionResetError) return error.code;
  if (error instanceof FlorenceStoreConflict) return "database_snapshot_changed_or_invalid";
  if (error instanceof ProductionRuntimeError) return error.code;
  if (error instanceof ResetCliError) return error.code;
  return "production_reset_failed";
}
