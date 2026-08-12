import { randomBytes, randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  type EncryptedImageAssetRecord,
  type EncryptedImageStore,
  EncryptedImageVault,
  ImageVaultError,
} from "./index.js";

const householdId = "11111111-1111-4111-8111-111111111111";
const signalId = "22222222-2222-4222-8222-222222222222";
const otherHouseholdId = "33333333-3333-4333-8333-333333333333";
const jpeg = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46]);

afterEach(() => {
  vi.useRealTimers();
});

class MemoryImageStore implements EncryptedImageStore {
  readonly records = new Map<string, EncryptedImageAssetRecord>();

  async readImageAsset(assetId: string): Promise<EncryptedImageAssetRecord | null> {
    const record = this.records.get(assetId);
    return record ? cloneRecord(record) : null;
  }

  async insertImageAsset(record: EncryptedImageAssetRecord): Promise<boolean> {
    if (this.records.has(record.assetId)) return false;
    this.records.set(record.assetId, cloneRecord(record));
    return true;
  }

  async deleteImageAsset(assetId: string): Promise<void> {
    this.records.delete(assetId);
  }

  async deleteExpiredImageAssets(now: Date): Promise<number> {
    let deleted = 0;
    for (const [assetId, record] of this.records) {
      if (Date.parse(record.expiresAt) > now.getTime()) continue;
      this.records.delete(assetId);
      deleted += 1;
    }
    return deleted;
  }
}

function cloneRecord(record: EncryptedImageAssetRecord): EncryptedImageAssetRecord {
  return { ...record, envelope: Uint8Array.from(record.envelope) };
}

describe("EncryptedImageVault", () => {
  it("encrypts bytes and authorizes reads and deletion by the exact source tuple", async () => {
    const store = new MemoryImageStore();
    const encryptionKey = randomBytes(32);
    const apiVault = new EncryptedImageVault({ store, encryptionKey });
    const workerVault = new EncryptedImageVault({ store, encryptionKey });
    const stored = await apiVault.store({
      householdId,
      signalId,
      declaredMimeType: "image/jpeg",
      bytes: jpeg,
    });

    const envelope = store.records.get(stored.image.assetId)?.envelope;
    if (!envelope) throw new Error("Expected an encrypted image record");
    expect(Buffer.from(envelope).includes(jpeg)).toBe(false);
    expect(Buffer.from(envelope).includes(Buffer.from(householdId))).toBe(false);
    await expect(
      workerVault.read({ householdId: otherHouseholdId, signalId, image: stored.image }),
    ).rejects.toMatchObject({ code: "unauthorized_or_missing" });
    await expect(
      workerVault.read({ householdId, signalId: randomUUID(), image: stored.image }),
    ).rejects.toMatchObject({ code: "unauthorized_or_missing" });
    await expect(
      workerVault.read({ householdId, signalId, image: { ...stored.image, mimeType: "image/png" } }),
    ).rejects.toMatchObject({ code: "unauthorized_or_missing" });

    const loaded = await workerVault.read({ householdId, signalId, image: stored.image });
    expect(loaded.mimeType).toBe("image/jpeg");
    expect(Buffer.from(loaded.bytes)).toEqual(jpeg);
    await workerVault.delete({ householdId, signalId, image: stored.image });
    await expect(apiVault.read({ householdId, signalId, image: stored.image })).rejects.toMatchObject({
      code: "unauthorized_or_missing",
    });
  });

  it("rejects unsupported and spoofed images and enforces retention", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-12T12:00:00.000Z"));
    const store = new MemoryImageStore();
    const heic = Buffer.from([0, 0, 0, 16, 0x66, 0x74, 0x79, 0x70, 0x68, 0x65, 0x69, 0x63, 0, 0, 0, 0]);
    const vault = new EncryptedImageVault({
      store,
      encryptionKey: randomBytes(32),
      retentionMs: 1_000,
    });
    await expect(
      vault.store({ householdId, signalId, declaredMimeType: "image/heic", bytes: heic }),
    ).rejects.toMatchObject({ code: "unsupported_image" });
    await expect(
      vault.store({ householdId, signalId, declaredMimeType: "image/png", bytes: jpeg }),
    ).rejects.toBeInstanceOf(ImageVaultError);

    vi.setSystemTime(new Date("2026-08-12T12:00:02.000Z"));
    const second = await vault.store({
      householdId,
      signalId,
      declaredMimeType: "image/jpeg",
      bytes: jpeg,
    });
    vi.setSystemTime(new Date("2026-08-12T12:00:04.000Z"));
    expect(await vault.purgeExpired()).toBe(1);
    await expect(vault.read({ householdId, signalId, image: second.image })).rejects.toMatchObject({
      code: "unauthorized_or_missing",
    });
  });

  it("makes deterministic ingress retries stable and rejects asset ID reuse", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-12T12:00:00.000Z"));
    const store = new MemoryImageStore();
    const vault = new EncryptedImageVault({ store, encryptionKey: randomBytes(32) });
    const assetId = "44444444-4444-4444-8444-444444444444";
    const input = {
      assetId,
      householdId,
      signalId,
      declaredMimeType: "image/jpeg" as const,
      bytes: jpeg,
    };
    const [first, concurrentRetry] = await Promise.all([vault.store(input), vault.store(input)]);
    expect(concurrentRetry).toEqual(first);
    const firstEnvelope = store.records.get(assetId)?.envelope;
    if (!firstEnvelope) throw new Error("Expected an encrypted image record");

    vi.setSystemTime(new Date("2026-08-13T12:00:00.000Z"));
    await expect(vault.store(input)).resolves.toEqual(first);
    expect(store.records.get(assetId)?.envelope).toEqual(firstEnvelope);
    await expect(
      vault.store({ ...input, bytes: Buffer.concat([jpeg, Buffer.from([1])]) }),
    ).rejects.toMatchObject({ code: "asset_conflict", retryable: false });
    await expect(vault.store({ ...input, householdId: otherHouseholdId })).rejects.toMatchObject({
      code: "asset_conflict",
      retryable: false,
    });
  });
});
