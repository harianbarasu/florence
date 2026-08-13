# Florence image vault

`EncryptedImageVault` is the app-owned storage path for inbound conversation images. It generates the
asset ID, validates bytes rather than trusting the declared MIME type, rejects unsupported HEIC, encrypts
metadata and bytes together, and releases plaintext only when
`householdId + signalId + assetId + MIME type` all match. Its injected `EncryptedImageStore` keeps the
encrypted records durable and shared between the API and worker processes; production uses the
PostgreSQL repository and tests use an in-memory adapter.

`insertImageAsset` must atomically insert only when `assetId` is absent, returning `true` for the
winning insert and `false` for an existing ID. That single invariant makes concurrent provider retries
idempotent without exposing encryption details to the storage adapter.

Production configuration:

- `FLORENCE_IMAGE_VAULT_KEY`: exactly 32 random bytes encoded as canonical base64. Keep it in the
  deployment secret store, never in the repository or database. `decodeImageVaultKey` validates it.
- `FLORENCE_IMAGE_RETENTION_DAYS`: optional, 1–365; the application default should remain 30 days.

Call `store` only after authenticating and authorizing the inbound signal's household. Webhook ingress
should pass an `assetId` deterministically derived from the provider event and attachment; an identical
retry returns the first reference and expiry, while changed bytes or authority fail with `asset_conflict`.
Dashboard uploads may omit `assetId` to generate one. Include the returned reference in that exact
signal. Pass the vault directly as the runtime's `ImageAssetReader`.
Schedule `purgeExpired` at least daily, and call `delete` for an authorized user deletion. HEIC ingress
fails explicitly instead of retaining undecodable bytes.
