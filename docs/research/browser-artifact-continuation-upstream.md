# Browser-acquired artifact continuation: upstream evidence and Florence decision

Research date: 2026-08-28

Source baselines reviewed:

- [`earendil-works/pi-mono`](https://github.com/earendil-works/pi-mono) at commit [`4e494929998d6bc4fccf75e0a233f727db4b70ee`](https://github.com/earendil-works/pi-mono/tree/4e494929998d6bc4fccf75e0a233f727db4b70ee)
- [`NousResearch/hermes-agent`](https://github.com/NousResearch/hermes-agent) at commit [`6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882`](https://github.com/NousResearch/hermes-agent/tree/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882)
- [`Merit-Systems/OpenInstinct`](https://github.com/Merit-Systems/OpenInstinct) at commit [`480045dbc63008e7f99313d1683858cd8657b35a`](https://github.com/Merit-Systems/OpenInstinct/tree/480045dbc63008e7f99313d1683858cd8657b35a)
- Kernel's current official [File I/O documentation](https://www.kernel.sh/docs/browsers/file-io/) and its Node SDK at commit [`40d5209dcba021dc90b37f5ca52ace7ebf1de884`](https://github.com/kernel/kernel-node-sdk/tree/40d5209dcba021dc90b37f5ca52ace7ebf1de884)

Florence baseline reviewed: local repository at `6642cf1de`, especially the current [browser adapter](../../apps/api/src/browser.ts), [durable work loop](../../apps/api/src/florence.ts), [artifact package](../../packages/artifacts/src/index.ts), and [Linq provider](../../packages/linq/src/index.ts).

## Bottom line

Florence needs one general product primitive: **a file acquired by a browser must become a durable Florence artifact before the Kernel browser disappears**. The artifact can then survive a worker restart, remain attached to the originating work, be inspected or transformed, be uploaded in a later browser session, and be returned to a parent as a native iMessage attachment.

No upstream supplies that whole path:

- **Kernel** supplies the real browser filesystem bridge: download into the browser VM, list/read the remote file, write/upload a file back, and pass an in-memory buffer to Playwright's `setInputFiles()`.
- **OpenInstinct** supplies the strongest app-level persistence pattern, but only for browser images: idempotent reservation, pending-to-ready finalization, content hashing, an opaque artifact descriptor, authenticated retrieval, and Linq attachment delivery.
- **Hermes** supplies a clean binary-plane/reference-plane contract and explicit outbound attachment intent, but its browser artifact store is process-local, short-lived, and one-shot. It is transport scaffolding, not durable continuation.
- **Pi** supplies the useful tool-result and continuation shape: small model-visible content, arbitrary application details, persisted tool-result messages, and automatic continuation. It has no browser file store.

The correct Florence implementation is therefore a thin composition of proven patterns, not a new document-management subsystem and not a hardcoded workflow for school forms, receipts, travel, or any other domain. Kernel acquires the bytes; Florence gives them a durable app-owned identity; the existing Florence work loop carries that identity forward; Kernel or another provider can consume it later; the existing Linq provider returns it to the family.

## The product primitive

One artifact should be usable across this sequence:

| Stage | Required behavior | Durable truth |
| --- | --- | --- |
| Acquire | A browser action produces a file or identifies a downloadable resource. Florence reads the bytes before the Kernel session ends. | Kernel path and browser session are temporary acquisition details only. |
| Promote | Florence idempotently reserves an artifact, writes the bytes to app-owned storage, computes/verifies its hash, and finalizes metadata. | App-minted `artifactId`, ownership/audience, work provenance, filename, media type, size, hash, and storage reference. |
| Continue | The tool returns a compact descriptor and the work checkpoint records it before the next model step. | The model sees the descriptor, never the bytes, provider URL, or storage path. |
| Reuse | A later tool resolves the artifact by ID, reopens the bytes, and supplies them to a connector or a new Kernel browser session. | The same ID and hash survive browser/session/process changes. |
| Return | Florence sends the artifact as a native attachment with the natural-language result. | Existing Florence/Linq delivery idempotency and provider receipt remain authoritative. |

This is deliberately broader than “download a file.” The same artifact can be summarized, parsed, transformed, attached to an email, uploaded to a portal, compared with another file, retained as a useful household asset, or sent back to a parent. The agent chooses the continuation from the objective and current state.

## Kernel: the browser filesystem bridge

Kernel's official [File I/O guide](https://www.kernel.sh/docs/browsers/file-io/) is explicit that browser downloads live in the browser VM and are retrievable only while that browser session remains active. A stopped or timed-out session permanently loses those files. Its documented download path is:

1. configure Chrome download behavior and progress events;
2. trigger the download;
3. observe the completed file path;
4. poll the browser filesystem because completion can precede filesystem API visibility;
5. call `kernel.browsers.fs.readFile(...)`; and
6. consume the returned bytes before ending the session.

The official Node SDK exposes the rest of the useful transfer surface in [`src/resources/browsers/fs/fs.ts`](https://github.com/kernel/kernel-node-sdk/blob/40d5209dcba021dc90b37f5ca52ace7ebf1de884/src/resources/browsers/fs/fs.ts): `readFile`, `writeFile`, `upload`, `uploadZip`, `downloadDirZip`, file listing, metadata, move, and delete. Kernel's Playwright execution creates a fresh execution context per call against the same live browser ([source](https://github.com/kernel/kernel-node-sdk/blob/40d5209dcba021dc90b37f5ca52ace7ebf1de884/src/resources/browsers/playwright.ts)); that makes a remote path plus `fs.readFile` a better binary channel than trying to place file bytes in a Playwright JSON result.

For the reverse direction, the same File I/O guide documents Playwright `setInputFiles()` with `{ name, mimeType, buffer }`. Once Florence resolves a durable artifact to bytes, it can upload the file into a later page without first exposing an app storage URL or depending on the original Kernel session.

Kernel's CLI independently demonstrates these are intended transfer primitives, including `browsers fs read-file --output`, directory ZIP download, and local-to-remote upload ([README](https://github.com/kernel/cli/blob/4e47af9d9ee6dfe39569cfa5b6cd775db48821b4/README.md), [command source](https://github.com/kernel/cli/blob/4e47af9d9ee6dfe39569cfa5b6cd775db48821b4/cmd/browsers.go)). Kernel session snapshots can retain VM-local files with browser state ([kernel-images README](https://github.com/onkernel/kernel-images/blob/edaff49f9793197c9e777369f7c154f6626e4e1b/README.md)), but that is browser recovery, not Florence artifact durability. A family-facing result must not depend on a provider VM remaining alive.

**Reuse decision:** call the Kernel SDK directly for acquisition and later upload. Wrap it in Florence's existing browser client and work lifecycle. Do not copy the SDK, build a second remote-filesystem abstraction, or treat a Kernel path/session snapshot as the durable artifact.

## OpenInstinct: the closest persistence and delivery pattern

OpenInstinct implements the end-to-end path for **images**, which makes it the most directly adaptable upstream source:

- [`capture_browser_image.ts`](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/tools/capture_browser_image.ts) captures viewport, full-page, element, or original-resource images. Original resources are fetched through the active Kernel browser network context; screenshots are written to a temporary browser path, read with `kernel.browsers.fs.readFile`, and deleted in `finally`.
- The capture uses an idempotency key derived from worker session and tool call, and returns an existing ready artifact on retry rather than duplicating it.
- [`db/services/browser-images.ts`](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/db/services/browser-images.ts) reserves a pending row, finalizes it to ready, and scopes retrieval.
- The [database schema](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/db/schema/application.ts) records workspace, user, root/worker/browser session provenance, status, label, filename, media type, byte size, SHA-256 hash, storage pathname, source kind, and idempotency key.
- [`lib/browser-images/server.ts`](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/lib/browser-images/server.ts) hashes and validates bytes, writes to a deterministic private blob path, handles a losing concurrent upload, and rechecks the hash on read.
- The model and final-task contract receive only a typed reference such as `{ id, label, filename, mediaType, byteSize, url }`, not base64 bytes or the private blob location ([reference type](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/lib/browser-images.ts), [task result](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/lib/task-completion.ts)).
- Its authenticated [artifact route](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/app/artifacts/%5BartifactId%5D/route.ts) resolves the app-owned ID to private bytes. Its [Linq delivery adapter](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/lib/linq-browser-image-delivery.ts) resolves exact artifact references, strips internal references from the visible message, and sends the bytes as native files.

OpenInstinct's root/worker contract also preserves the reference across continuation: a worker returns structured artifact references, the root renders only those exact IDs, and a blocked task resumes the same agent rather than fabricating a new result ([root instructions](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/instructions.md), [worker instructions](https://github.com/Merit-Systems/OpenInstinct/blob/480045dbc63008e7f99313d1683858cd8657b35a/agent/subagents/worker/instructions.md)).

**Reuse decision:** directly adapt the reservation/finalization lifecycle, idempotent ready reuse, content hashing, compact descriptor, scoped retrieval, cleanup, and native Linq-delivery semantics. Generalize `browser_images` to arbitrary files and map the records to Florence's household/work model. Do not port OpenInstinct's image-only schema, four-image output shape, Vercel Blob dependency, Eve worker runtime, or Next.js artifact route as Florence architecture.

## Hermes: useful artifact contracts, but not durable application storage

Hermes has two relevant patterns.

First, [`gateway/browser_control_artifacts.py`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/gateway/browser_control_artifacts.py) separates binary storage from command frames. `ArtifactStore` mints an opaque ID and a receipt containing the hash, byte size, content type, filename, timestamps, TTL, one-shot behavior, and an optional download path. It writes through a temporary file and atomic rename, validates the hash on load, scopes the artifact to a principal/transport family, and sends only the server-minted artifact ID through browser-control commands. The authenticated API exposes raw-byte upload and artifact-ID download endpoints in [`gateway/platforms/api_server.py`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/gateway/platforms/api_server.py), while [`gateway/browser_control_broker.py`](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/gateway/browser_control_broker.py) validates the reference before sending it to the browser client.

The important limit is structural: Hermes indexes these receipts in process memory, deletes orphaned files when the store is constructed, and commonly consumes an artifact on first read. Repository-wide production code does not complete the browser-download-to-artifact flow. This is a good transport receipt, but it cannot carry Florence work across a process restart or support “upload this, then also send it to me.”

Second, Hermes makes outbound attachment intent explicit. The model emits `MEDIA:/absolute/path`, the gateway parses and strips the control tag, validates the path, and the platform adapter sends the document after the text stream completes ([prompt contract](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/agent/prompt_builder.py), [path and media handling](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/gateway/platforms/base.py), [relay adapter](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/gateway/relay/adapter.py)). This is better than interpreting any file-looking text as permission to attach it.

Hermes's Google Drive skill also demonstrates a composable local-file handoff: `drive_download` returns path/name/media-type metadata and `drive_upload` consumes a local path ([source](https://github.com/NousResearch/hermes-agent/blob/6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882/skills/productivity/google-workspace/scripts/google_api.py)). That is evidence that one acquired artifact can feed a later provider, not a reason to add a Drive-specific implementation now.

**Reuse decision:** adapt Hermes's opaque ID, receipt fields, hash verification, atomic finalization, and explicit attachment intent. Reject its in-memory index, one-shot default, model-authored host paths, gateway stack, broad MIME allowlist, fixed TTL, and global byte ceiling as Florence product semantics. Provider-specific upload limits still need to be surfaced when encountered, but they must not become arbitrary Florence-wide count or retention caps.

## Pi: tool-result separation and automatic continuation

Pi has no browser acquisition or artifact persistence implementation. Its reusable contribution is the boundary between tool output, application details, and the next reasoning turn:

- [`AgentToolResult<T>`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/agent/src/types.ts) limits model-facing `content` to text/images while allowing arbitrary typed `details` for application/UI state.
- [`agent-loop.ts`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/agent/src/agent-loop.ts) finalizes a tool result, appends a `ToolResultMessage`, and automatically calls the model again. It can also continue a context whose last item is a user message or tool result.
- Pi's [`SessionManager`](https://github.com/earendil-works/pi-mono/blob/4e494929998d6bc4fccf75e0a233f727db4b70ee/packages/coding-agent/src/core/session-manager.ts) persists message/tool-result entries in an append-only JSONL tree and permits custom entries for non-model application state.

That shape supports the right Florence rule: the tool result should tell the model “artifact `A` is ready, named `receipt.pdf`, type PDF, size N” while the application record owns the storage pointer and exact provenance. The next turn can decide whether to inspect, upload, return, or retain it.

**Reuse decision:** adapt Pi's small result envelope and “persist result before continuation” lifecycle. Do not port its JSONL session store, coding-agent file conventions, or content model as Florence durability. The existing PostgreSQL work state remains the source of truth, and the actual file bytes remain outside model context.

## Florence-owned implementation and reuse classification

Every required Florence-owned piece has an explicit upstream disposition:

| Florence-owned piece | Upstream disposition | Why |
| --- | --- | --- |
| Kernel download/read and later upload | **Direct SDK reuse** | Kernel already implements browser-VM file transfer and Playwright buffer upload. Florence only needs to call it from the existing browser client and settle the operation through the existing work loop. |
| Generic artifact manifest | **Adapt OpenInstinct + Hermes** | OpenInstinct's pending/ready row and Hermes's receipt contain the right fields. Florence must own a generic schema because OpenInstinct is image-only and Hermes is in-memory/one-shot. |
| Durable byte storage | **Adapt OpenInstinct semantics; use Florence infrastructure** | Hash-before/finalize/private-object semantics are directly useful. Vercel Blob and a Next artifact route are not applicable to Florence's Railway/PostgreSQL architecture. |
| Artifact identity and idempotency | **Direct semantic adaptation from OpenInstinct** | `workId + toolCallId`-derived reservation and ready reuse prevent duplicate artifacts when a durable task retries. The namespace must be Florence's household/work namespace. |
| Model-facing artifact descriptor | **Adapt Pi + OpenInstinct** | Both keep large/private application data outside ordinary model text. Florence needs a generic descriptor rather than OpenInstinct's image Markdown URL. |
| Work continuation across restart | **Reuse Florence; borrow Pi lifecycle** | Florence already has durable PostgreSQL work/checkpoint state. Pi demonstrates result-before-next-turn, but its JSONL context is not application truth. Hermes's process-local store cannot provide this property. |
| Upload into a later browser | **Direct Kernel reuse, Florence ID resolution** | Resolve `artifactId` to authorized bytes, then call Playwright `setInputFiles()` with a buffer. OpenInstinct has no arbitrary file upload path; Florence's current upload only accepts parent-provided images/PDFs. |
| Return to the parent/family | **Direct Florence reuse; adapt explicit intent** | [`uploadAttachment`](../../packages/linq/src/index.ts) and `sendMove` already send raw bytes as native Linq attachments with Florence authority/idempotency. Borrow Hermes/OpenInstinct's explicit reference resolution, not their messaging stacks. |
| Retention and reuse | **Florence-owned; upstream limits not applicable** | Kernel VM lifetime, Hermes TTL/one-shot behavior, and OpenInstinct's output count are transport/runtime choices, not family-product policy. Retained household assets persist with the household until removal; transient intermediates may be released once no durable reference or active work requires them. Do not add a fixed artifact-count ceiling or arbitrary short TTL. |
| Cross-provider composition | **Florence-owned general capability** | Hermes Drive proves the pattern but is provider-specific. The artifact contract should be usable by any later connector without a Drive-first implementation. |

## Recommended Florence contract

The storage record should be generic and provider-independent. Exact database naming can follow the existing store, but it needs the equivalent of:

```text
artifact_id
household_id
visibility / owner scope inherited from the originating work
work_id
source_tool_call_id
idempotency_key
status                         pending | ready | failed
filename
content_type
byte_size
sha256
storage_key                    application-only
source_kind                    browser_download | browser_capture | generated | connector
source_browser_session_id      application-only, optional
source_url / remote_path       application-only, optional
created_at
ready_at
retention_class                transient_working | retained_household | user_returned
```

The model-facing descriptor should remain small:

```ts
type FlorenceArtifactRef = {
  artifactId: string;
  filename: string;
  contentType: string;
  byteSize: number;
  label?: string;
  sha256?: string;
};
```

The lifecycle should be:

1. Reserve by `(workId, sourceToolCallId)` before persisting bytes.
2. Trigger or observe the browser download, poll Kernel file visibility, and read bytes before browser teardown.
3. Write bytes to Florence-owned storage, compute/verify SHA-256, and atomically mark the manifest ready.
4. Persist the ready descriptor in the tool outcome/work checkpoint before invoking the model again.
5. On retry, return the same ready descriptor. If the record is pending, reconcile storage rather than blindly minting a second artifact.
6. For a later browser upload, resolve the ID under the current work's inherited scope and pass a buffer to Kernel `setInputFiles()`.
7. For user return, resolve the ID and use the existing Linq attachment upload/send path; retain the artifact until delivery settles so a provider retry does not require redownloading it.
8. Let subsequent reasoning choose the next tool. Do not encode per-domain continuation branches.

The public tool result must never expose the Kernel path, private storage key, signed object URL, or raw bytes. Those are execution details and make continuation brittle. An app-owned artifact ID is the stable handle.

## Current Florence fit

Florence already owns most of the surrounding path:

- [`apps/api/src/browser.ts`](../../apps/api/src/browser.ts) has browser capture and upload operations, but upload resolves only a caller-supplied attachment and there is no arbitrary browser-download promotion.
- [`packages/artifacts/src/index.ts`](../../packages/artifacts/src/index.ts) has a transient encrypted image vault and sealed PDF helpers, not a general durable artifact manifest.
- [`apps/api/src/florence.ts`](../../apps/api/src/florence.ts) already checkpoints browser work and stores selected browser images. The generic artifact descriptor belongs in this existing durable work flow, not in a second worker runtime.
- [`packages/linq/src/index.ts`](../../packages/linq/src/index.ts) already exposes `uploadAttachment` and media delivery through `sendMove`. That is the correct user-return path.

The missing product seam is narrow: promote arbitrary browser bytes into a durable generic artifact, carry the descriptor through existing work state, and allow later tools/delivery to resolve it.

## Product acceptance narratives

These are end-to-end product behaviors, not a request for broad new test infrastructure:

1. Florence downloads a school PDF, the Kernel browser closes, the worker restarts, and Florence uploads the exact same SHA-256 bytes to another site before returning the resulting file to a parent.
2. Florence downloads a receipt, summarizes it, and attaches the original receipt to the iMessage response.
3. The process stops after bytes are written but before the tool result is finalized; retry reconciles and returns the same artifact ID instead of creating a duplicate.
4. A parent steers the destination after the download; the existing artifact remains available without reopening the source website.
5. An attachment delivery fails transiently; Florence retries from the durable artifact instead of redownloading or losing it.
6. A useful family artifact is retained and remains available after the original task is older than the onboarding lookback window. The 90-day onboarding scan does not impose artifact expiry.

## Non-goals for this tranche

- Do not add Drive reading or a Drive-specific artifact path. Hermes's Drive code is composability evidence only.
- Do not build login/payment behavior, a general filesystem UI, or a second agent runtime.
- Do not keep a Kernel browser alive merely to preserve a downloaded file.
- Do not put file bytes or provider paths in model context.
- Do not introduce fixed artifact counts, five-tool-style ceilings, or short retention windows copied from upstream defaults.
- Do not hardcode workflows by domain. This is one general artifact continuation primitive for whichever family task needs it.

## Decision

Implement the bridge as **Kernel acquisition + Florence durable artifact + existing work continuation + Kernel/connector reuse + existing Linq return**.

The highest-value direct upstream reuse is Kernel's filesystem API and OpenInstinct's idempotent reserve/finalize/descriptor/delivery semantics. Hermes contributes the opaque-reference receipt and explicit attachment-intent pattern, but not the store. Pi contributes the result-before-continuation envelope, but not the bytes or persistence layer. This preserves Florence as one general family agent while giving it the missing ability to carry real files through real work.
