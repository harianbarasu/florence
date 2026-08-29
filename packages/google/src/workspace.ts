import { createHash } from "node:crypto";

/**
 * Google Workspace REST adapter.
 *
 * Operation contracts and provider projections are adapted from Nous Research's
 * Hermes Agent Google Workspace skill at commit
 * 6dcebea7fc5d0cc4f621eeaddf52b7d877a5f882, especially its MIME/reply handling,
 * Drive field projections and native-file semantics, People projection, Sheets
 * USER_ENTERED/INSERT_ROWS writes, and Docs text/index handling. Draft, forward,
 * multipart/attachment composition, and provider readback follow the same pinned
 * source's Himalaya composition and inbox-triage practices. Florence keeps those
 * useful contracts while adding bounded pagination and replay reconciliation.
 */

const GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me";
const DRIVE_API = "https://www.googleapis.com/drive/v3";
const PEOPLE_API = "https://people.googleapis.com/v1";
const DOCS_API = "https://docs.googleapis.com/v1";
const SHEETS_API = "https://sheets.googleapis.com/v4";
const SLIDES_API = "https://slides.googleapis.com/v1";
const TASKS_API = "https://tasks.googleapis.com/tasks/v1";
const REQUEST_TIMEOUT_MS = 20_000;
const MAX_PAGES = 20;
const MAX_BODY_CHARACTERS = 500_000;
const MAX_GMAIL_DRAFT_ATTACHMENT_BYTES = 20 * 1024 * 1024;
const MAX_GMAIL_DRAFT_TOTAL_ATTACHMENT_BYTES = 24 * 1024 * 1024;
const MAX_GMAIL_DRAFT_MESSAGE_RESULT_BYTES = 140_000;
const MAX_DRIVE_NATIVE_EXPORT_BYTES = 10 * 1024 * 1024;

export type GoogleSheetScalar = string | number | boolean | null;

export type GoogleContactSource = Readonly<{
  type: "CONTACT";
  id: string;
  etag: string;
}>;

export type GoogleWorkspaceMailAttachment =
  | Readonly<{
      source: "gmail";
      messageId: string;
      attachmentId: string;
    }>
  | Readonly<{
      source: "drive";
      fileId: string;
    }>;

export type GoogleWorkspaceOperation =
  | Readonly<{ operation: "gmail_get"; messageId: string }>
  | Readonly<{ operation: "gmail_search"; query: string; limit: number }>
  | Readonly<{
      operation: "gmail_send";
      to: readonly string[];
      cc: readonly string[];
      bcc: readonly string[];
      subject: string;
      body: string;
      bodyFormat: "plain" | "html";
      threadId?: string;
      idempotencyKey: string;
    }>
  | Readonly<{
      operation: "gmail_reply";
      messageId: string;
      body: string;
      bodyFormat: "plain" | "html";
      idempotencyKey: string;
    }>
  | Readonly<{
      operation: "gmail_draft_create";
      mode: "new";
      to: readonly string[];
      cc: readonly string[];
      bcc: readonly string[];
      subject: string;
      body: string;
      bodyFormat: "plain" | "html";
      attachments: readonly GoogleWorkspaceMailAttachment[];
      idempotencyKey: string;
    }>
  | Readonly<{
      operation: "gmail_draft_create";
      mode: "reply";
      messageId: string;
      body: string;
      bodyFormat: "plain" | "html";
      attachments: readonly GoogleWorkspaceMailAttachment[];
      idempotencyKey: string;
    }>
  | Readonly<{
      operation: "gmail_draft_create";
      mode: "forward";
      messageId: string;
      to: readonly string[];
      cc: readonly string[];
      bcc: readonly string[];
      body: string;
      bodyFormat: "plain" | "html";
      includeSourceAttachments: boolean;
      attachments: readonly GoogleWorkspaceMailAttachment[];
      idempotencyKey: string;
    }>
  | Readonly<{ operation: "gmail_draft_get"; draftId: string }>
  | Readonly<{
      operation: "gmail_draft_send";
      draftId: string;
      /** The exact messageHeaderId returned by gmail_draft_create for this draft. */
      messageHeaderId: string;
    }>
  | Readonly<{ operation: "gmail_labels" }>
  | Readonly<{
      operation: "gmail_modify";
      messageId: string;
      addLabelIds: readonly string[];
      removeLabelIds: readonly string[];
    }>
  | Readonly<{ operation: "drive_search"; query: string; limit: number }>
  | Readonly<{ operation: "drive_get"; fileId: string }>
  | Readonly<{
      operation: "drive_create_folder";
      name: string;
      parentId?: string;
      idempotencyKey: string;
    }>
  | Readonly<{
      operation: "drive_share";
      fileId: string;
      role: "reader" | "commenter" | "writer";
      type: "user" | "group" | "domain" | "anyone";
      email?: string;
      domain?: string;
      notify: boolean;
    }>
  | Readonly<{ operation: "drive_trash"; fileId: string }>
  | Readonly<{ operation: "contacts_search"; query: string; limit: number }>
  | Readonly<{
      operation: "contacts_create";
      givenName: string;
      familyName?: string;
      emails: readonly string[];
      phones: readonly string[];
    }>
  | Readonly<{
      operation: "contacts_update";
      resourceName: string;
      contactSource: GoogleContactSource;
      givenName?: string;
      familyName?: string;
      emails?: readonly string[];
      phones?: readonly string[];
    }>
  | Readonly<{ operation: "docs_get"; documentId: string }>
  | Readonly<{ operation: "docs_create"; title: string; body?: string; idempotencyKey: string }>
  | Readonly<{ operation: "docs_append"; documentId: string; text: string; tabId?: string }>
  | Readonly<{ operation: "sheets_get"; spreadsheetId: string; range: string }>
  | Readonly<{
      operation: "sheets_create";
      title: string;
      sheetName?: string;
      idempotencyKey: string;
    }>
  | Readonly<{
      operation: "sheets_update";
      spreadsheetId: string;
      range: string;
      values: readonly (readonly GoogleSheetScalar[])[];
    }>
  | Readonly<{
      operation: "sheets_append";
      spreadsheetId: string;
      range: string;
      values: readonly (readonly GoogleSheetScalar[])[];
    }>
  | Readonly<{ operation: "slides_get"; presentationId: string }>
  | Readonly<{ operation: "slides_create"; title: string; idempotencyKey: string }>
  | Readonly<{
      operation: "slides_add_text_slide";
      presentationId: string;
      title: string;
      body: string;
    }>
  | Readonly<{ operation: "tasklists_list" }>
  | Readonly<{
      operation: "tasks_list";
      taskListId?: string;
      showCompleted: boolean;
      limit: number;
    }>
  | Readonly<{
      operation: "tasks_create";
      taskListId?: string;
      title: string;
      notes?: string;
      due?: string;
    }>
  | Readonly<{
      operation: "tasks_update";
      taskListId: string;
      taskId: string;
      title?: string;
      notes?: string;
      due?: string;
      status?: "needsAction" | "completed";
    }>;

export type GoogleWorkspaceJsonValue =
  | string
  | number
  | boolean
  | null
  | readonly GoogleWorkspaceJsonValue[]
  | { readonly [key: string]: GoogleWorkspaceJsonValue };

export type GoogleWorkspaceResult = Readonly<{
  operation: GoogleWorkspaceOperation["operation"];
  result: { readonly [key: string]: GoogleWorkspaceJsonValue };
}>;

export type GoogleWorkspaceErrorCode =
  | "invalid_input"
  | "provider_rejected"
  | "provider_unavailable"
  | "invalid_response"
  | "reconciliation_failed";

export class GoogleWorkspaceError extends Error {
  readonly code: GoogleWorkspaceErrorCode;
  readonly service: string | null;
  readonly status: number | null;

  constructor(
    message: string,
    code: GoogleWorkspaceErrorCode,
    options: { service?: string; status?: number; cause?: unknown } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "GoogleWorkspaceError";
    this.code = code;
    this.service = options.service ?? null;
    this.status = options.status ?? null;
  }
}

type ExecutionContext = Readonly<{
  fetch: typeof fetch;
  accessToken: string;
  signal?: AbortSignal;
}>;

type JsonRecord = Record<string, unknown>;

export async function executeGoogleWorkspaceOperation(input: {
  fetch: typeof fetch;
  accessToken: string;
  operation: GoogleWorkspaceOperation;
  signal?: AbortSignal;
}): Promise<GoogleWorkspaceResult> {
  if (typeof input.fetch !== "function") invalid("Google Workspace fetch implementation is required");
  const accessToken = bounded(input.accessToken, "Google access token", 16_384);
  const context: ExecutionContext = {
    fetch: input.fetch,
    accessToken,
    ...(input.signal === undefined ? {} : { signal: input.signal }),
  };
  const operation = input.operation;

  switch (operation.operation) {
    case "gmail_get":
      return envelope(operation.operation, await gmailGet(context, operation.messageId));
    case "gmail_search":
      return envelope(operation.operation, await gmailSearch(context, operation.query, operation.limit));
    case "gmail_send":
      return envelope(operation.operation, await gmailSend(context, operation));
    case "gmail_reply":
      return envelope(operation.operation, await gmailReply(context, operation));
    case "gmail_draft_create":
      return envelope(operation.operation, await gmailDraftCreate(context, operation));
    case "gmail_draft_get":
      return envelope(operation.operation, await gmailDraftGet(context, operation.draftId));
    case "gmail_draft_send":
      return envelope(operation.operation, await gmailDraftSend(context, operation));
    case "gmail_labels":
      return envelope(operation.operation, await gmailLabels(context));
    case "gmail_modify":
      return envelope(operation.operation, await gmailModify(context, operation));
    case "drive_search":
      return envelope(operation.operation, await driveSearch(context, operation.query, operation.limit));
    case "drive_get":
      return envelope(operation.operation, { file: await driveGet(context, operation.fileId) });
    case "drive_create_folder":
      return envelope(operation.operation, await driveCreateFolder(context, operation));
    case "drive_share":
      return envelope(operation.operation, await driveShare(context, operation));
    case "drive_trash":
      return envelope(operation.operation, await driveTrash(context, operation.fileId));
    case "contacts_search":
      return envelope(operation.operation, await contactsSearch(context, operation.query, operation.limit));
    case "contacts_create":
      return envelope(operation.operation, await contactsCreate(context, operation));
    case "contacts_update":
      return envelope(operation.operation, await contactsUpdate(context, operation));
    case "docs_get":
      return envelope(operation.operation, { document: await docsGet(context, operation.documentId) });
    case "docs_create":
      return envelope(operation.operation, await docsCreate(context, operation));
    case "docs_append":
      return envelope(operation.operation, await docsAppend(context, operation));
    case "sheets_get":
      return envelope(
        operation.operation,
        await sheetsGet(context, operation.spreadsheetId, operation.range),
      );
    case "sheets_create":
      return envelope(operation.operation, await sheetsCreate(context, operation));
    case "sheets_update":
      return envelope(operation.operation, await sheetsUpdate(context, operation));
    case "sheets_append":
      return envelope(operation.operation, await sheetsAppend(context, operation));
    case "slides_get":
      return envelope(operation.operation, {
        presentation: await slidesGet(context, operation.presentationId),
      });
    case "slides_create":
      return envelope(operation.operation, await slidesCreate(context, operation));
    case "slides_add_text_slide":
      return envelope(operation.operation, await slidesAddTextSlide(context, operation));
    case "tasklists_list":
      return envelope(operation.operation, await tasklistsList(context));
    case "tasks_list":
      return envelope(operation.operation, await tasksList(context, operation));
    case "tasks_create":
      return envelope(operation.operation, await tasksCreate(context, operation));
    case "tasks_update":
      return envelope(operation.operation, await tasksUpdate(context, operation));
  }
}

function envelope(
  operation: GoogleWorkspaceOperation["operation"],
  result: { readonly [key: string]: GoogleWorkspaceJsonValue },
): GoogleWorkspaceResult {
  return { operation, result };
}

async function gmailGet(
  context: ExecutionContext,
  messageIdInput: string,
): Promise<{ readonly message: GoogleWorkspaceJsonValue }> {
  const messageId = identifier(messageIdInput, "Gmail message ID");
  const message = await googleJson(
    context,
    "Gmail",
    `${GMAIL_API}/messages/${encodeURIComponent(messageId)}?format=full`,
  );
  return { message: normalizeGmailMessage(message) };
}

async function gmailSearch(
  context: ExecutionContext,
  queryInput: string,
  limitInput: number,
): Promise<{ readonly messages: GoogleWorkspaceJsonValue }> {
  const query = boundedAllowEmpty(queryInput, "Gmail search query", 2_000);
  const limit = boundedInteger(limitInput, "Gmail search limit", 1, 100);
  const listed = await paginatedRecords(context, {
    service: "Gmail",
    url: `${GMAIL_API}/messages`,
    itemField: "messages",
    limit,
    params: { q: query, maxResults: String(Math.min(limit, 100)) },
  });
  const projected: GoogleWorkspaceJsonValue[] = [];
  for (let offset = 0; offset < listed.length; offset += 10) {
    const batch = listed.slice(offset, offset + 10);
    const messages = await Promise.all(
      batch.map(async (entry) => {
        const messageId = identifier(stringField(entry, "id"), "Gmail message ID");
        const params = new URLSearchParams({ format: "metadata" });
        for (const header of ["From", "To", "Subject", "Date"]) params.append("metadataHeaders", header);
        const message = await googleJson(
          context,
          "Gmail",
          `${GMAIL_API}/messages/${encodeURIComponent(messageId)}?${params}`,
        );
        const headers = gmailHeaders(recordField(message, "payload"));
        return {
          messageId: stringField(message, "id"),
          threadId: stringField(message, "threadId"),
          from: headers.get("from") ?? "",
          to: headers.get("to") ?? "",
          subject: headers.get("subject") ?? "",
          date: headers.get("date") ?? "",
          snippet: optionalString(message.snippet) ?? "",
          labelIds: stringArray(message.labelIds, "Gmail label IDs"),
        };
      }),
    );
    projected.push(...messages);
  }
  return { messages: projected };
}

async function gmailSend(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "gmail_send" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const to = emailList(operation.to, "Gmail To recipients", true);
  const cc = emailList(operation.cc, "Gmail Cc recipients", false);
  const bcc = emailList(operation.bcc, "Gmail Bcc recipients", false);
  if (to.length + cc.length + bcc.length > 100) invalid("Gmail has too many recipients");
  const subject = headerText(operation.subject, "Gmail subject", 998, true);
  const body = bodyText(operation.body, "Gmail body");
  const threadId = optionalIdentifier(operation.threadId, "Gmail thread ID");
  const actionKey = stableActionKey(operation.idempotencyKey);
  const messageHeaderId = deterministicMessageId(actionKey);
  const existing = await findSentMessage(context, messageHeaderId);
  if (existing) return sentResult("already_done", existing);

  const raw = createMimeMessage({
    to,
    cc,
    bcc,
    subject,
    body,
    bodyFormat: operation.bodyFormat,
    messageHeaderId,
  });
  const response = await googleJson(context, "Gmail", `${GMAIL_API}/messages/send`, {
    method: "POST",
    body: JSON.stringify({ raw, ...(threadId === null ? {} : { threadId }) }),
  });
  return sentResult("sent", response);
}

async function gmailReply(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "gmail_reply" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const originalId = identifier(operation.messageId, "Gmail message ID");
  const body = bodyText(operation.body, "Gmail reply body");
  const actionKey = stableActionKey(operation.idempotencyKey);
  const messageHeaderId = deterministicMessageId(actionKey);
  const existing = await findSentMessage(context, messageHeaderId);
  if (existing) return sentResult("already_done", existing);

  const original = await googleJson(
    context,
    "Gmail",
    `${GMAIL_API}/messages/${encodeURIComponent(originalId)}?format=metadata&metadataHeaders=From&metadataHeaders=Reply-To&metadataHeaders=Subject&metadataHeaders=Message-ID&metadataHeaders=References`,
  );
  const headers = gmailHeaders(recordField(original, "payload"));
  const recipient = mailboxFromHeader(
    headers.get("reply-to") ?? headers.get("from") ?? "",
    "Gmail reply recipient",
  );
  const originalSubject = headerText(headers.get("subject") ?? "", "Gmail reply subject", 998, true);
  const subject = headerText(
    /^re:/i.test(originalSubject) ? originalSubject : `Re: ${originalSubject}`,
    "Gmail reply subject",
    998,
    true,
  );
  const inReplyTo = optionalHeaderValue(headers.get("message-id"), "Gmail Message-ID");
  const references = optionalHeaderValue(headers.get("references"), "Gmail References");
  const threadId = identifier(stringField(original, "threadId"), "Gmail thread ID");
  const raw = createMimeMessage({
    to: [recipient],
    cc: [],
    bcc: [],
    subject,
    body,
    bodyFormat: operation.bodyFormat,
    messageHeaderId,
    ...(inReplyTo === null ? {} : { inReplyTo }),
    ...(inReplyTo === null
      ? {}
      : { references: references === null ? inReplyTo : `${references} ${inReplyTo}` }),
  });
  const response = await googleJson(context, "Gmail", `${GMAIL_API}/messages/send`, {
    method: "POST",
    body: JSON.stringify({ raw, threadId }),
  });
  return sentResult("sent", response);
}

type GmailDraftCreateOperation = Extract<GoogleWorkspaceOperation, { operation: "gmail_draft_create" }>;

type MailAttachmentContent = Readonly<{
  filename: string;
  mimeType: string;
  bytes: Buffer;
}>;

type GmailSourceMessage = Readonly<{
  message: JsonRecord;
  payload: JsonRecord;
  headers: Map<string, string>;
  body: ReturnType<typeof extractGmailBody>;
}>;

async function gmailDraftCreate(
  context: ExecutionContext,
  operation: GmailDraftCreateOperation,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const actionKey = stableActionKey(operation.idempotencyKey);
  const messageHeaderId = deterministicMessageId(actionKey);
  const sent = await findSentMessage(context, messageHeaderId);
  if (sent !== null) {
    return {
      status: "already_sent",
      draftId: null,
      messageHeaderId,
      messageId: stringField(sent, "id"),
      threadId: optionalString(sent.threadId),
    };
  }
  const existing = await findDraftByMessageHeaderId(context, messageHeaderId);
  if (existing !== null) {
    const draft = await readGmailDraft(context, stringField(existing, "id"));
    assertGmailMessageHeaderId(draft.message, messageHeaderId, "draft");
    return draftResult("already_done", draft.draftId, draft.message, messageHeaderId);
  }

  const sourceCache = new Map<string, GmailSourceMessage>();
  const composed = await composeGmailDraft(context, operation, sourceCache);
  const attachments = await resolveMailAttachments(context, composed.attachmentSources, sourceCache);
  const raw = createMimeMessage({
    to: composed.to,
    cc: composed.cc,
    bcc: composed.bcc,
    subject: composed.subject,
    body: composed.body,
    bodyFormat: operation.bodyFormat,
    messageHeaderId,
    ...(composed.inReplyTo === null ? {} : { inReplyTo: composed.inReplyTo }),
    ...(composed.references === null ? {} : { references: composed.references }),
    attachments,
  });
  const response = await googleJson(context, "Gmail", `${GMAIL_API}/drafts`, {
    method: "POST",
    body: JSON.stringify({
      message: {
        raw,
        ...(composed.threadId === null ? {} : { threadId: composed.threadId }),
      },
    }),
  });
  const draftId = identifier(stringField(response, "id"), "Gmail draft ID");
  const draft = await readGmailDraft(context, draftId);
  assertGmailMessageHeaderId(draft.message, messageHeaderId, "draft");
  return draftResult("created", draft.draftId, draft.message, messageHeaderId);
}

async function gmailDraftGet(
  context: ExecutionContext,
  draftIdInput: string,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const draft = await readGmailDraft(context, draftIdInput);
  return {
    draftId: draft.draftId,
    message: compactGmailDraftMessage(draft.message),
  };
}

async function gmailDraftSend(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "gmail_draft_send" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const draftId = identifier(operation.draftId, "Gmail draft ID");
  const messageHeaderId = florenceMessageHeaderId(operation.messageHeaderId);
  let draft: Awaited<ReturnType<typeof readGmailDraft>>;
  try {
    draft = await readGmailDraft(context, draftId);
  } catch (error) {
    if (error instanceof GoogleWorkspaceError && error.status === 404) {
      // Gmail deletes a draft when it sends it, so only the absent-draft retry path
      // may reconcile from Sent without first reading the provider draft.
      const sent = await findSentMessage(context, messageHeaderId);
      if (sent !== null) {
        return {
          ...sentResult("already_done", sent),
          draftId,
          messageHeaderId,
        };
      }
      throw new GoogleWorkspaceError(
        "Gmail draft no longer exists and no matching sent message was found",
        "reconciliation_failed",
        { service: "Gmail", cause: error },
      );
    }
    throw error;
  }
  assertGmailMessageHeaderId(draft.message, messageHeaderId, "draft");
  const sent = await findSentMessage(context, messageHeaderId);
  if (sent !== null) {
    return {
      ...sentResult("already_done", sent),
      draftId,
      messageHeaderId,
    };
  }
  const response = await googleJson(context, "Gmail", `${GMAIL_API}/drafts/send`, {
    method: "POST",
    body: JSON.stringify({ id: draftId }),
  });
  assertGmailMessageHeaderId(
    await googleJson(
      context,
      "Gmail",
      `${GMAIL_API}/messages/${encodeURIComponent(stringField(response, "id"))}?format=metadata&metadataHeaders=Message-ID`,
    ),
    messageHeaderId,
    "sent message",
  );
  return {
    ...sentResult("sent", response),
    draftId,
    messageHeaderId,
  };
}

async function composeGmailDraft(
  context: ExecutionContext,
  operation: GmailDraftCreateOperation,
  sourceCache: Map<string, GmailSourceMessage>,
): Promise<
  Readonly<{
    to: readonly string[];
    cc: readonly string[];
    bcc: readonly string[];
    subject: string;
    body: string;
    threadId: string | null;
    inReplyTo: string | null;
    references: string | null;
    attachmentSources: readonly GoogleWorkspaceMailAttachment[];
  }>
> {
  const body = boundedAllowEmpty(operation.body, "Gmail draft body", MAX_BODY_CHARACTERS);
  const attachmentSources = mailAttachmentSources(operation.attachments);
  if (operation.mode === "new") {
    const recipients = gmailRecipients(operation.to, operation.cc, operation.bcc);
    return {
      ...recipients,
      subject: headerText(operation.subject, "Gmail draft subject", 998, true),
      body,
      threadId: null,
      inReplyTo: null,
      references: null,
      attachmentSources,
    };
  }

  const source = await readGmailSourceMessage(context, operation.messageId, sourceCache);
  const originalSubject = headerText(source.headers.get("subject") ?? "", "Gmail source subject", 998, true);
  if (operation.mode === "reply") {
    const recipient = mailboxFromHeader(
      source.headers.get("reply-to") ?? source.headers.get("from") ?? "",
      "Gmail reply recipient",
    );
    const subject = headerText(
      /^re:/i.test(originalSubject) ? originalSubject : `Re: ${originalSubject}`,
      "Gmail reply subject",
      998,
      true,
    );
    const inReplyTo = optionalHeaderValue(source.headers.get("message-id"), "Gmail Message-ID");
    const priorReferences = optionalHeaderValue(source.headers.get("references"), "Gmail References");
    return {
      to: [recipient],
      cc: [],
      bcc: [],
      subject,
      body,
      threadId: identifier(stringField(source.message, "threadId"), "Gmail thread ID"),
      inReplyTo,
      references:
        inReplyTo === null ? null : priorReferences === null ? inReplyTo : `${priorReferences} ${inReplyTo}`,
      attachmentSources,
    };
  }

  const recipients = gmailRecipients(operation.to, operation.cc, operation.bcc);
  const subject = headerText(
    /^(?:fwd?|fw):/i.test(originalSubject) ? originalSubject : `Fwd: ${originalSubject}`,
    "Gmail forward subject",
    998,
    true,
  );
  const sourceAttachments = operation.includeSourceAttachments
    ? gmailAttachmentReferences(source.payload).map((attachment) => ({
        source: "gmail" as const,
        messageId: identifier(stringField(source.message, "id"), "Gmail message ID"),
        attachmentId: attachment.attachmentId,
      }))
    : [];
  return {
    ...recipients,
    subject,
    body: forwardedMessageBody(body, operation.bodyFormat, source),
    threadId: null,
    inReplyTo: null,
    references: null,
    attachmentSources: mailAttachmentSources([...attachmentSources, ...sourceAttachments]),
  };
}

function gmailRecipients(
  toInput: readonly string[],
  ccInput: readonly string[],
  bccInput: readonly string[],
): Readonly<{ to: readonly string[]; cc: readonly string[]; bcc: readonly string[] }> {
  const to = emailList(toInput, "Gmail To recipients", true);
  const cc = emailList(ccInput, "Gmail Cc recipients", false);
  const bcc = emailList(bccInput, "Gmail Bcc recipients", false);
  if (to.length + cc.length + bcc.length > 100) invalid("Gmail has too many recipients");
  return { to, cc, bcc };
}

function mailAttachmentSources(
  values: readonly GoogleWorkspaceMailAttachment[],
): readonly GoogleWorkspaceMailAttachment[] {
  if (!Array.isArray(values)) invalid("Gmail draft attachments must be an array");
  const result: GoogleWorkspaceMailAttachment[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    if (!isRecord(value)) invalid("Gmail draft attachment is invalid");
    let normalized: GoogleWorkspaceMailAttachment;
    let key: string;
    if (value.source === "gmail") {
      const messageId = identifier(value.messageId, "Gmail attachment message ID");
      const attachmentId = identifier(value.attachmentId, "Gmail attachment ID");
      normalized = { source: "gmail", messageId, attachmentId };
      key = `gmail:${messageId}:${attachmentId}`;
    } else if (value.source === "drive") {
      const fileId = identifier(value.fileId, "Drive attachment file ID");
      normalized = { source: "drive", fileId };
      key = `drive:${fileId}`;
    } else {
      invalid("Gmail draft attachment source is invalid");
    }
    if (!seen.has(key)) {
      seen.add(key);
      result.push(normalized);
    }
  }
  return result;
}

async function resolveMailAttachments(
  context: ExecutionContext,
  sources: readonly GoogleWorkspaceMailAttachment[],
  sourceCache: Map<string, GmailSourceMessage>,
): Promise<readonly MailAttachmentContent[]> {
  const attachments: MailAttachmentContent[] = [];
  let totalBytes = 0;
  for (const source of sources) {
    const attachment =
      source.source === "gmail"
        ? await readGmailAttachmentContent(context, source, sourceCache)
        : await readDriveAttachmentContent(context, source.fileId);
    totalBytes += attachment.bytes.byteLength;
    if (totalBytes > MAX_GMAIL_DRAFT_TOTAL_ATTACHMENT_BYTES) {
      invalid(`Gmail draft attachments exceed ${MAX_GMAIL_DRAFT_TOTAL_ATTACHMENT_BYTES} total bytes`);
    }
    attachments.push(attachment);
  }
  return attachments;
}

async function readGmailSourceMessage(
  context: ExecutionContext,
  messageIdInput: string,
  cache: Map<string, GmailSourceMessage>,
): Promise<GmailSourceMessage> {
  const messageId = identifier(messageIdInput, "Gmail message ID");
  const cached = cache.get(messageId);
  if (cached !== undefined) return cached;
  const message = await googleJson(
    context,
    "Gmail",
    `${GMAIL_API}/messages/${encodeURIComponent(messageId)}?format=full`,
  );
  const payload = recordField(message, "payload");
  const result = {
    message,
    payload,
    headers: gmailHeaders(payload),
    body: await extractGmailBodyForForward(context, messageId, payload),
  } as const;
  cache.set(messageId, result);
  return result;
}

async function readGmailAttachmentContent(
  context: ExecutionContext,
  source: Extract<GoogleWorkspaceMailAttachment, { source: "gmail" }>,
  cache: Map<string, GmailSourceMessage>,
): Promise<MailAttachmentContent> {
  const message = await readGmailSourceMessage(context, source.messageId, cache);
  const reference = gmailAttachmentReferences(message.payload).find(
    (candidate) => candidate.attachmentId === source.attachmentId,
  );
  if (reference === undefined) {
    throw new GoogleWorkspaceError(
      "Gmail attachment was not found on the specified message",
      "reconciliation_failed",
      { service: "Gmail" },
    );
  }
  const part = findGmailMimePart(message.payload, reference);
  if (part === null) invalidResponse("Gmail attachment MIME part disappeared", "Gmail");
  const body = recordField(part, "body");
  let encoded: string;
  if (reference.storage === "external") {
    const attachment = await googleJson(
      context,
      "Gmail",
      `${GMAIL_API}/messages/${encodeURIComponent(source.messageId)}/attachments/${encodeURIComponent(reference.providerAttachmentId)}`,
    );
    if (nonNegativeInteger(attachment.size, "Gmail attachment size", "Gmail") !== reference.sizeBytes) {
      invalidResponse("Gmail attachment size changed before drafting", "Gmail");
    }
    encoded = stringField(attachment, "data");
  } else {
    encoded = stringField(body, "data");
  }
  const bytes = decodeGmailAttachment(encoded, reference.sizeBytes);
  return {
    filename: reference.filename,
    mimeType: reference.mimeType,
    bytes,
  };
}

async function readDriveAttachmentContent(
  context: ExecutionContext,
  fileIdInput: string,
): Promise<MailAttachmentContent> {
  const fileId = identifier(fileIdInput, "Drive attachment file ID");
  const metadata = await googleJson(
    context,
    "Drive",
    `${DRIVE_API}/files/${encodeURIComponent(fileId)}?fields=id,name,mimeType,size,trashed,capabilities(canDownload)`,
  );
  if (metadata.trashed === true) {
    throw new GoogleWorkspaceError("Drive attachment is in the trash", "provider_rejected", {
      service: "Drive",
    });
  }
  const capabilities = recordField(metadata, "capabilities");
  if (capabilities.canDownload !== true) {
    throw new GoogleWorkspaceError("Drive file cannot be downloaded", "provider_rejected", {
      service: "Drive",
    });
  }
  const name = providerAttachmentFilename(stringField(metadata, "name"), "Drive");
  const sourceMimeType = mimeType(stringField(metadata, "mimeType"), "Drive attachment MIME type");
  const nativeExport = driveNativeAttachmentExport(sourceMimeType, name);
  if (nativeExport === null) {
    const declaredSize = optionalDriveFileSize(metadata.size);
    if (declaredSize !== null && declaredSize > MAX_GMAIL_DRAFT_ATTACHMENT_BYTES) {
      throw new GoogleWorkspaceError(
        `Drive attachment exceeds ${MAX_GMAIL_DRAFT_ATTACHMENT_BYTES} bytes`,
        "provider_rejected",
        { service: "Drive" },
      );
    }
  }
  const bytes = await googleBytes(
    context,
    "Drive",
    nativeExport === null
      ? `${DRIVE_API}/files/${encodeURIComponent(fileId)}?alt=media`
      : `${DRIVE_API}/files/${encodeURIComponent(fileId)}/export?${new URLSearchParams({ mimeType: nativeExport.mimeType })}`,
    nativeExport === null ? MAX_GMAIL_DRAFT_ATTACHMENT_BYTES : MAX_DRIVE_NATIVE_EXPORT_BYTES,
  );
  return {
    filename: nativeExport?.filename ?? name,
    mimeType: nativeExport?.mimeType ?? sourceMimeType,
    bytes,
  };
}

function driveNativeAttachmentExport(
  sourceMimeType: string,
  filename: string,
): Readonly<{ mimeType: string; filename: string }> | null {
  const nativeTypes = new Set([
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/vnd.google-apps.presentation",
    "application/vnd.google-apps.drawing",
  ]);
  if (nativeTypes.has(sourceMimeType)) {
    return {
      mimeType: "application/pdf",
      filename: replaceFilenameExtension(filename, ".pdf"),
    };
  }
  if (sourceMimeType.startsWith("application/vnd.google-apps.")) {
    throw new GoogleWorkspaceError(
      `Drive cannot export ${sourceMimeType} as a mail attachment`,
      "provider_rejected",
      { service: "Drive" },
    );
  }
  return null;
}

function replaceFilenameExtension(filename: string, extension: string): string {
  const lastDot = filename.lastIndexOf(".");
  const base = lastDot > 0 ? filename.slice(0, lastDot) : filename;
  return providerAttachmentFilename(`${base}${extension}`, "Drive");
}

function forwardedMessageBody(
  introduction: string,
  format: "plain" | "html",
  source: GmailSourceMessage,
): string {
  const fields = [
    ["From", source.headers.get("from") ?? ""],
    ["Date", source.headers.get("date") ?? ""],
    ["Subject", source.headers.get("subject") ?? ""],
    ["To", source.headers.get("to") ?? ""],
    ...(source.headers.get("cc") ? [["Cc", source.headers.get("cc") ?? ""]] : []),
  ] as const;
  if (format === "plain") {
    return boundedAllowEmpty(
      `${introduction}${introduction ? "\n\n" : ""}---------- Forwarded message ---------\n${fields
        .map(([label, value]) => `${label}: ${value}`)
        .join("\n")}\n\n${source.body.body}`,
      "Gmail forward body",
      MAX_BODY_CHARACTERS,
    );
  }
  const originalBody =
    source.body.format === "html" ? source.body.body : htmlEscape(source.body.body).replace(/\r?\n/g, "<br>");
  return boundedAllowEmpty(
    `${introduction}${introduction ? "<br><br>" : ""}<div class="gmail_quote"><div dir="ltr" class="gmail_attr">---------- Forwarded message ---------<br>${fields
      .map(([label, value]) => `<b>${label}:</b> ${htmlEscape(value)}<br>`)
      .join("")}</div><br>${originalBody}</div>`,
    "Gmail forward body",
    MAX_BODY_CHARACTERS,
  );
}

async function findDraftByMessageHeaderId(
  context: ExecutionContext,
  messageHeaderId: string,
): Promise<JsonRecord | null> {
  const query = new URLSearchParams({
    q: `rfc822msgid:${messageHeaderId.replace(/^<|>$/g, "")}`,
    maxResults: "2",
  });
  const response = await googleJson(context, "Gmail", `${GMAIL_API}/drafts?${query}`);
  const drafts = recordArray(response.drafts, "Gmail drafts");
  if (drafts.length > 1) {
    throw new GoogleWorkspaceError(
      "Gmail contains more than one draft for this Florence action",
      "reconciliation_failed",
      { service: "Gmail" },
    );
  }
  return drafts[0] ?? null;
}

async function readGmailDraft(
  context: ExecutionContext,
  draftIdInput: string,
): Promise<Readonly<{ draftId: string; message: JsonRecord }>> {
  const draftId = identifier(draftIdInput, "Gmail draft ID");
  const response = await googleJson(
    context,
    "Gmail",
    `${GMAIL_API}/drafts/${encodeURIComponent(draftId)}?format=full`,
  );
  return {
    draftId: identifier(stringField(response, "id"), "Gmail draft ID"),
    message: recordField(response, "message"),
  };
}

function assertGmailMessageHeaderId(message: JsonRecord, expected: string, label: string): void {
  const actual = gmailHeaders(recordField(message, "payload")).get("message-id");
  if (actual !== expected) {
    throw new GoogleWorkspaceError(
      `Gmail ${label} identity did not match the Florence action`,
      "reconciliation_failed",
      { service: "Gmail" },
    );
  }
}

function draftResult(
  status: "created" | "already_done",
  draftId: string,
  message: JsonRecord,
  messageHeaderId: string,
): { readonly [key: string]: GoogleWorkspaceJsonValue } {
  return {
    status,
    draftId,
    messageId: stringField(message, "id"),
    threadId: optionalString(message.threadId),
    messageHeaderId,
    message: compactGmailDraftMessage(message),
  };
}

async function gmailLabels(
  context: ExecutionContext,
): Promise<{ readonly labels: GoogleWorkspaceJsonValue }> {
  const response = await googleJson(context, "Gmail", `${GMAIL_API}/labels`);
  const labels = recordArray(response.labels, "Gmail labels").map((label) => ({
    id: stringField(label, "id"),
    name: stringField(label, "name"),
    type: optionalString(label.type) ?? "",
  }));
  return { labels };
}

async function gmailModify(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "gmail_modify" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const messageId = identifier(operation.messageId, "Gmail message ID");
  const addLabelIds = uniqueIdentifiers(operation.addLabelIds, "Gmail labels to add", 100);
  const removeLabelIds = uniqueIdentifiers(operation.removeLabelIds, "Gmail labels to remove", 100);
  if (addLabelIds.length === 0 && removeLabelIds.length === 0) {
    invalid("Gmail label modification requires at least one label");
  }
  const overlap = addLabelIds.find((label) => removeLabelIds.includes(label));
  if (overlap !== undefined) invalid(`Gmail label ${overlap} cannot be added and removed together`);
  const response = await googleJson(
    context,
    "Gmail",
    `${GMAIL_API}/messages/${encodeURIComponent(messageId)}/modify`,
    {
      method: "POST",
      body: JSON.stringify({ addLabelIds, removeLabelIds }),
    },
  );
  return {
    messageId: stringField(response, "id"),
    threadId: optionalString(response.threadId),
    labelIds: stringArray(response.labelIds, "Gmail label IDs"),
  };
}

async function findSentMessage(
  context: ExecutionContext,
  messageHeaderId: string,
): Promise<JsonRecord | null> {
  const query = new URLSearchParams({
    q: `in:sent rfc822msgid:${messageHeaderId.replace(/^<|>$/g, "")}`,
    maxResults: "1",
  });
  const response = await googleJson(context, "Gmail", `${GMAIL_API}/messages?${query}`);
  const listed = recordArray(response.messages, "Gmail messages");
  return listed[0] ?? null;
}

function sentResult(
  status: "sent" | "already_done",
  message: JsonRecord,
): { readonly [key: string]: GoogleWorkspaceJsonValue } {
  return {
    status,
    messageId: stringField(message, "id"),
    threadId: optionalString(message.threadId),
  };
}

function normalizeGmailMessage(message: JsonRecord): { readonly [key: string]: GoogleWorkspaceJsonValue } {
  const payload = recordField(message, "payload");
  const headers = gmailHeaders(payload);
  const extracted = extractGmailBody(payload);
  return {
    messageId: stringField(message, "id"),
    threadId: stringField(message, "threadId"),
    historyId: optionalString(message.historyId),
    from: headers.get("from") ?? "",
    to: headers.get("to") ?? "",
    cc: headers.get("cc") ?? "",
    bcc: headers.get("bcc") ?? "",
    replyTo: headers.get("reply-to") ?? "",
    subject: headers.get("subject") ?? "",
    date: headers.get("date") ?? "",
    messageHeaderId: headers.get("message-id") ?? "",
    labelIds: stringArray(message.labelIds, "Gmail label IDs"),
    snippet: optionalString(message.snippet) ?? "",
    body: extracted.body,
    bodyFormat: extracted.format,
    attachments: gmailAttachmentReferences(payload).map((attachment) => ({
      attachmentId: attachment.attachmentId,
      partId: attachment.partId,
      filename: attachment.filename,
      mimeType: attachment.mimeType,
      sizeBytes: attachment.sizeBytes,
    })),
  };
}

function compactGmailDraftMessage(message: JsonRecord): { readonly [key: string]: GoogleWorkspaceJsonValue } {
  const normalized = normalizeGmailMessage(message);
  const body = normalized.body;
  if (typeof body !== "string") invalidResponse("Gmail draft body is invalid", "Gmail");
  const complete = { ...normalized, bodyTruncated: false } as const;
  if (Buffer.byteLength(JSON.stringify(complete), "utf8") <= MAX_GMAIL_DRAFT_MESSAGE_RESULT_BYTES) {
    return complete;
  }

  const characters = [...body];
  let lower = 0;
  let upper = characters.length;
  let compact: { readonly [key: string]: GoogleWorkspaceJsonValue } = {
    ...normalized,
    body: "",
    bodyTruncated: true,
  };
  if (Buffer.byteLength(JSON.stringify(compact), "utf8") > MAX_GMAIL_DRAFT_MESSAGE_RESULT_BYTES) {
    invalidResponse("Gmail draft metadata is too large", "Gmail");
  }
  while (lower <= upper) {
    const midpoint = Math.floor((lower + upper) / 2);
    const candidate = {
      ...normalized,
      body: characters.slice(0, midpoint).join(""),
      bodyTruncated: true,
    } as const;
    if (Buffer.byteLength(JSON.stringify(candidate), "utf8") <= MAX_GMAIL_DRAFT_MESSAGE_RESULT_BYTES) {
      compact = candidate;
      lower = midpoint + 1;
    } else {
      upper = midpoint - 1;
    }
  }
  return compact;
}

function gmailHeaders(payload: JsonRecord): Map<string, string> {
  const headers = recordArray(payload.headers, "Gmail headers");
  const result = new Map<string, string>();
  for (const header of headers) {
    const name = optionalString(header.name)?.toLowerCase();
    const value = optionalString(header.value);
    if (name && value && !result.has(name)) result.set(name, value.slice(0, 10_000));
  }
  return result;
}

function extractGmailBody(payload: JsonRecord): { body: string; format: "plain" | "html" | "unknown" } {
  const candidates: { body: string; format: "plain" | "html" }[] = [];
  const visit = (part: JsonRecord, depth: number): void => {
    if (depth > 20 || candidates.length >= 100) return;
    const mimeType = optionalString(part.mimeType)?.toLowerCase() ?? "";
    const body = optionalRecord(part.body);
    const data = body === null ? null : optionalString(body.data);
    if (data !== null && (mimeType === "text/plain" || mimeType === "text/html")) {
      const decoded = decodeBase64Url(data).slice(0, MAX_BODY_CHARACTERS);
      candidates.push({ body: decoded, format: mimeType === "text/plain" ? "plain" : "html" });
    }
    for (const child of recordArray(part.parts, "Gmail MIME parts")) visit(child, depth + 1);
  };
  visit(payload, 0);
  const chosen = candidates.find((candidate) => candidate.format === "plain") ?? candidates[0];
  return chosen ?? { body: "", format: "unknown" };
}

async function extractGmailBodyForForward(
  context: ExecutionContext,
  messageId: string,
  payload: JsonRecord,
): Promise<{ body: string; format: "plain" | "html" | "unknown" }> {
  const candidates: {
    data: string | null;
    attachmentId: string | null;
    declaredSize: number;
    format: "plain" | "html";
  }[] = [];
  const visit = (part: JsonRecord, depth: number): void => {
    if (depth > 20 || candidates.length >= 100) return;
    const mimeType = optionalString(part.mimeType)?.toLowerCase() ?? "";
    const body = optionalRecord(part.body);
    if (body !== null && (mimeType === "text/plain" || mimeType === "text/html")) {
      const data = optionalString(body.data);
      const attachmentId = optionalString(body.attachmentId);
      if (data !== null || attachmentId !== null) {
        candidates.push({
          data,
          attachmentId,
          declaredSize: nonNegativeInteger(body.size, "Gmail message body size", "Gmail"),
          format: mimeType === "text/plain" ? "plain" : "html",
        });
      }
    }
    for (const child of recordArray(part.parts, "Gmail MIME parts")) visit(child, depth + 1);
  };
  visit(payload, 0);
  const chosen = candidates.find((candidate) => candidate.format === "plain") ?? candidates[0];
  if (chosen === undefined) return { body: "", format: "unknown" };
  if (chosen.data !== null) {
    return { body: decodeBase64Url(chosen.data).slice(0, MAX_BODY_CHARACTERS), format: chosen.format };
  }
  if (chosen.attachmentId === null) return { body: "", format: "unknown" };
  const attachment = await googleJson(
    context,
    "Gmail",
    `${GMAIL_API}/messages/${encodeURIComponent(messageId)}/attachments/${encodeURIComponent(chosen.attachmentId)}`,
  );
  if (nonNegativeInteger(attachment.size, "Gmail message body size", "Gmail") !== chosen.declaredSize) {
    invalidResponse("Gmail message body size changed before forwarding", "Gmail");
  }
  return {
    body: decodeBase64Url(stringField(attachment, "data")).slice(0, MAX_BODY_CHARACTERS),
    format: chosen.format,
  };
}

type GmailDraftAttachmentReference = Readonly<{
  attachmentId: string;
  providerAttachmentId: string;
  storage: "external" | "inline";
  partId: string;
  filename: string;
  mimeType: string;
  sizeBytes: number;
}>;

function gmailAttachmentReferences(payload: JsonRecord): readonly GmailDraftAttachmentReference[] {
  const result: GmailDraftAttachmentReference[] = [];
  const visit = (part: JsonRecord, depth: number): void => {
    if (depth > 20) {
      invalidResponse("Gmail message MIME structure is too deep", "Gmail");
    }
    const filenameValue = optionalString(part.filename);
    const body = optionalRecord(part.body);
    const partIdValue = optionalString(part.partId);
    if (filenameValue && body !== null) {
      const filename = providerAttachmentFilename(filenameValue, "Gmail");
      const attachmentMimeType = mimeType(
        optionalString(part.mimeType) ?? "application/octet-stream",
        "Gmail attachment MIME type",
      );
      const sizeBytes = nonNegativeInteger(body.size, "Gmail attachment size", "Gmail");
      const providerAttachmentId = optionalString(body.attachmentId);
      const inlineData = optionalString(body.data);
      if (providerAttachmentId !== null) {
        result.push({
          attachmentId: providerAttachmentId,
          providerAttachmentId,
          storage: "external",
          partId: partIdValue ?? "",
          filename,
          mimeType: attachmentMimeType,
          sizeBytes,
        });
      } else if (inlineData !== null && partIdValue !== null) {
        result.push({
          attachmentId: `inline:${partIdValue}`,
          providerAttachmentId: "",
          storage: "inline",
          partId: partIdValue,
          filename,
          mimeType: attachmentMimeType,
          sizeBytes,
        });
      }
    }
    for (const child of recordArray(part.parts, "Gmail MIME parts")) visit(child, depth + 1);
  };
  visit(payload, 0);
  return result;
}

function findGmailMimePart(payload: JsonRecord, reference: GmailDraftAttachmentReference): JsonRecord | null {
  let found: JsonRecord | null = null;
  const visit = (part: JsonRecord, depth: number): void => {
    if (found !== null || depth > 20) return;
    const partId = optionalString(part.partId) ?? "";
    const body = optionalRecord(part.body);
    const attachmentId = body === null ? null : optionalString(body.attachmentId);
    const matchesStorage =
      reference.storage === "external"
        ? attachmentId === reference.providerAttachmentId
        : body !== null && optionalString(body.data) !== null;
    if (partId === reference.partId && matchesStorage) {
      found = part;
      return;
    }
    for (const child of recordArray(part.parts, "Gmail MIME parts")) visit(child, depth + 1);
  };
  visit(payload, 0);
  return found;
}

function createMimeMessage(input: {
  to: readonly string[];
  cc: readonly string[];
  bcc: readonly string[];
  subject: string;
  body: string;
  bodyFormat: "plain" | "html";
  messageHeaderId: string;
  inReplyTo?: string;
  references?: string;
  attachments?: readonly MailAttachmentContent[];
}): string {
  const commonHeaders = [
    `To: ${input.to.join(", ")}`,
    ...(input.cc.length === 0 ? [] : [`Cc: ${input.cc.join(", ")}`]),
    ...(input.bcc.length === 0 ? [] : [`Bcc: ${input.bcc.join(", ")}`]),
    `Subject: ${encodedHeader(input.subject)}`,
    `Message-ID: ${input.messageHeaderId}`,
    ...(input.inReplyTo === undefined ? [] : [`In-Reply-To: ${input.inReplyTo}`]),
    ...(input.references === undefined ? [] : [`References: ${input.references}`]),
    "MIME-Version: 1.0",
  ];
  const attachments = input.attachments ?? [];
  const encodedBody = foldedBase64(Buffer.from(input.body, "utf8"));
  if (attachments.length === 0) {
    const headers = [
      ...commonHeaders,
      `Content-Type: text/${input.bodyFormat}; charset=UTF-8`,
      "Content-Transfer-Encoding: base64",
    ];
    return Buffer.from(`${headers.join("\r\n")}\r\n\r\n${encodedBody}\r\n`, "utf8").toString("base64url");
  }

  const boundary = `florence_${createHash("sha256").update(input.messageHeaderId, "utf8").digest("hex")}`;
  const parts = [
    `--${boundary}\r\nContent-Type: text/${input.bodyFormat}; charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n${encodedBody}\r\n`,
    ...attachments.map((attachment) => {
      const filename = mimeQuotedFilename(attachment.filename);
      return `--${boundary}\r\nContent-Type: ${attachment.mimeType}; name="${filename}"\r\nContent-Disposition: attachment; filename="${filename}"\r\nContent-Transfer-Encoding: base64\r\n\r\n${foldedBase64(attachment.bytes)}\r\n`;
    }),
    `--${boundary}--\r\n`,
  ];
  return Buffer.from(
    `${[...commonHeaders, `Content-Type: multipart/mixed; boundary="${boundary}"`].join("\r\n")}\r\n\r\n${parts.join("")}`,
    "utf8",
  ).toString("base64url");
}

function foldedBase64(bytes: Buffer): string {
  return (
    bytes
      .toString("base64")
      .match(/.{1,76}/g)
      ?.join("\r\n") ?? ""
  );
}

function mimeQuotedFilename(filename: string): string {
  const encoded = encodedHeader(filename);
  return encoded.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

const DRIVE_FILE_FIELDS =
  "id,name,mimeType,modifiedTime,createdTime,size,webViewLink,parents,owners(displayName,emailAddress),trashed,appProperties";

async function driveSearch(
  context: ExecutionContext,
  queryInput: string,
  limitInput: number,
): Promise<{ readonly files: GoogleWorkspaceJsonValue }> {
  const query = boundedAllowEmpty(queryInput, "Drive search query", 1_000);
  const limit = boundedInteger(limitInput, "Drive search limit", 1, 100);
  const driveQuery = query.trim()
    ? `fullText contains '${escapeDriveQuery(query.trim())}' and trashed = false`
    : "trashed = false";
  const files = await paginatedRecords(context, {
    service: "Drive",
    url: `${DRIVE_API}/files`,
    itemField: "files",
    limit,
    params: {
      q: driveQuery,
      pageSize: String(Math.min(limit, 100)),
      orderBy: "modifiedTime desc",
      spaces: "drive",
      fields: `nextPageToken,files(${DRIVE_FILE_FIELDS})`,
    },
  });
  return { files: files.map(normalizeDriveFile) };
}

async function driveGet(context: ExecutionContext, fileIdInput: string): Promise<GoogleWorkspaceJsonValue> {
  const fileId = identifier(fileIdInput, "Drive file ID");
  const params = new URLSearchParams({ fields: DRIVE_FILE_FIELDS });
  const response = await googleJson(
    context,
    "Drive",
    `${DRIVE_API}/files/${encodeURIComponent(fileId)}?${params}`,
  );
  return normalizeDriveFile(response);
}

async function driveCreateFolder(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "drive_create_folder" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const name = bounded(operation.name, "Drive folder name", 255);
  const parentId = optionalIdentifier(operation.parentId, "Drive parent folder ID");
  const actionKey = stableActionKey(operation.idempotencyKey);
  const existing = await findDriveActionFile(context, actionKey);
  if (existing !== null) {
    assertDriveFileKind(existing, "application/vnd.google-apps.folder", "folder");
    return { status: "already_done", folder: normalizeDriveFile(existing) };
  }
  const created = await createDriveNativeFile(context, {
    name,
    mimeType: "application/vnd.google-apps.folder",
    actionKey,
    ...(parentId === null ? {} : { parentId }),
  });
  return { status: "created", folder: normalizeDriveFile(created) };
}

async function driveShare(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "drive_share" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const fileId = identifier(operation.fileId, "Drive file ID");
  const email =
    operation.email === undefined ? null : emailAddress(operation.email, "Drive permission email");
  const domain = operation.domain === undefined ? null : domainName(operation.domain);
  if ((operation.type === "user" || operation.type === "group") && email === null) {
    invalid(`Drive ${operation.type} permission requires an email`);
  }
  if (operation.type === "domain" && domain === null) invalid("Drive domain permission requires a domain");
  if ((operation.type === "anyone" || operation.type === "domain") && operation.notify) {
    invalid("Drive can only notify a user or group permission recipient");
  }
  const permission: Record<string, string> = {
    type: operation.type,
    role: operation.role,
    ...(email === null ? {} : { emailAddress: email }),
    ...(domain === null ? {} : { domain }),
  };
  const existingPermissions = await paginatedRecords(context, {
    service: "Drive",
    url: `${DRIVE_API}/files/${encodeURIComponent(fileId)}/permissions`,
    itemField: "permissions",
    limit: 2_000,
    requireComplete: true,
    params: {
      pageSize: "100",
      fields: "nextPageToken,permissions(id,type,role,emailAddress,domain,allowFileDiscovery)",
    },
  });
  const existing = existingPermissions.find(
    (candidate) =>
      optionalString(candidate.type) === operation.type &&
      optionalString(candidate.role) === operation.role &&
      (optionalString(candidate.emailAddress)?.toLocaleLowerCase() ?? null) ===
        (email?.toLocaleLowerCase() ?? null) &&
      (optionalString(candidate.domain)?.toLocaleLowerCase() ?? null) ===
        (domain?.toLocaleLowerCase() ?? null),
  );
  if (existing !== undefined) {
    return {
      status: "already_done",
      fileId,
      permission: {
        permissionId: stringField(existing, "id"),
        type: operation.type,
        role: operation.role,
        email,
        domain,
      },
    };
  }
  const params = new URLSearchParams({
    sendNotificationEmail: String(operation.notify),
    fields: "id,type,role,emailAddress,domain,allowFileDiscovery",
  });
  const response = await googleJson(
    context,
    "Drive",
    `${DRIVE_API}/files/${encodeURIComponent(fileId)}/permissions?${params}`,
    { method: "POST", body: JSON.stringify(permission) },
  );
  return {
    status: "shared",
    fileId,
    permission: {
      permissionId: stringField(response, "id"),
      type: optionalString(response.type) ?? operation.type,
      role: optionalString(response.role) ?? operation.role,
      email: optionalString(response.emailAddress),
      domain: optionalString(response.domain),
    },
  };
}

async function driveTrash(
  context: ExecutionContext,
  fileIdInput: string,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const fileId = identifier(fileIdInput, "Drive file ID");
  const params = new URLSearchParams({ fields: DRIVE_FILE_FIELDS });
  const response = await googleJson(
    context,
    "Drive",
    `${DRIVE_API}/files/${encodeURIComponent(fileId)}?${params}`,
    { method: "PATCH", body: JSON.stringify({ trashed: true }) },
  );
  return { status: "trashed", file: normalizeDriveFile(response) };
}

async function findDriveActionFile(context: ExecutionContext, actionKey: string): Promise<JsonRecord | null> {
  const files = await paginatedRecords(context, {
    service: "Drive",
    url: `${DRIVE_API}/files`,
    itemField: "files",
    limit: 2,
    params: {
      q: `appProperties has { key='florenceActionKey' and value='${actionKey}' } and trashed = false`,
      pageSize: "2",
      spaces: "drive",
      fields: `nextPageToken,files(${DRIVE_FILE_FIELDS})`,
    },
  });
  if (files.length > 1) {
    throw new GoogleWorkspaceError(
      "Google Drive contains more than one file for this Florence action",
      "reconciliation_failed",
      { service: "Drive" },
    );
  }
  return files[0] ?? null;
}

async function createDriveNativeFile(
  context: ExecutionContext,
  input: {
    name: string;
    mimeType: string;
    actionKey: string;
    parentId?: string;
  },
): Promise<JsonRecord> {
  const params = new URLSearchParams({ fields: DRIVE_FILE_FIELDS });
  return googleJson(context, "Drive", `${DRIVE_API}/files?${params}`, {
    method: "POST",
    body: JSON.stringify({
      name: input.name,
      mimeType: input.mimeType,
      appProperties: { florenceActionKey: input.actionKey },
      ...(input.parentId === undefined ? {} : { parents: [input.parentId] }),
    }),
  });
}

function normalizeDriveFile(file: JsonRecord): GoogleWorkspaceJsonValue {
  const mimeType = stringField(file, "mimeType");
  const owners = recordArray(file.owners, "Drive owners").map((owner) => ({
    displayName: optionalString(owner.displayName) ?? "",
    email: optionalString(owner.emailAddress) ?? "",
  }));
  return {
    fileId: stringField(file, "id"),
    name: stringField(file, "name"),
    mimeType,
    modifiedTime: optionalString(file.modifiedTime),
    createdTime: optionalString(file.createdTime),
    size: optionalString(file.size),
    webViewLink: optionalString(file.webViewLink),
    parents: stringArray(file.parents, "Drive parents"),
    owners,
    trashed: optionalBoolean(file.trashed) ?? false,
  };
}

function assertDriveFileKind(file: JsonRecord, expectedMimeType: string, label: string): void {
  if (stringField(file, "mimeType") !== expectedMimeType) {
    throw new GoogleWorkspaceError(
      `A prior Florence action created a different Drive resource instead of the expected ${label}`,
      "reconciliation_failed",
      { service: "Drive" },
    );
  }
}

type ContactDraft = Readonly<{
  givenName: string;
  familyName: string;
  emails: readonly string[];
  phones: readonly string[];
}>;

async function contactsSearch(
  context: ExecutionContext,
  queryInput: string,
  limitInput: number,
): Promise<{ readonly contacts: GoogleWorkspaceJsonValue }> {
  const query = boundedAllowEmpty(queryInput, "Contacts search query", 500).trim().toLocaleLowerCase();
  const limit = boundedInteger(limitInput, "Contacts search limit", 1, 100);
  const contacts = await allContacts(context, 20_000);
  const matching = contacts
    .map(normalizeContact)
    .filter((contact) => !query || contactSearchText(contact).includes(query))
    .slice(0, limit);
  return { contacts: matching };
}

async function contactsCreate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "contacts_create" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const draft = contactDraft(operation);
  const contacts = await allContacts(context, 20_000);
  const matches = contacts.filter((contact) => contactMatches(normalizeContact(contact), draft));
  if (matches.length > 1) {
    throw new GoogleWorkspaceError(
      "Google Contacts contains more than one exact match for this contact",
      "reconciliation_failed",
      { service: "Contacts" },
    );
  }
  if (matches[0] !== undefined) {
    return { status: "already_done", contact: normalizeContact(matches[0]) };
  }
  const response = await googleJson(
    context,
    "Contacts",
    `${PEOPLE_API}/people:createContact?personFields=names,emailAddresses,phoneNumbers,metadata`,
    { method: "POST", body: JSON.stringify(contactRequest(draft)) },
  );
  return { status: "created", contact: normalizeContact(response) };
}

async function contactsUpdate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "contacts_update" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const resourceName = bounded(operation.resourceName, "Contact resource name", 1_024);
  if (!/^people\/[A-Za-z0-9._~-]+$/.test(resourceName)) invalid("Contact resource name is invalid");
  const contactSource = validatedContactSource(operation.contactSource);
  const updates: JsonRecord = {};
  const updatePersonFields: string[] = [];
  if (operation.givenName !== undefined || operation.familyName !== undefined) {
    const current = await contactGet(context, resourceName);
    if (current.contactSource.id !== contactSource.id || current.contactSource.etag !== contactSource.etag) {
      throw new GoogleWorkspaceError(
        "The Google contact changed since it was read; search for it again before updating",
        "reconciliation_failed",
        { service: "Contacts" },
      );
    }
    const givenName =
      operation.givenName === undefined
        ? current.givenName
        : bounded(operation.givenName, "Contact given name", 256);
    const familyName =
      operation.familyName === undefined
        ? current.familyName
        : boundedAllowEmpty(operation.familyName, "Contact family name", 256).trim();
    updates.names =
      givenName || familyName
        ? [
            {
              ...(givenName ? { givenName } : {}),
              ...(familyName ? { familyName } : {}),
            },
          ]
        : [];
    updatePersonFields.push("names");
  }
  if (operation.emails !== undefined) {
    updates.emailAddresses = contactEmailValues(operation.emails).map((value) => ({ value }));
    updatePersonFields.push("emailAddresses");
  }
  if (operation.phones !== undefined) {
    updates.phoneNumbers = contactPhoneValues(operation.phones).map((value) => ({ value }));
    updatePersonFields.push("phoneNumbers");
  }
  if (updatePersonFields.length === 0) {
    invalid("Contact update requires at least one name, email, or phone field");
  }
  const params = new URLSearchParams({
    updatePersonFields: updatePersonFields.join(","),
    personFields: "names,emailAddresses,phoneNumbers,metadata",
  });
  const response = await googleJson(
    context,
    "Contacts",
    `${PEOPLE_API}/${resourceName}:updateContact?${params}`,
    {
      method: "PATCH",
      body: JSON.stringify({
        ...updates,
        metadata: { sources: [contactSource] },
      }),
    },
  );
  return { status: "updated", contact: normalizeContact(response) };
}

async function contactGet(
  context: ExecutionContext,
  resourceName: string,
): Promise<ReturnType<typeof normalizeContact>> {
  const params = new URLSearchParams({
    personFields: "names,emailAddresses,phoneNumbers,metadata",
    sources: "READ_SOURCE_TYPE_CONTACT",
  });
  const response = await googleJson(context, "Contacts", `${PEOPLE_API}/${resourceName}?${params}`);
  return normalizeContact(response);
}

async function allContacts(context: ExecutionContext, limit: number): Promise<readonly JsonRecord[]> {
  return paginatedRecords(context, {
    service: "Contacts",
    url: `${PEOPLE_API}/people/me/connections`,
    itemField: "connections",
    limit,
    requireComplete: true,
    params: {
      pageSize: "1000",
      personFields: "names,emailAddresses,phoneNumbers,metadata",
      sources: "READ_SOURCE_TYPE_CONTACT",
      sortOrder: "LAST_MODIFIED_DESCENDING",
    },
  });
}

function contactDraft(input: {
  givenName: string;
  familyName?: string;
  emails: readonly string[];
  phones: readonly string[];
}): ContactDraft {
  const givenName = bounded(input.givenName, "Contact given name", 256);
  const familyName =
    input.familyName === undefined
      ? ""
      : boundedAllowEmpty(input.familyName, "Contact family name", 256).trim();
  const emails = contactEmailValues(input.emails);
  const phones = contactPhoneValues(input.phones);
  if (emails.length === 0 && phones.length === 0) invalid("Contact requires an email or phone number");
  return { givenName, familyName, emails, phones };
}

function contactEmailValues(values: readonly string[]): readonly string[] {
  if (!Array.isArray(values)) invalid("Contact emails must be an array");
  return uniqueStrings(
    values.map((email) => emailAddress(email, "Contact email")),
    "Contact emails",
    20,
  );
}

function contactPhoneValues(values: readonly string[]): readonly string[] {
  if (!Array.isArray(values)) invalid("Contact phones must be an array");
  return uniqueStrings(
    values.map((phone) => bounded(phone, "Contact phone", 100)),
    "Contact phones",
    20,
  );
}

function contactRequest(draft: ContactDraft): JsonRecord {
  return {
    names: [{ givenName: draft.givenName, ...(draft.familyName ? { familyName: draft.familyName } : {}) }],
    emailAddresses: draft.emails.map((value) => ({ value })),
    phoneNumbers: draft.phones.map((value) => ({ value })),
  };
}

function validatedContactSource(source: GoogleContactSource): GoogleContactSource {
  if (!isRecord(source) || source.type !== "CONTACT") {
    invalid("Contact update requires the CONTACT source returned by Contacts search");
  }
  return {
    type: "CONTACT",
    id: identifier(source.id, "Contact source ID"),
    etag: bounded(source.etag, "Contact source etag", 1_024),
  };
}

function contactSource(contact: JsonRecord): GoogleContactSource {
  const metadata = recordField(contact, "metadata");
  const sources = recordArray(metadata.sources, "Contact sources").filter(
    (source) => optionalString(source.type) === "CONTACT",
  );
  if (sources.length !== 1) {
    invalidResponse("Google Contacts did not return exactly one writable CONTACT source", "Contacts");
  }
  const source = sources[0] as JsonRecord;
  return {
    type: "CONTACT",
    id: stringField(source, "id"),
    etag: stringField(source, "etag"),
  };
}

function normalizeContact(contact: JsonRecord): {
  resourceName: string;
  contactSource: GoogleContactSource;
  displayName: string;
  givenName: string;
  familyName: string;
  emails: readonly string[];
  phones: readonly string[];
} {
  const names = recordArray(contact.names, "Contact names");
  const primaryName = names[0];
  const emails = recordArray(contact.emailAddresses, "Contact email addresses")
    .map((entry) => optionalString(entry.value) ?? "")
    .filter(Boolean);
  const phones = recordArray(contact.phoneNumbers, "Contact phone numbers")
    .map((entry) => optionalString(entry.value) ?? "")
    .filter(Boolean);
  return {
    resourceName: stringField(contact, "resourceName"),
    contactSource: contactSource(contact),
    displayName: primaryName === undefined ? "" : (optionalString(primaryName.displayName) ?? ""),
    givenName: primaryName === undefined ? "" : (optionalString(primaryName.givenName) ?? ""),
    familyName: primaryName === undefined ? "" : (optionalString(primaryName.familyName) ?? ""),
    emails,
    phones,
  };
}

function contactSearchText(contact: ReturnType<typeof normalizeContact>): string {
  return [contact.displayName, contact.givenName, contact.familyName, ...contact.emails, ...contact.phones]
    .join("\n")
    .toLocaleLowerCase();
}

function contactMatches(contact: ReturnType<typeof normalizeContact>, draft: ContactDraft): boolean {
  return (
    contact.givenName.trim().toLocaleLowerCase() === draft.givenName.trim().toLocaleLowerCase() &&
    contact.familyName.trim().toLocaleLowerCase() === draft.familyName.trim().toLocaleLowerCase() &&
    sameStringSet(contact.emails.map(normalizeEmail), draft.emails.map(normalizeEmail)) &&
    sameStringSet(contact.phones.map(normalizePhone), draft.phones.map(normalizePhone))
  );
}

async function docsGet(
  context: ExecutionContext,
  documentIdInput: string,
): Promise<GoogleWorkspaceJsonValue> {
  const documentId = identifier(documentIdInput, "Google Doc ID");
  const document = await docsFetch(context, documentId);
  const tabs = extractDocumentTabs(document);
  const firstTab = firstDocumentTab(tabs);
  const returnedDocumentId = stringField(document, "documentId");
  return {
    documentId: returnedDocumentId,
    title: stringField(document, "title"),
    body: firstTab.text,
    tabs: tabs.map((tab) => ({
      tabId: tab.tabId,
      title: tab.title,
      parentTabId: tab.parentTabId,
      index: tab.index,
      nestingLevel: tab.nestingLevel,
      body: tab.text,
      characterCount: tab.characterCount,
      truncated: tab.characterCount > tab.text.length,
    })),
    revisionId: optionalString(document.revisionId),
    url: `https://docs.google.com/document/d/${encodeURIComponent(returnedDocumentId)}/edit`,
  };
}

async function docsCreate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "docs_create" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const title = bounded(operation.title, "Google Doc title", 255);
  const body = operation.body === undefined ? null : bodyText(operation.body, "Google Doc body");
  const actionKey = stableActionKey(operation.idempotencyKey);
  let status: "created" | "already_done" = "already_done";
  let file = await findDriveActionFile(context, actionKey);
  if (file === null) {
    status = "created";
    file = await createDriveNativeFile(context, {
      name: title,
      mimeType: "application/vnd.google-apps.document",
      actionKey,
    });
  } else {
    assertDriveFileKind(file, "application/vnd.google-apps.document", "Google Doc");
  }
  const documentId = stringField(file, "id");
  if (body !== null && body.length > 0) await ensureInitialDocumentBody(context, documentId, body);
  const document = await docsGet(context, documentId);
  return { status, document };
}

async function docsAppend(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "docs_append" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const documentId = identifier(operation.documentId, "Google Doc ID");
  const supplied = bodyText(operation.text, "Google Doc append text");
  const text = supplied.endsWith("\n") ? supplied : `${supplied}\n`;
  const document = await docsFetch(context, documentId);
  const tabs = extractDocumentTabs(document);
  const requestedTabId = optionalIdentifier(operation.tabId, "Google Doc tab ID");
  const tab =
    requestedTabId === null
      ? firstDocumentTab(tabs)
      : tabs.find((candidate) => candidate.tabId === requestedTabId);
  if (tab === undefined) invalid("Google Doc tab ID was not found in the document");
  if (tab.tail.endsWith(text)) {
    return {
      status: "already_done",
      documentId,
      tabId: tab.tabId,
      tabTitle: tab.title,
      characters: text.length,
    };
  }
  const insertedAt = Math.max(tab.endIndex - 1, 1);
  await docsInsert(context, documentId, tab.tabId, text, insertedAt);
  return {
    status: "appended",
    documentId,
    tabId: tab.tabId,
    tabTitle: tab.title,
    insertedAt,
    characters: text.length,
  };
}

async function ensureInitialDocumentBody(
  context: ExecutionContext,
  documentId: string,
  body: string,
): Promise<void> {
  const desired = body.endsWith("\n") ? body : `${body}\n`;
  const document = await docsFetch(context, documentId);
  const tab = firstDocumentTab(extractDocumentTabs(document));
  if (tab.tail.endsWith(desired)) return;
  if (tab.text.trim().length > 0) {
    throw new GoogleWorkspaceError(
      "A replayed Google Doc creation found unexpected existing content",
      "reconciliation_failed",
      { service: "Docs" },
    );
  }
  await docsInsert(context, documentId, tab.tabId, desired, 1);
}

async function docsInsert(
  context: ExecutionContext,
  documentId: string,
  tabId: string,
  text: string,
  index: number,
): Promise<void> {
  await googleJson(context, "Docs", `${DOCS_API}/documents/${encodeURIComponent(documentId)}:batchUpdate`, {
    method: "POST",
    body: JSON.stringify({ requests: [{ insertText: { location: { index, tabId }, text } }] }),
  });
}

async function docsFetch(context: ExecutionContext, documentId: string): Promise<JsonRecord> {
  const params = new URLSearchParams({
    includeTabsContent: "true",
    suggestionsViewMode: "SUGGESTIONS_INLINE",
  });
  return googleJson(context, "Docs", `${DOCS_API}/documents/${encodeURIComponent(documentId)}?${params}`);
}

type ExtractedDocumentTab = Readonly<{
  tabId: string;
  title: string;
  parentTabId: string | null;
  index: number;
  nestingLevel: number;
  text: string;
  tail: string;
  characterCount: number;
  endIndex: number;
}>;

function extractDocumentTabs(document: JsonRecord): readonly ExtractedDocumentTab[] {
  const result: ExtractedDocumentTab[] = [];
  const visit = (tabs: readonly JsonRecord[], parentTabId: string | null, depth: number): void => {
    if (depth > 20) invalidResponse("Google Docs returned tabs nested too deeply", "Docs");
    for (const tab of tabs) {
      const properties = recordField(tab, "tabProperties");
      const tabId = stringField(properties, "tabId");
      const documentTab = recordField(tab, "documentTab");
      const extracted = extractDocumentBody(recordField(documentTab, "body"));
      result.push({
        tabId,
        title: stringField(properties, "title"),
        parentTabId: optionalString(properties.parentTabId) ?? parentTabId,
        index: optionalNumber(properties.index) ?? 0,
        nestingLevel: optionalNumber(properties.nestingLevel) ?? depth,
        ...extracted,
      });
      visit(recordArray(tab.childTabs, "Google Docs child tabs"), tabId, depth + 1);
    }
  };
  visit(recordArray(document.tabs, "Google Docs tabs"), null, 0);
  if (result.length === 0) invalidResponse("Google Docs returned no document tabs", "Docs");
  return result;
}

function firstDocumentTab(tabs: readonly ExtractedDocumentTab[]): ExtractedDocumentTab {
  const first = tabs[0];
  if (first === undefined) invalidResponse("Google Docs returned no first tab", "Docs");
  return first;
}

function extractDocumentBody(body: JsonRecord): {
  text: string;
  tail: string;
  characterCount: number;
  endIndex: number;
} {
  let endIndex = 1;
  const text: string[] = [];
  const visitStructuralElements = (elements: readonly JsonRecord[], depth: number): void => {
    if (depth > 20) return;
    for (const element of elements) {
      const candidateEnd = optionalNumber(element.endIndex);
      if (candidateEnd !== null) endIndex = Math.max(endIndex, candidateEnd);
      const paragraph = optionalRecord(element.paragraph);
      if (paragraph !== null) {
        for (const paragraphElement of recordArray(paragraph.elements, "Docs paragraph elements")) {
          const textRun = optionalRecord(paragraphElement.textRun);
          const content = textRun === null ? null : optionalString(textRun.content);
          if (content !== null) text.push(content);
        }
      }
      const table = optionalRecord(element.table);
      if (table !== null) {
        for (const row of recordArray(table.tableRows, "Docs table rows")) {
          for (const cell of recordArray(row.tableCells, "Docs table cells")) {
            visitStructuralElements(recordArray(cell.content, "Docs table cell content"), depth + 1);
          }
        }
      }
      const tableOfContents = optionalRecord(element.tableOfContents);
      if (tableOfContents !== null) {
        visitStructuralElements(recordArray(tableOfContents.content, "Docs table of contents"), depth + 1);
      }
    }
  };
  visitStructuralElements(recordArray(body.content, "Docs body content"), 0);
  const joined = text.join("");
  return {
    text: joined.slice(0, MAX_BODY_CHARACTERS),
    tail: joined.slice(-MAX_BODY_CHARACTERS),
    characterCount: joined.length,
    endIndex,
  };
}

async function sheetsGet(
  context: ExecutionContext,
  spreadsheetIdInput: string,
  rangeInput: string,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const spreadsheetId = identifier(spreadsheetIdInput, "Google Sheets spreadsheet ID");
  const range = bounded(rangeInput, "Google Sheets range", 1_000);
  const response = await sheetsValuesGet(context, spreadsheetId, range);
  return {
    spreadsheetId,
    range: optionalString(response.range) ?? range,
    majorDimension: optionalString(response.majorDimension) ?? "ROWS",
    values: sheetValues(response.values),
  };
}

async function sheetsCreate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "sheets_create" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const title = bounded(operation.title, "Google Sheets title", 255);
  const sheetName =
    operation.sheetName === undefined ? null : bounded(operation.sheetName, "Google Sheets tab name", 100);
  const actionKey = stableActionKey(operation.idempotencyKey);
  let status: "created" | "already_done" = "already_done";
  let file = await findDriveActionFile(context, actionKey);
  if (file === null) {
    status = "created";
    file = await createDriveNativeFile(context, {
      name: title,
      mimeType: "application/vnd.google-apps.spreadsheet",
      actionKey,
    });
  } else {
    assertDriveFileKind(file, "application/vnd.google-apps.spreadsheet", "Google Sheet");
  }
  const spreadsheetId = stringField(file, "id");
  if (sheetName !== null) await ensureSheetName(context, spreadsheetId, sheetName);
  const metadata = await googleJson(
    context,
    "Sheets",
    `${SHEETS_API}/spreadsheets/${encodeURIComponent(spreadsheetId)}?fields=spreadsheetId,properties(title),spreadsheetUrl,sheets(properties(sheetId,title,index))`,
  );
  return { status, spreadsheet: normalizeSpreadsheetMetadata(metadata) };
}

async function sheetsUpdate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "sheets_update" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const spreadsheetId = identifier(operation.spreadsheetId, "Google Sheets spreadsheet ID");
  const range = bounded(operation.range, "Google Sheets range", 1_000);
  const values = validateSheetValues(operation.values);
  const params = new URLSearchParams({ valueInputOption: "USER_ENTERED" });
  const response = await googleJson(
    context,
    "Sheets",
    `${SHEETS_API}/spreadsheets/${encodeURIComponent(spreadsheetId)}/values/${encodeURIComponent(range)}?${params}`,
    { method: "PUT", body: JSON.stringify({ range, majorDimension: "ROWS", values }) },
  );
  return {
    status: "updated",
    spreadsheetId,
    updatedRange: optionalString(response.updatedRange) ?? range,
    updatedRows: optionalNumber(response.updatedRows) ?? 0,
    updatedColumns: optionalNumber(response.updatedColumns) ?? 0,
    updatedCells: optionalNumber(response.updatedCells) ?? 0,
  };
}

async function sheetsAppend(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "sheets_append" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const spreadsheetId = identifier(operation.spreadsheetId, "Google Sheets spreadsheet ID");
  const range = bounded(operation.range, "Google Sheets range", 1_000);
  const values = validateSheetValues(operation.values);
  if (values.some((row) => normalizeSheetRow(row).length === 0)) {
    invalid("Google Sheets cannot append an entirely blank row reliably");
  }
  const current = sheetValues((await sheetsValuesGet(context, spreadsheetId, range)).values);
  if (sheetTailMatches(current, values)) {
    return { status: "already_done", spreadsheetId, appendedRows: values.length };
  }
  const params = new URLSearchParams({
    // Append retries reconcile against the exact values read back below. RAW keeps
    // formula-like, date-like, and numeric-looking strings stable across that round trip.
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
  });
  const response = await googleJson(
    context,
    "Sheets",
    `${SHEETS_API}/spreadsheets/${encodeURIComponent(spreadsheetId)}/values/${encodeURIComponent(range)}:append?${params}`,
    { method: "POST", body: JSON.stringify({ range, majorDimension: "ROWS", values }) },
  );
  const updates = optionalRecord(response.updates);
  if (updates === null) invalidResponse("Google Sheets append response is missing updates", "Sheets");
  const updatedRange = stringField(updates, "updatedRange");
  const written = sheetValues((await sheetsValuesGet(context, spreadsheetId, updatedRange)).values);
  if (!sheetTailMatches(written, values)) {
    throw new GoogleWorkspaceError(
      "Google Sheets did not read back the appended values exactly",
      "reconciliation_failed",
      { service: "Sheets" },
    );
  }
  return {
    status: "appended",
    spreadsheetId,
    updatedRange,
    updatedCells: optionalNumber(updates.updatedCells) ?? 0,
    appendedRows: values.length,
    valueInputOption: "RAW",
  };
}

async function sheetsValuesGet(
  context: ExecutionContext,
  spreadsheetId: string,
  range: string,
): Promise<JsonRecord> {
  const params = new URLSearchParams({ majorDimension: "ROWS", valueRenderOption: "UNFORMATTED_VALUE" });
  return googleJson(
    context,
    "Sheets",
    `${SHEETS_API}/spreadsheets/${encodeURIComponent(spreadsheetId)}/values/${encodeURIComponent(range)}?${params}`,
  );
}

async function ensureSheetName(
  context: ExecutionContext,
  spreadsheetId: string,
  desiredName: string,
): Promise<void> {
  const metadata = await googleJson(
    context,
    "Sheets",
    `${SHEETS_API}/spreadsheets/${encodeURIComponent(spreadsheetId)}?fields=sheets(properties(sheetId,title,index))`,
  );
  const sheets = recordArray(metadata.sheets, "Google Sheets tabs");
  const properties = sheets.map((sheet) => recordField(sheet, "properties"));
  if (properties.some((property) => optionalString(property.title) === desiredName)) return;
  if (properties.length !== 1) {
    throw new GoogleWorkspaceError(
      "A replayed spreadsheet creation found unexpected existing tabs",
      "reconciliation_failed",
      { service: "Sheets" },
    );
  }
  const sheetId = numberField(properties[0] as JsonRecord, "sheetId");
  await googleJson(
    context,
    "Sheets",
    `${SHEETS_API}/spreadsheets/${encodeURIComponent(spreadsheetId)}:batchUpdate`,
    {
      method: "POST",
      body: JSON.stringify({
        requests: [
          {
            updateSheetProperties: {
              properties: { sheetId, title: desiredName },
              fields: "title",
            },
          },
        ],
      }),
    },
  );
}

function normalizeSpreadsheetMetadata(metadata: JsonRecord): GoogleWorkspaceJsonValue {
  const sheets = recordArray(metadata.sheets, "Google Sheets tabs").map((sheet) => {
    const properties = recordField(sheet, "properties");
    return {
      sheetId: numberField(properties, "sheetId"),
      title: stringField(properties, "title"),
      index: optionalNumber(properties.index) ?? 0,
    };
  });
  return {
    spreadsheetId: stringField(metadata, "spreadsheetId"),
    title:
      optionalRecord(metadata.properties) === null
        ? ""
        : (optionalString((metadata.properties as JsonRecord).title) ?? ""),
    spreadsheetUrl: optionalString(metadata.spreadsheetUrl),
    sheets,
  };
}

function validateSheetValues(
  values: readonly (readonly GoogleSheetScalar[])[],
): readonly (readonly GoogleSheetScalar[])[] {
  if (!Array.isArray(values) || values.length === 0 || values.length > 1_000) {
    invalid("Google Sheets values must contain between 1 and 1,000 rows");
  }
  let cells = 0;
  const result = values.map((row, rowIndex) => {
    if (!Array.isArray(row) || row.length === 0 || row.length > 100) {
      invalid(`Google Sheets row ${rowIndex + 1} must contain between 1 and 100 cells`);
    }
    cells += row.length;
    return row.map((cell, columnIndex) => {
      if (cell === null || typeof cell === "number" || typeof cell === "boolean") {
        if (typeof cell === "number" && !Number.isFinite(cell)) {
          invalid(`Google Sheets cell ${rowIndex + 1}:${columnIndex + 1} is not finite`);
        }
        return cell;
      }
      if (typeof cell !== "string")
        invalid(`Google Sheets cell ${rowIndex + 1}:${columnIndex + 1} is invalid`);
      return boundedAllowEmpty(cell, `Google Sheets cell ${rowIndex + 1}:${columnIndex + 1}`, 50_000);
    });
  });
  if (cells > 10_000) invalid("Google Sheets write exceeds 10,000 cells");
  return result;
}

function sheetValues(value: unknown): readonly (readonly GoogleSheetScalar[])[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) invalidResponse("Google Sheets returned invalid values", "Sheets");
  return value.map((row, rowIndex) => {
    if (!Array.isArray(row))
      invalidResponse(`Google Sheets returned an invalid row ${rowIndex + 1}`, "Sheets");
    return row.map((cell) => {
      if (cell === null || typeof cell === "string" || typeof cell === "boolean") return cell;
      if (typeof cell === "number" && Number.isFinite(cell)) return cell;
      return invalidResponse("Google Sheets returned an invalid cell", "Sheets");
    });
  });
}

function sheetTailMatches(
  current: readonly (readonly GoogleSheetScalar[])[],
  expected: readonly (readonly GoogleSheetScalar[])[],
): boolean {
  if (current.length < expected.length) return false;
  const tail = current.slice(current.length - expected.length);
  return tail.every((row, index) => {
    const wanted = expected[index];
    return (
      wanted !== undefined &&
      JSON.stringify(normalizeSheetRow(row)) === JSON.stringify(normalizeSheetRow(wanted))
    );
  });
}

function normalizeSheetRow(row: readonly GoogleSheetScalar[]): readonly GoogleSheetScalar[] {
  let end = row.length;
  while (end > 0 && (row[end - 1] === null || row[end - 1] === "")) end -= 1;
  return row.slice(0, end).map((cell) => (cell === null ? "" : cell));
}

async function slidesGet(
  context: ExecutionContext,
  presentationIdInput: string,
): Promise<GoogleWorkspaceJsonValue> {
  const presentationId = identifier(presentationIdInput, "Google Slides presentation ID");
  const presentation = await googleJson(
    context,
    "Slides",
    `${SLIDES_API}/presentations/${encodeURIComponent(presentationId)}`,
  );
  return normalizePresentation(presentation);
}

async function slidesCreate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "slides_create" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const title = bounded(operation.title, "Google Slides title", 255);
  const actionKey = stableActionKey(operation.idempotencyKey);
  let status: "created" | "already_done" = "already_done";
  let file = await findDriveActionFile(context, actionKey);
  if (file === null) {
    status = "created";
    file = await createDriveNativeFile(context, {
      name: title,
      mimeType: "application/vnd.google-apps.presentation",
      actionKey,
    });
  } else {
    assertDriveFileKind(file, "application/vnd.google-apps.presentation", "Google Slides presentation");
  }
  const presentation = await slidesGet(context, stringField(file, "id"));
  return { status, presentation };
}

async function slidesAddTextSlide(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "slides_add_text_slide" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const presentationId = identifier(operation.presentationId, "Google Slides presentation ID");
  const title = bounded(operation.title, "Google Slides slide title", 5_000);
  const body = bodyText(operation.body, "Google Slides slide body");
  const presentation = await googleJson(
    context,
    "Slides",
    `${SLIDES_API}/presentations/${encodeURIComponent(presentationId)}`,
  );
  if (presentationHasTextSlide(presentation, title, body)) {
    return { status: "already_done", presentationId, title };
  }
  const identity = createHash("sha256")
    .update(`${presentationId}\u0000${title}\u0000${body}`)
    .digest("hex")
    .slice(0, 24);
  const slideId = `fl_slide_${identity}`;
  const titleId = `fl_title_${identity}`;
  const bodyId = `fl_body_${identity}`;
  await googleJson(
    context,
    "Slides",
    `${SLIDES_API}/presentations/${encodeURIComponent(presentationId)}:batchUpdate`,
    {
      method: "POST",
      body: JSON.stringify({
        requests: [
          { createSlide: { objectId: slideId, slideLayoutReference: { predefinedLayout: "BLANK" } } },
          {
            createShape: {
              objectId: titleId,
              shapeType: "TEXT_BOX",
              elementProperties: slideElementProperties(slideId, 42, 30, 636, 64),
            },
          },
          { insertText: { objectId: titleId, insertionIndex: 0, text: title } },
          {
            createShape: {
              objectId: bodyId,
              shapeType: "TEXT_BOX",
              elementProperties: slideElementProperties(slideId, 42, 112, 636, 390),
            },
          },
          { insertText: { objectId: bodyId, insertionIndex: 0, text: body } },
        ],
      }),
    },
  );
  return { status: "created", presentationId, slideId, title };
}

function slideElementProperties(
  pageObjectId: string,
  x: number,
  y: number,
  width: number,
  height: number,
): JsonRecord {
  return {
    pageObjectId,
    size: {
      width: { magnitude: width, unit: "PT" },
      height: { magnitude: height, unit: "PT" },
    },
    transform: {
      scaleX: 1,
      scaleY: 1,
      translateX: x,
      translateY: y,
      unit: "PT",
    },
  };
}

function normalizePresentation(presentation: JsonRecord): GoogleWorkspaceJsonValue {
  const slides = recordArray(presentation.slides, "Google Slides slides").map((slide, index) => ({
    slideId: stringField(slide, "objectId"),
    index,
    text: slideTextSegments(slide).join("\n"),
  }));
  const pageSize = optionalRecord(presentation.pageSize);
  return {
    presentationId: stringField(presentation, "presentationId"),
    title: stringField(presentation, "title"),
    revisionId: optionalString(presentation.revisionId),
    presentationUrl: `https://docs.google.com/presentation/d/${encodeURIComponent(stringField(presentation, "presentationId"))}/edit`,
    pageSize:
      pageSize === null
        ? null
        : {
            width: dimensionMagnitude(pageSize.width),
            height: dimensionMagnitude(pageSize.height),
          },
    slides,
  };
}

function presentationHasTextSlide(presentation: JsonRecord, title: string, body: string): boolean {
  return recordArray(presentation.slides, "Google Slides slides").some((slide) => {
    const segments = slideTextSegments(slide)
      .map((segment) => segment.trim())
      .filter(Boolean);
    return segments.includes(title.trim()) && segments.includes(body.trim());
  });
}

function slideTextSegments(slide: JsonRecord): readonly string[] {
  const result: string[] = [];
  for (const element of recordArray(slide.pageElements, "Google Slides page elements")) {
    const shape = optionalRecord(element.shape);
    const text = shape === null ? null : optionalRecord(shape.text);
    if (text === null) continue;
    const content = recordArray(text.textElements, "Google Slides text elements")
      .map((textElement) => {
        const textRun = optionalRecord(textElement.textRun);
        return textRun === null ? "" : (optionalString(textRun.content) ?? "");
      })
      .join("");
    if (content) result.push(content.slice(0, MAX_BODY_CHARACTERS));
  }
  return result;
}

function dimensionMagnitude(value: unknown): number | null {
  const dimension = optionalRecord(value);
  return dimension === null ? null : optionalNumber(dimension.magnitude);
}

async function tasksList(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "tasks_list" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const taskListId = taskListIdentifier(operation.taskListId);
  const limit = boundedInteger(operation.limit, "Google Tasks list limit", 1, 100);
  const tasks = await listTasks(context, taskListId, operation.showCompleted, limit);
  return { taskListId, tasks: tasks.map(normalizeTask) };
}

async function tasklistsList(
  context: ExecutionContext,
): Promise<{ readonly taskLists: GoogleWorkspaceJsonValue }> {
  const taskLists = await paginatedRecords(context, {
    service: "Tasks",
    url: `${TASKS_API}/users/@me/lists`,
    itemField: "items",
    limit: 2_000,
    requireComplete: true,
    params: { maxResults: "100" },
  });
  return {
    taskLists: taskLists.map((taskList) => ({
      taskListId: stringField(taskList, "id"),
      title: stringField(taskList, "title"),
      updatedAt: optionalString(taskList.updated),
    })),
  };
}

async function tasksCreate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "tasks_create" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const taskListId = taskListIdentifier(operation.taskListId);
  const title = bounded(operation.title, "Google Task title", 1_024);
  const notes =
    operation.notes === undefined ? "" : boundedAllowEmpty(operation.notes, "Google Task notes", 50_000);
  const due = operation.due === undefined ? null : taskDue(operation.due);
  const tasks = await listTasks(context, taskListId, true, 2_000);
  const matches = tasks.filter((task) => taskMatches(task, { title, notes, due, status: "needsAction" }));
  if (matches.length > 1) {
    throw new GoogleWorkspaceError(
      "Google Tasks contains more than one exact match for this task",
      "reconciliation_failed",
      { service: "Tasks" },
    );
  }
  if (matches[0] !== undefined) return { status: "already_done", task: normalizeTask(matches[0]) };
  const response = await googleJson(
    context,
    "Tasks",
    `${TASKS_API}/lists/${encodeURIComponent(taskListId)}/tasks`,
    {
      method: "POST",
      body: JSON.stringify({ title, ...(notes ? { notes } : {}), ...(due === null ? {} : { due }) }),
    },
  );
  return { status: "created", task: normalizeTask(response) };
}

async function tasksUpdate(
  context: ExecutionContext,
  operation: Extract<GoogleWorkspaceOperation, { operation: "tasks_update" }>,
): Promise<{ readonly [key: string]: GoogleWorkspaceJsonValue }> {
  const taskListId = taskListIdentifier(operation.taskListId);
  const taskId = identifier(operation.taskId, "Google Task ID");
  const patch: Record<string, string> = {};
  if (operation.title !== undefined) patch.title = bounded(operation.title, "Google Task title", 1_024);
  if (operation.notes !== undefined)
    patch.notes = boundedAllowEmpty(operation.notes, "Google Task notes", 50_000);
  if (operation.due !== undefined) patch.due = taskDue(operation.due);
  if (operation.status !== undefined) patch.status = operation.status;
  if (Object.keys(patch).length === 0) invalid("Google Task update requires at least one field");
  const response = await googleJson(
    context,
    "Tasks",
    `${TASKS_API}/lists/${encodeURIComponent(taskListId)}/tasks/${encodeURIComponent(taskId)}`,
    { method: "PATCH", body: JSON.stringify(patch) },
  );
  return { status: "updated", task: normalizeTask(response) };
}

async function listTasks(
  context: ExecutionContext,
  taskListId: string,
  showCompleted: boolean,
  limit: number,
): Promise<readonly JsonRecord[]> {
  return paginatedRecords(context, {
    service: "Tasks",
    url: `${TASKS_API}/lists/${encodeURIComponent(taskListId)}/tasks`,
    itemField: "items",
    limit,
    requireComplete: limit > 100,
    params: {
      maxResults: String(Math.min(limit, 100)),
      showCompleted: String(showCompleted),
      showDeleted: "false",
      showHidden: String(showCompleted),
    },
  });
}

function normalizeTask(task: JsonRecord): GoogleWorkspaceJsonValue {
  return {
    taskId: stringField(task, "id"),
    title: optionalString(task.title) ?? "",
    notes: optionalString(task.notes) ?? "",
    status: optionalString(task.status) ?? "needsAction",
    due: optionalString(task.due),
    completedAt: optionalString(task.completed),
    updatedAt: optionalString(task.updated),
    parentTaskId: optionalString(task.parent),
    position: optionalString(task.position),
    webViewLink: optionalString(task.webViewLink),
  };
}

function taskMatches(
  task: JsonRecord,
  expected: { title: string; notes: string; due: string | null; status: string },
): boolean {
  return (
    (optionalString(task.title) ?? "") === expected.title &&
    (optionalString(task.notes) ?? "") === expected.notes &&
    (optionalString(task.due) ?? null) === expected.due &&
    (optionalString(task.status) ?? "needsAction") === expected.status
  );
}

function taskListIdentifier(value: string | undefined): string {
  return value === undefined ? "@default" : identifier(value, "Google Task list ID");
}

function taskDue(value: string): string {
  const input = bounded(value, "Google Task due date", 100);
  const candidate = /^\d{4}-\d{2}-\d{2}$/.test(input) ? `${input}T00:00:00.000Z` : input;
  const parsed = new Date(candidate);
  if (!Number.isFinite(parsed.getTime())) invalid("Google Task due date must be ISO 8601");
  const day = parsed.toISOString().slice(0, 10);
  return `${day}T00:00:00.000Z`;
}

async function paginatedRecords(
  context: ExecutionContext,
  input: {
    service: string;
    url: string;
    itemField: string;
    limit: number;
    params: Readonly<Record<string, string>>;
    requireComplete?: boolean;
  },
): Promise<readonly JsonRecord[]> {
  if (!Number.isInteger(input.limit) || input.limit < 1) invalid("Google pagination limit is invalid");
  const items: JsonRecord[] = [];
  const seenTokens = new Set<string>();
  let pageToken: string | null = null;
  for (let page = 0; page < MAX_PAGES && items.length < input.limit; page += 1) {
    const url = new URL(input.url);
    for (const [name, value] of Object.entries(input.params)) url.searchParams.set(name, value);
    if (pageToken !== null) url.searchParams.set("pageToken", pageToken);
    const response = await googleJson(context, input.service, url.toString());
    const pageItems = recordArray(response[input.itemField], `${input.service} ${input.itemField}`);
    items.push(...pageItems.slice(0, input.limit - items.length));
    const next = optionalString(response.nextPageToken);
    if (next === null) return items;
    if (next.length > 16_384 || seenTokens.has(next)) {
      invalidResponse(`${input.service} returned an invalid pagination token`, input.service);
    }
    seenTokens.add(next);
    pageToken = next;
  }
  if (input.requireComplete && pageToken !== null) {
    throw new GoogleWorkspaceError(
      `${input.service} pagination exceeded Florence's bounded scan`,
      "reconciliation_failed",
      { service: input.service },
    );
  }
  return items;
}

async function googleJson(
  context: ExecutionContext,
  service: string,
  url: string,
  init: { method?: "GET" | "POST" | "PUT" | "PATCH"; body?: string } = {},
): Promise<JsonRecord> {
  const signal =
    context.signal === undefined
      ? AbortSignal.timeout(REQUEST_TIMEOUT_MS)
      : AbortSignal.any([context.signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]);
  let response: Response;
  try {
    response = await context.fetch(url, {
      method: init.method ?? "GET",
      headers: {
        authorization: `Bearer ${context.accessToken}`,
        accept: "application/json",
        ...(init.body === undefined ? {} : { "content-type": "application/json; charset=utf-8" }),
      },
      ...(init.body === undefined ? {} : { body: init.body }),
      signal,
    });
  } catch (cause) {
    if (cause instanceof GoogleWorkspaceError) throw cause;
    throw new GoogleWorkspaceError(
      googleTransportFailureMessage(context, signal, service, "request"),
      "provider_unavailable",
      { service, cause },
    );
  }

  let text: string;
  try {
    text = await response.text();
  } catch (cause) {
    throw new GoogleWorkspaceError(
      googleTransportFailureMessage(context, signal, service, "response"),
      "provider_unavailable",
      { service, cause },
    );
  }
  let payload: unknown = null;
  if (text) {
    try {
      payload = JSON.parse(text) as unknown;
    } catch {
      if (response.ok) invalidResponse(`${service} returned invalid JSON`, service);
    }
  }
  if (!response.ok) {
    const message = googleErrorMessage(payload) ?? `${service} rejected the request`;
    const code: GoogleWorkspaceErrorCode =
      response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500
        ? "provider_unavailable"
        : "provider_rejected";
    throw new GoogleWorkspaceError(message, code, { service, status: response.status });
  }
  if (response.status === 204 || !text) return {};
  if (!isRecord(payload)) invalidResponse(`${service} returned an invalid response`, service);
  return payload;
}

async function googleBytes(
  context: ExecutionContext,
  service: string,
  url: string,
  maximumBytes: number,
): Promise<Buffer> {
  const signal =
    context.signal === undefined
      ? AbortSignal.timeout(REQUEST_TIMEOUT_MS)
      : AbortSignal.any([context.signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]);
  let response: Response;
  try {
    response = await context.fetch(url, {
      headers: {
        authorization: `Bearer ${context.accessToken}`,
        accept: "*/*",
      },
      signal,
    });
  } catch (cause) {
    throw new GoogleWorkspaceError(
      googleTransportFailureMessage(context, signal, service, "request"),
      "provider_unavailable",
      { service, cause },
    );
  }

  if (!response.ok) {
    let payload: unknown = null;
    try {
      const text = await response.text();
      if (text) payload = JSON.parse(text) as unknown;
    } catch {
      // Google occasionally returns a non-JSON intermediary error page.
    }
    const message = googleErrorMessage(payload) ?? `${service} rejected the request`;
    const code: GoogleWorkspaceErrorCode =
      response.status === 408 || response.status === 425 || response.status === 429 || response.status >= 500
        ? "provider_unavailable"
        : "provider_rejected";
    throw new GoogleWorkspaceError(message, code, { service, status: response.status });
  }

  const contentLength = response.headers.get("content-length");
  if (contentLength !== null) {
    const parsed = Number(contentLength);
    if (Number.isFinite(parsed) && parsed > maximumBytes) {
      throw new GoogleWorkspaceError(
        `${service} attachment exceeds ${maximumBytes} bytes`,
        "provider_rejected",
        { service },
      );
    }
  }
  let bytes: Buffer;
  try {
    bytes = Buffer.from(await response.arrayBuffer());
  } catch (cause) {
    throw new GoogleWorkspaceError(
      googleTransportFailureMessage(context, signal, service, "response"),
      "provider_unavailable",
      { service, cause },
    );
  }
  if (bytes.byteLength > maximumBytes) {
    throw new GoogleWorkspaceError(
      `${service} attachment exceeds ${maximumBytes} bytes`,
      "provider_rejected",
      { service },
    );
  }
  return bytes;
}

function googleTransportFailureMessage(
  context: ExecutionContext,
  signal: AbortSignal,
  service: string,
  phase: "request" | "response",
): string {
  if (context.signal?.aborted) return `${service} request was cancelled`;
  if (signal.aborted) return `${service} request timed out after ${REQUEST_TIMEOUT_MS} ms`;
  return phase === "request"
    ? `${service} request could not be completed`
    : `${service} response could not be read`;
}

function googleErrorMessage(value: unknown): string | null {
  if (!isRecord(value)) return null;
  const error = optionalRecord(value.error);
  if (error === null) return optionalString(value.error_description);
  const message = optionalString(error.message);
  if (message === null) return null;
  return message.slice(0, 2_000);
}

function invalid(message: string): never {
  throw new GoogleWorkspaceError(message, "invalid_input");
}

function invalidResponse(message: string, service: string): never {
  throw new GoogleWorkspaceError(message, "invalid_response", { service });
}

function bounded(value: unknown, label: string, maximum: number): string {
  if (typeof value !== "string") invalid(`${label} must be text`);
  const result = value.trim();
  if (!result) invalid(`${label} is required`);
  if (result.length > maximum) invalid(`${label} exceeds ${maximum} characters`);
  if (hasUnsupportedControlCharacter(result)) {
    invalid(`${label} contains unsupported control characters`);
  }
  return result;
}

function boundedAllowEmpty(value: unknown, label: string, maximum: number): string {
  if (typeof value !== "string") invalid(`${label} must be text`);
  if (value.length > maximum) invalid(`${label} exceeds ${maximum} characters`);
  if (hasUnsupportedControlCharacter(value)) {
    invalid(`${label} contains unsupported control characters`);
  }
  return value;
}

function bodyText(value: unknown, label: string): string {
  const result = boundedAllowEmpty(value, label, MAX_BODY_CHARACTERS);
  if (!result) invalid(`${label} is required`);
  return result;
}

function headerText(value: unknown, label: string, maximum: number, allowEmpty: boolean): string {
  const result = boundedAllowEmpty(value, label, maximum).trim();
  if (!allowEmpty && !result) invalid(`${label} is required`);
  if (/\r|\n/.test(result)) invalid(`${label} cannot contain a line break`);
  return result;
}

function identifier(value: unknown, label: string): string {
  const result = bounded(value, label, 2_048);
  if (/\s/.test(result)) invalid(`${label} cannot contain whitespace`);
  return result;
}

function optionalIdentifier(value: unknown, label: string): string | null {
  return value === undefined ? null : identifier(value, label);
}

function boundedInteger(value: unknown, label: string, minimum: number, maximum: number): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < minimum || value > maximum) {
    invalid(`${label} must be an integer from ${minimum} to ${maximum}`);
  }
  return value;
}

function emailAddress(value: unknown, label: string): string {
  const result = headerText(value, label, 320, false);
  if (!/^[^\s@<>(),;:"[\]]+@[^\s@<>(),;:"[\]]+\.[^\s@<>(),;:"[\]]+$/.test(result)) {
    invalid(`${label} must be an email address`);
  }
  return result;
}

function mailboxFromHeader(value: unknown, label: string): string {
  const header = headerText(value, label, 2_000, false);
  const angleAddress = /<([^<>]+)>/.exec(header)?.[1];
  const firstMailbox = (angleAddress ?? header.split(",", 1)[0] ?? "").trim();
  return emailAddress(firstMailbox, label);
}

function emailList(value: readonly string[], label: string, required: boolean): readonly string[] {
  if (!Array.isArray(value) || value.length > 100 || (required && value.length === 0)) {
    invalid(`${label} is invalid`);
  }
  return uniqueStrings(
    value.map((email) => emailAddress(email, label)),
    label,
    100,
  );
}

function normalizeEmail(value: string): string {
  return value.trim().toLocaleLowerCase();
}

function normalizePhone(value: string): string {
  return value.replace(/[^0-9+]/g, "");
}

function domainName(value: unknown): string {
  const result = bounded(value, "Drive permission domain", 253).toLocaleLowerCase();
  if (!/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/.test(result)) {
    invalid("Drive permission domain is invalid");
  }
  return result;
}

function uniqueIdentifiers(values: readonly string[], label: string, maximum: number): readonly string[] {
  if (!Array.isArray(values)) invalid(`${label} must be an array`);
  return uniqueStrings(
    values.map((value) => identifier(value, label)),
    label,
    maximum,
  );
}

function uniqueStrings(values: readonly string[], label: string, maximum: number): readonly string[] {
  if (values.length > maximum) invalid(`${label} exceeds ${maximum} items`);
  const result = [...new Set(values)];
  if (result.length !== values.length) invalid(`${label} contains duplicates`);
  return result;
}

function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) return false;
  const sortedLeft = [...left].sort();
  const sortedRight = [...right].sort();
  return sortedLeft.every((value, index) => value === sortedRight[index]);
}

function stableActionKey(value: unknown): string {
  const input = bounded(value, "Workspace idempotency key", 500);
  return createHash("sha256").update(input, "utf8").digest("hex");
}

function deterministicMessageId(actionKey: string): string {
  return `<florence-${actionKey}@actions.florence.invalid>`;
}

function florenceMessageHeaderId(value: unknown): string {
  const result = headerText(value, "Gmail Florence Message-ID", 200, false);
  if (!/^<florence-[a-f0-9]{64}@actions\.florence\.invalid>$/.test(result)) {
    invalid("Gmail Florence Message-ID is invalid");
  }
  return result;
}

function encodedHeader(value: string): string {
  if (/^[\x20-\x7E]*$/.test(value)) return value;
  return `=?UTF-8?B?${Buffer.from(value, "utf8").toString("base64")}?=`;
}

function optionalHeaderValue(value: string | undefined, label: string): string | null {
  if (value === undefined) return null;
  return headerText(value, label, 10_000, false);
}

function escapeDriveQuery(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function decodeBase64Url(value: string): string {
  if (value.length > 4 * MAX_BODY_CHARACTERS) invalidResponse("Gmail message body is too large", "Gmail");
  try {
    return Buffer.from(value, "base64url").toString("utf8");
  } catch (cause) {
    throw new GoogleWorkspaceError("Gmail returned an invalid message body", "invalid_response", {
      service: "Gmail",
      cause,
    });
  }
}

function decodeGmailAttachment(value: string, expectedBytes: number): Buffer {
  if (!/^[A-Za-z0-9_-]*={0,2}$/.test(value) || value.length > MAX_GMAIL_DRAFT_ATTACHMENT_BYTES * 2) {
    invalidResponse("Gmail returned invalid attachment encoding", "Gmail");
  }
  let bytes: Buffer;
  try {
    bytes = Buffer.from(value, "base64url");
  } catch (cause) {
    throw new GoogleWorkspaceError("Gmail returned invalid attachment encoding", "invalid_response", {
      service: "Gmail",
      cause,
    });
  }
  if (bytes.byteLength !== expectedBytes) {
    invalidResponse("Gmail attachment size changed before drafting", "Gmail");
  }
  if (bytes.byteLength > MAX_GMAIL_DRAFT_ATTACHMENT_BYTES) {
    throw new GoogleWorkspaceError(
      `Gmail attachment exceeds ${MAX_GMAIL_DRAFT_ATTACHMENT_BYTES} bytes`,
      "provider_rejected",
      { service: "Gmail" },
    );
  }
  return bytes;
}

function providerAttachmentFilename(value: string, service: string): string {
  if (
    !value ||
    value.length > 240 ||
    value.includes("\r") ||
    value.includes("\n") ||
    hasUnsupportedControlCharacter(value)
  ) {
    invalidResponse(`${service} returned an invalid attachment filename`, service);
  }
  return value;
}

function mimeType(value: string, label: string): string {
  const result = value.toLowerCase();
  if (
    result.length > 200 ||
    !/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/i.test(result) ||
    /[\r\n]/.test(result)
  ) {
    invalidResponse(`${label} is invalid`, label.startsWith("Drive") ? "Drive" : "Gmail");
  }
  return result;
}

function nonNegativeInteger(value: unknown, label: string, service: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    invalidResponse(`${label} is invalid`, service);
  }
  return value;
}

function optionalDriveFileSize(value: unknown): number | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    invalidResponse("Drive attachment size is invalid", "Drive");
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) invalidResponse("Drive attachment size is invalid", "Drive");
  return parsed;
}

function htmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function optionalRecord(value: unknown): JsonRecord | null {
  if (value === undefined || value === null) return null;
  if (!isRecord(value)) invalidResponse("Google returned an invalid object", "Google");
  return value;
}

function recordField(value: JsonRecord, field: string): JsonRecord {
  const result = value[field];
  if (!isRecord(result)) invalidResponse(`Google response is missing ${field}`, "Google");
  return result;
}

function recordArray(value: unknown, label: string): readonly JsonRecord[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some((item) => !isRecord(item))) {
    invalidResponse(`${label} is invalid`, "Google");
  }
  return value as JsonRecord[];
}

function stringArray(value: unknown, label: string): readonly string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    invalidResponse(`${label} is invalid`, "Google");
  }
  return value as string[];
}

function stringField(value: JsonRecord, field: string): string {
  const result = value[field];
  if (typeof result !== "string" || !result) invalidResponse(`Google response is missing ${field}`, "Google");
  return result;
}

function numberField(value: JsonRecord, field: string): number {
  const result = value[field];
  if (typeof result !== "number" || !Number.isFinite(result)) {
    invalidResponse(`Google response is missing ${field}`, "Google");
  }
  return result;
}

function optionalString(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") invalidResponse("Google returned invalid text", "Google");
  return value;
}

function optionalNumber(value: unknown): number | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    invalidResponse("Google returned an invalid number", "Google");
  }
  return value;
}

function optionalBoolean(value: unknown): boolean | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "boolean") invalidResponse("Google returned an invalid boolean", "Google");
  return value;
}

function hasUnsupportedControlCharacter(value: string): boolean {
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (
      (code >= 0 && code <= 8) ||
      code === 11 ||
      code === 12 ||
      (code >= 14 && code <= 31) ||
      code === 127
    ) {
      return true;
    }
  }
  return false;
}
