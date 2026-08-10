export type GoogleCapability = "mail" | "calendar";
export type GoogleConnectionProfile = "personal_family" | "work";

export interface GoogleCredentials {
  accessToken?: string;
  refreshToken?: string;
  expiryDate?: number;
  scope?: string;
  tokenType?: string;
}

export interface GoogleTokenExchange {
  credentials: GoogleCredentials;
  subject: string;
  email: string;
  grantedScopes: readonly string[];
  grantedCapabilities: readonly GoogleCapability[];
}

export interface GoogleTokenRevocationReceipt {
  readonly outcome: "revoked" | "already_invalid" | "no_token";
  readonly httpStatus: number;
}

export interface GmailAttachmentReference {
  attachmentId: string | null;
  bodyData: string | null;
  partId: string;
  filename: string;
  mimeType: string;
  size: number;
  inline: boolean;
  contentId?: string;
}

export interface NormalizedGmailMessage {
  id: string;
  threadId: string;
  historyId: string;
  internalDate: Date;
  labelIds: readonly string[];
  from: string | null;
  to: readonly string[];
  cc: readonly string[];
  subject: string | null;
  messageIdHeader: string | null;
  text: string;
  html: string | null;
  attachments: readonly GmailAttachmentReference[];
  hasAttachmentHint: boolean;
  snippet: string;
}

export interface NormalizedCalendar {
  id: string;
  summary: string;
  primary: boolean;
  accessRole: string;
  timezone: string | null;
  deleted: boolean;
}

export interface NormalizedCalendarEvent {
  id: string;
  calendarId: string;
  etag: string | null;
  status: string;
  summary: string | null;
  description: string | null;
  location: string | null;
  start: string;
  end: string;
  timezone: string | null;
  recurringEventId: string | null;
  updatedAt: Date | null;
  attendees: readonly { email: string; responseStatus: string | null; self: boolean }[];
}
