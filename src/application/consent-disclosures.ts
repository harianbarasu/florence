export const CONSENT_DISCLOSURE_VERSION = 1 as const;

const FLORENCE_CONSENT_DISCLOSURE =
  "Florence is a proactive family Chief of Staff in iMessage. It stores the family plan and conversation-derived coordination details, and—if you connect Google—privately reviews your mail and calendar for family obligations. Your private-source content stays private unless you approve the minimum family meaning or a narrow future-sharing rule. Florence may send useful reminders, private urgent notices, and a daily family brief, but it will ask before external actions such as sending, booking, purchasing, cancelling, or writing to a calendar. You can correct or revoke rules and connections, export or delete your data, or text STOP at any time.";

export const FOUNDING_ADULT_CONSENT_DISCLOSURE = `${FLORENCE_CONSENT_DISCLOSURE} If you agree to Florence processing this information for family coordination, use iMessage’s Reply on this Florence message and say exactly “I consent.”`;

export const INVITED_ADULT_CONSENT_DISCLOSURE = `You were invited to share a Florence family. ${FLORENCE_CONSENT_DISCLOSURE} If you agree to join and to Florence processing this information for family coordination, use iMessage’s Reply on this Florence message and say exactly “I accept.”`;

export const TRANSFER_ADULT_CONSENT_DISCLOSURE = `You were invited to share a different Florence family. ${FLORENCE_CONSENT_DISCLOSURE} This transfer will not merge or copy messages, mail, calendars, or history. Existing household history stays where it is, but this iMessage identity will stop accessing its current household. If you agree to join, to Florence processing this information for family coordination, and to transfer this identity, use iMessage’s Reply on this Florence message and send exactly “I accept and confirm transfer.” To stay with the current household, reply “I decline transfer.”`;
