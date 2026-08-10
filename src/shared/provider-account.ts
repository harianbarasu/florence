export function providerAccountOwnershipLockKey(provider: string, subjectDigest: string): string {
  return `provider-account:${provider}:${subjectDigest}`;
}
