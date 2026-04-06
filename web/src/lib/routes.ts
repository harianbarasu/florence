export function withToken(path: string, token?: string) {
  if (!token) {
    return path;
  }

  const url = new URL(path, "http://florence.local");
  url.searchParams.set("token", token);
  return `${url.pathname}${url.search}${url.hash}`;
}
