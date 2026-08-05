import { google } from "googleapis";
import type { GoogleAdapterConfig } from "./config.js";

export type GoogleAccessTokenAuth = InstanceType<typeof google.auth.OAuth2>;

export function googleAuthWithAccessToken(
  config: Pick<GoogleAdapterConfig, "clientId" | "clientSecret" | "redirectUri">,
  accessToken: string,
): GoogleAccessTokenAuth {
  const auth = new google.auth.OAuth2(config.clientId, config.clientSecret, config.redirectUri);
  auth.setCredentials({ access_token: accessToken });
  return auth;
}
