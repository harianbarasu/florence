import type { FastifyInstance } from "fastify";
import { type CreateFlorenceHttpServerOptions, createFlorenceHttpServer } from "./http/index.js";

export interface StartFlorenceServerOptions extends CreateFlorenceHttpServerOptions {
  host?: string;
  port: number;
}

/**
 * Production composition roots inject application services here. This module
 * owns HTTP lifecycle only; it does not construct domain, database, or provider
 * implementations.
 */
export async function startFlorenceServer(options: StartFlorenceServerOptions): Promise<FastifyInstance> {
  const server = await createFlorenceHttpServer(options);
  try {
    await server.listen({
      host: options.host ?? "0.0.0.0",
      port: options.port,
    });
    return server;
  } catch (error) {
    await server.close();
    throw error;
  }
}
