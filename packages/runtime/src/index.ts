import {
  type ImageReference,
  type WorkerInput,
  type WorkerProposal,
  workerInputSchema,
  workerResultSchema,
} from "@florence/contracts";
import { renderWorkerEvidence, WORKER_INSTRUCTIONS } from "./prompt.js";

export interface WorkerRuntime {
  deliberate(input: WorkerInput): Promise<readonly WorkerProposal[]>;
}

export type WorkerScript = (
  input: WorkerInput,
) => readonly WorkerProposal[] | Promise<readonly WorkerProposal[]>;

export class ScriptedWorkerRuntime implements WorkerRuntime {
  readonly #script: WorkerScript;

  constructor(script: WorkerScript) {
    this.#script = script;
  }

  async deliberate(input: WorkerInput): Promise<readonly WorkerProposal[]> {
    return this.#script(input);
  }
}

export type SupportedImageMimeType = "image/jpeg" | "image/png" | "image/webp";

export type ModelContent =
  | { type: "text"; text: string }
  | { type: "image"; mimeType: SupportedImageMimeType; bytes: Uint8Array };

export type ModelRequest = {
  instructions: string;
  content: readonly ModelContent[];
};

export interface ModelGateway {
  generate(request: ModelRequest): Promise<unknown>;
}

export type WorkerRuntimeErrorCategory =
  | "rate_limited"
  | "context_exceeded"
  | "unsupported_capability"
  | "invalid_output"
  | "transient"
  | "permanent";

export class WorkerRuntimeError extends Error {
  readonly retryable: boolean;

  constructor(
    readonly category: WorkerRuntimeErrorCategory,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "WorkerRuntimeError";
    this.retryable = category === "rate_limited" || category === "transient";
  }
}

export type ResolvedImage = {
  mimeType: SupportedImageMimeType;
  bytes: Uint8Array;
};

export interface ImageAssetReader {
  read(input: { householdId: string; signalId: string; image: ImageReference }): Promise<ResolvedImage>;
}

const maximumImageBytes = 20 * 1024 * 1024;
const supportedImageTypes = new Set<SupportedImageMimeType>(["image/jpeg", "image/png", "image/webp"]);

export class GatewayWorkerRuntime implements WorkerRuntime {
  constructor(
    private readonly gateway: ModelGateway,
    private readonly imageReader: ImageAssetReader | null = null,
  ) {}

  async deliberate(untrustedInput: WorkerInput): Promise<readonly WorkerProposal[]> {
    const input = workerInputSchema.parse(untrustedInput);
    const imageReferences = input.signal.type === "conversation.message" ? input.signal.images : [];
    const images = await this.resolveImages(input, imageReferences);
    const result = await this.gateway.generate({
      instructions: WORKER_INSTRUCTIONS,
      content: [
        { type: "text", text: renderWorkerEvidence(input) },
        ...images.map((image) => ({ type: "image" as const, ...image })),
      ],
    });
    const parsed = workerResultSchema.safeParse(result);
    if (!parsed.success) {
      throw new WorkerRuntimeError("invalid_output", "The model returned an invalid worker result");
    }
    return parsed.data.proposals;
  }

  private async resolveImages(
    input: WorkerInput,
    references: readonly ImageReference[],
  ): Promise<ResolvedImage[]> {
    if (references.length === 0) return [];
    if (!this.imageReader) {
      throw new WorkerRuntimeError("unsupported_capability", "No authorized image reader is configured");
    }
    const reader = this.imageReader;
    let images: ResolvedImage[];
    try {
      images = await Promise.all(
        references.map((image) =>
          reader.read({
            householdId: input.snapshot.householdId,
            signalId: input.signal.signalId,
            image,
          }),
        ),
      );
    } catch (error) {
      if (error instanceof WorkerRuntimeError) throw error;
      if (isExplicitlyNonRetryable(error)) {
        throw new WorkerRuntimeError("unsupported_capability", "An authorized image cannot be processed", {
          cause: error,
        });
      }
      throw new WorkerRuntimeError("transient", "Unable to read an authorized image", { cause: error });
    }
    for (const image of images) {
      if (!supportedImageTypes.has(image.mimeType) || image.bytes.length === 0) {
        throw new WorkerRuntimeError("unsupported_capability", "Image normalization is unsupported");
      }
      if (image.bytes.length > maximumImageBytes) {
        throw new WorkerRuntimeError("unsupported_capability", "An image exceeds Florence's size limit");
      }
    }
    return images;
  }
}

function isExplicitlyNonRetryable(error: unknown): boolean {
  return typeof error === "object" && error !== null && "retryable" in error && error.retryable === false;
}
