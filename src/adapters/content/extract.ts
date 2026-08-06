import { parse as parseCsv } from "csv-parse/sync";
import { fileTypeFromBuffer } from "file-type";
import { htmlToText } from "html-to-text";
import ICAL from "ical.js";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";

export interface ExtractedDocument {
  detectedMime: string;
  kind: "pdf" | "image" | "text" | "csv" | "ics";
  text: string;
  metadata: Readonly<Record<string, string | number | boolean>>;
}

export interface ExtractionLimits {
  maxBytes: number;
  maxTextCharacters: number;
  maxPdfPages: number;
  maxCsvRows: number;
}

const DEFAULT_LIMITS: ExtractionLimits = {
  maxBytes: 15 * 1024 * 1024,
  maxTextCharacters: 250_000,
  maxPdfPages: 80,
  maxCsvRows: 5_000,
};

export async function extractDocument(
  bytes: Buffer,
  declaredMime: string,
  limits: ExtractionLimits = DEFAULT_LIMITS,
): Promise<ExtractedDocument> {
  if (bytes.length === 0) throw new Error("Attachment is empty");
  if (bytes.length > limits.maxBytes) throw new Error("Attachment exceeds the extraction size limit");
  const detected = await fileTypeFromBuffer(bytes);
  const mime = detected?.mime ?? normalizeDeclaredMime(declaredMime);

  if (mime === "application/pdf") return extractPdf(bytes, limits);
  if (mime.startsWith("image/")) {
    return {
      detectedMime: mime,
      kind: "image",
      text: "",
      metadata: { bytes: bytes.length, requiresVisionInterpretation: true },
    };
  }
  if (mime === "text/csv" || mime === "application/csv") return extractCsv(bytes, mime, limits);
  if (mime === "text/calendar" || mime === "application/ics") return extractIcs(bytes, mime, limits);
  if (mime === "text/html") {
    return {
      detectedMime: mime,
      kind: "text",
      text: clip(htmlToText(bytes.toString("utf8"), { wordwrap: false }), limits.maxTextCharacters),
      metadata: { bytes: bytes.length, sourceFormat: "html" },
    };
  }
  if (mime.startsWith("text/") || mime === "application/json" || mime === "application/xml") {
    return {
      detectedMime: mime,
      kind: "text",
      text: clip(bytes.toString("utf8"), limits.maxTextCharacters),
      metadata: { bytes: bytes.length, sourceFormat: mime },
    };
  }
  throw new Error(`Unsupported attachment type: ${mime}`);
}

async function extractPdf(bytes: Buffer, limits: ExtractionLimits): Promise<ExtractedDocument> {
  const document = await getDocument({ data: new Uint8Array(bytes), useSystemFonts: true }).promise;
  if (document.numPages > limits.maxPdfPages) throw new Error("PDF exceeds the page limit");
  const pages: string[] = [];
  for (let pageNumber = 1; pageNumber <= document.numPages; pageNumber += 1) {
    const page = await document.getPage(pageNumber);
    const content = await page.getTextContent();
    const text = content.items
      .flatMap((item) => ("str" in item && typeof item.str === "string" ? [item.str] : []))
      .join(" ");
    pages.push(`[Page ${pageNumber}]\n${text}`);
  }
  return {
    detectedMime: "application/pdf",
    kind: "pdf",
    text: clip(pages.join("\n\n"), limits.maxTextCharacters),
    metadata: { bytes: bytes.length, pages: document.numPages },
  };
}

function extractCsv(bytes: Buffer, mime: string, limits: ExtractionLimits): ExtractedDocument {
  const records = parseCsv(bytes, {
    bom: true,
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    to: limits.maxCsvRows,
  }) as Record<string, string>[];
  return {
    detectedMime: mime,
    kind: "csv",
    text: clip(JSON.stringify(records), limits.maxTextCharacters),
    metadata: { bytes: bytes.length, rows: records.length },
  };
}

function extractIcs(bytes: Buffer, mime: string, limits: ExtractionLimits): ExtractedDocument {
  const component = new ICAL.Component(ICAL.parse(bytes.toString("utf8")));
  const events = component.getAllSubcomponents("vevent").map((eventComponent) => {
    const event = new ICAL.Event(eventComponent);
    return {
      uid: event.uid,
      summary: event.summary,
      description: event.description,
      location: event.location,
      start: event.startDate?.toString(),
      end: event.endDate?.toString(),
    };
  });
  return {
    detectedMime: mime,
    kind: "ics",
    text: clip(JSON.stringify(events), limits.maxTextCharacters),
    metadata: { bytes: bytes.length, events: events.length },
  };
}

function clip(value: string, maximum: number): string {
  return value.length <= maximum ? value : `${value.slice(0, maximum)}\n[truncated]`;
}

function normalizeDeclaredMime(value: string): string {
  return value.split(";", 1)[0]?.trim().toLowerCase() || "application/octet-stream";
}
