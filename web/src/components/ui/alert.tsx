import type { HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

export function Alert({
  className,
  tone = "default",
  ...props
}: HTMLAttributes<HTMLDivElement> & {
  tone?: "default" | "warning" | "success" | "destructive";
}) {
  const toneClasses =
    tone === "warning"
      ? "border-amber-200 bg-amber-50 text-amber-900"
      : tone === "success"
        ? "border-emerald-200 bg-emerald-50 text-emerald-900"
        : tone === "destructive"
          ? "border-red-200 bg-red-50 text-red-900"
          : "border-border bg-card text-card-foreground";
  return <div className={cn("rounded-2xl border p-4 text-sm", toneClasses, className)} {...props} />;
}
