import { cn } from "@/lib/utils";

export function Stepper({ currentStep, steps }: { currentStep: number; steps: string[] }) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      {steps.map((label, index) => {
        const stepNumber = index + 1;
        const isActive = stepNumber === currentStep;
        const isCompleted = stepNumber < currentStep;

        return (
          <div key={label} className="flex items-center gap-3">
            {index > 0 ? (
              <div className={cn("hidden h-px w-8 sm:block", isCompleted ? "bg-primary" : "bg-border")} />
            ) : null}
            <div className="flex items-center gap-2">
              <div
                className={cn(
                  "flex h-8 w-8 items-center justify-center rounded-full text-xs font-semibold",
                  isActive && "bg-primary text-primary-foreground",
                  isCompleted && "bg-primary/12 text-primary",
                  !isActive && !isCompleted && "bg-muted text-muted-foreground",
                )}
              >
                {stepNumber}
              </div>
              <span className={cn("text-sm", isActive ? "font-semibold" : "text-muted-foreground")}>{label}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}
