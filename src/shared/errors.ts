export class ConflictError extends Error {
  public override readonly name = "ConflictError";
}

export class NotFoundError extends Error {
  public override readonly name = "NotFoundError";
}

export class UnauthorizedError extends Error {
  public override readonly name = "UnauthorizedError";
}

export class StaleAuthorityError extends Error {
  public override readonly name = "StaleAuthorityError";
}
