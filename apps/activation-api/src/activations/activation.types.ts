export type ActivationStatus = "PENDING" | "IN_PROGRESS" | "ACTIVE" | "FAILED";

export interface StepResult {
  status: "OK" | "FAILED";
  at: string;
}

export interface HistoryEntry {
  eventType: string;
  at: string;
  source?: string;
  detail?: string;
}

/** Documento de la colección `activations` (ver sección 07 del documento). */
export interface ActivationDoc {
  _id: string; // activationId
  customerId: string;
  planId: string;
  channel: string;
  status: ActivationStatus;
  simulateFailure: "none" | "billing" | "provisioning";
  steps: { billing?: StepResult; provisioning?: StepResult };
  history: HistoryEntry[];
  createdAt: string;
  updatedAt: string;
}

/** Forma pública (REST y WebSocket): _id se expone como activationId. */
export function toView(doc: ActivationDoc) {
  const { _id, ...rest } = doc;
  return { activationId: _id, ...rest };
}
