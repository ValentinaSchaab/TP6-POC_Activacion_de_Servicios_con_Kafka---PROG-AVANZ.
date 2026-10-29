import { EventEnvelope, SimulateFailure } from './envelope';
import { TOPICS, Topic } from './topics';

/** Paso de la saga que puede fallar. */
export type FailedStep = 'billing' | 'provisioning';

// ---------- Payloads ----------
export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: SimulateFailure;
}
export interface BillingAccountCreatedPayload {
  accountId: string;
}
export interface BillingFailedPayload {
  reason: string;
}
export interface BillingAccountCancelledPayload {
  accountId?: string;
  reason: string;
}
export interface ProvisioningCompletedPayload {
  orderId: string;
}
export interface ProvisioningFailedPayload {
  reason: string;
}
export interface ActivationCompletedPayload {
  planId: string;
}
export interface ActivationFailedPayload {
  failedStep: FailedStep;
  reason: string;
}

// ---------- Eventos ----------
export type ActivationRequested = EventEnvelope<'ActivationRequested', ActivationRequestedPayload>;
export type BillingAccountCreated = EventEnvelope<'BillingAccountCreated', BillingAccountCreatedPayload>;
export type BillingFailed = EventEnvelope<'BillingFailed', BillingFailedPayload>;
export type BillingAccountCancelled = EventEnvelope<'BillingAccountCancelled', BillingAccountCancelledPayload>;
export type ProvisioningCompleted = EventEnvelope<'ProvisioningCompleted', ProvisioningCompletedPayload>;
export type ProvisioningFailed = EventEnvelope<'ProvisioningFailed', ProvisioningFailedPayload>;
export type ActivationCompleted = EventEnvelope<'ActivationCompleted', ActivationCompletedPayload>;
export type ActivationFailed = EventEnvelope<'ActivationFailed', ActivationFailedPayload>;

export type DomainEvent =
  | ActivationRequested
  | BillingAccountCreated
  | BillingFailed
  | BillingAccountCancelled
  | ProvisioningCompleted
  | ProvisioningFailed
  | ActivationCompleted
  | ActivationFailed;

export type EventType = DomainEvent['eventType'];

/** En qué topic se publica cada tipo de evento. */
export const EVENT_TOPIC: Record<EventType, Topic> = {
  ActivationRequested: TOPICS.ACTIVATION_REQUESTED,
  BillingAccountCreated: TOPICS.BILLING_EVENTS,
  BillingFailed: TOPICS.BILLING_EVENTS,
  BillingAccountCancelled: TOPICS.BILLING_EVENTS,
  ProvisioningCompleted: TOPICS.PROVISIONING_EVENTS,
  ProvisioningFailed: TOPICS.PROVISIONING_EVENTS,
  ActivationCompleted: TOPICS.ACTIVATION_EVENTS,
  ActivationFailed: TOPICS.ACTIVATION_EVENTS,
};

const KNOWN_TYPES = new Set<string>(Object.keys(EVENT_TOPIC));

/** Valida que un objeto parseado tenga la forma de un evento del contrato. */
export function isDomainEvent(x: unknown): x is DomainEvent {
  if (typeof x !== "object" || x === null) return false;
  const e = x as Record<string, unknown>;
  return (
    typeof e.eventId === "string" &&
    typeof e.eventType === "string" &&
    KNOWN_TYPES.has(e.eventType) &&
    typeof e.version === "number" &&
    typeof e.occurredAt === "string" &&
    typeof e.correlationId === "string" &&
    typeof e.customerId === "string" &&
    typeof e.source === "string" &&
    typeof e.payload === "object" &&
    e.payload !== null
  );
}
