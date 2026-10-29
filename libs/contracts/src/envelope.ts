import { createHash, randomUUID } from "crypto";

/** Servicios que pueden publicar eventos. */
export type ServiceName =
  | "activation-api"
  | "billing"
  | "provisioning"
  | "notification"
  | "crm-analytics"
  | "loyalty";

/** Solo para la demo: qué servicio debe fallar a propósito. */
export type SimulateFailure = "none" | "billing" | "provisioning";

/**
 * Sobre común de todos los eventos. El payload cambia según el tipo.
 * - eventId: base de la idempotencia.
 * - eventType + version: permiten evolucionar el contrato sin romper consumidores.
 * - correlationId: el activationId; agrupa todos los eventos de una activación.
 * - customerId: también es la clave del mensaje en Kafka (orden por cliente).
 */
export interface EventEnvelope<TType extends string = string, TPayload = unknown> {
  eventId: string;
  eventType: TType;
  version: number;
  occurredAt: string; // ISO 8601
  correlationId: string;
  customerId: string;
  source: ServiceName;
  payload: TPayload;
}

export interface CreateEventParams<TType extends string, TPayload> {
  eventType: TType;
  correlationId: string;
  customerId: string;
  source: ServiceName;
  payload: TPayload;
  version?: number;
  /** Si se omite, se genera un UUID aleatorio. */
  eventId?: string;
}

/** Crea un evento con eventId y fecha generados. */
export function createEvent<TType extends string, TPayload>(
  params: CreateEventParams<TType, TPayload>,
): EventEnvelope<TType, TPayload> {
  return {
    eventId: params.eventId ?? randomUUID(),
    eventType: params.eventType,
    version: params.version ?? 1,
    occurredAt: new Date().toISOString(),
    correlationId: params.correlationId,
    customerId: params.customerId,
    source: params.source,
    payload: params.payload,
  };
}

/**
 * eventId determinístico para un evento que un servicio publica EN RESPUESTA a otro.
 * Si el evento de entrada se entrega dos veces y el servicio lo reprocesa, el evento
 * de salida sale con el MISMO eventId y los consumidores lo descartan por idempotencia.
 */
export function deriveEventId(parentEventId: string, outputEventType: string): string {
  const h = createHash("sha256").update(`${parentEventId}:${outputEventType}`).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}
