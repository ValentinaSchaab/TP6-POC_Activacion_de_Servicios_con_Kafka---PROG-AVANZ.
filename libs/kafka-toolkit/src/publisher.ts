import { DomainEvent, EVENT_TOPIC } from "@poc/contracts";
import { Bus } from "./bus";

/**
 * Publica eventos de dominio. El topic sale del contrato y la clave del mensaje es
 * customerId (orden por cliente, RNF-02).
 */
export class EventPublisher {
  constructor(private readonly bus: Bus) {}

  async publish(event: DomainEvent): Promise<void> {
    await this.bus.publish({
      topic: EVENT_TOPIC[event.eventType],
      key: event.customerId,
      value: JSON.stringify(event),
      headers: { "x-event-type": event.eventType, "x-correlation-id": event.correlationId },
    });
  }
}
