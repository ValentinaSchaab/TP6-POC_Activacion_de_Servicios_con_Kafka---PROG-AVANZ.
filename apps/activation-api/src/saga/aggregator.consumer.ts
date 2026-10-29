import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { DomainEvent, TOPICS, createEvent, deriveEventId } from "@poc/contracts";
import {
  BUS,
  Bus,
  EventPublisher,
  HandlerContext,
  ProcessedEvents,
  startEventConsumer,
} from "@poc/kafka-toolkit";
import { ActivationDoc, ActivationStatus, HistoryEntry } from "../activations/activation.types";
import { ActivationsGateway } from "../activations/activations.gateway";
import { ActivationsRepository } from "../activations/activations.repository";
import { config } from "../config";

type StepKey = "billing" | "provisioning";
const STEP_OF: Partial<Record<DomainEvent["eventType"], { key: StepKey; ok: boolean }>> = {
  BillingAccountCreated: { key: "billing", ok: true },
  BillingFailed: { key: "billing", ok: false },
  ProvisioningCompleted: { key: "provisioning", ok: true },
  ProvisioningFailed: { key: "provisioning", ok: false },
};

/**
 * Agregador de la saga (coreografía con agregador): junta los resultados de billing
 * y provisioning y decide si la activación terminó bien o mal.
 *  - Con los dos OK              -> ACTIVE  + ActivationCompleted
 *  - Con el PRIMER fallo         -> FAILED  + ActivationFailed (los resultados
 *    posteriores solo se registran en el historial)
 */
@Injectable()
export class AggregatorConsumer implements OnModuleInit {
  private readonly logger = new Logger(AggregatorConsumer.name);

  constructor(
    @Inject(BUS) private readonly bus: Bus,
    private readonly processed: ProcessedEvents,
    private readonly publisher: EventPublisher,
    private readonly repo: ActivationsRepository,
    private readonly gateway: ActivationsGateway,
  ) {}

  async onModuleInit() {
    await startEventConsumer({
      bus: this.bus,
      groupId: config.groupId,
      topics: [TOPICS.BILLING_EVENTS, TOPICS.PROVISIONING_EVENTS],
      idempotency: this.processed,
      handler: (event, ctx) => this.handle(event, ctx),
    });
  }

  private async handle(event: DomainEvent, ctx: HandlerContext) {
    const doc = await this.repo.findById(event.correlationId, ctx.session);
    if (!doc) {
      this.logger.warn(`Evento ${event.eventType} de una activación desconocida (${event.correlationId}); se ignora`);
      return;
    }

    const detail = "reason" in event.payload ? String((event.payload as { reason: string }).reason) : undefined;
    const entries: HistoryEntry[] = [
      { eventType: event.eventType, at: event.occurredAt, source: event.source, ...(detail ? { detail } : {}) },
    ];
    const steps = { ...doc.steps };
    let status: ActivationStatus = doc.status;
    let toPublish: DomainEvent | undefined;
    const terminal = doc.status === "ACTIVE" || doc.status === "FAILED";

    const step = STEP_OF[event.eventType];
    if (!terminal && step) {
      steps[step.key] = { status: step.ok ? "OK" : "FAILED", at: event.occurredAt };

      if (steps.billing?.status === "FAILED" || steps.provisioning?.status === "FAILED") {
        const failedStep: StepKey = steps.billing?.status === "FAILED" ? "billing" : "provisioning";
        status = "FAILED";
        toPublish = createEvent({
          eventType: "ActivationFailed" as const,
          eventId: deriveEventId(event.eventId, "ActivationFailed"),
          correlationId: doc._id,
          customerId: doc.customerId,
          source: "activation-api" as const,
          payload: { failedStep, reason: detail ?? `Falló ${failedStep}` },
        });
      } else if (steps.billing?.status === "OK" && steps.provisioning?.status === "OK") {
        status = "ACTIVE";
        toPublish = createEvent({
          eventType: "ActivationCompleted" as const,
          eventId: deriveEventId(event.eventId, "ActivationCompleted"),
          correlationId: doc._id,
          customerId: doc.customerId,
          source: "activation-api" as const,
          payload: { planId: doc.planId },
        });
      } else {
        status = "IN_PROGRESS";
      }
    }

    if (toPublish) {
      entries.push({
        eventType: toPublish.eventType,
        at: toPublish.occurredAt,
        source: "activation-api",
        ...(toPublish.eventType === "ActivationFailed" ? { detail: toPublish.payload.reason } : {}),
      });
    }

    const updatedAt = new Date().toISOString();
    await this.repo.collection().updateOne(
      { _id: doc._id },
      { $set: { status, steps, updatedAt }, $push: { history: { $each: entries } } },
      { session: ctx.session },
    );
    if (toPublish) {
      await this.publisher.publish(toPublish);
      this.logger.log(`[${doc._id}] ${toPublish.eventType} -> ${status}`);
    }

    const updated: ActivationDoc = { ...doc, status, steps, updatedAt, history: [...doc.history, ...entries] };
    ctx.afterCommit(() => this.gateway.emitUpdate(updated));
  }
}
