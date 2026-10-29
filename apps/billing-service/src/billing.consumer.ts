import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import {
  ActivationFailed,
  ActivationRequested,
  CONSUMER_GROUPS,
  DomainEvent,
  TOPICS,
  createEvent,
  deriveEventId,
} from "@poc/contracts";
import {
  BUS,
  Bus,
  EventPublisher,
  HandlerContext,
  MongoHolder,
  ProcessedEvents,
  startEventConsumer,
} from "@poc/kafka-toolkit";
import { config, randomBetween, sleep } from "./config";

interface BillingAccountDoc {
  _id: string; // activationId (único)
  customerId: string;
  accountId?: string;
  status?: "CREATED" | "CANCELLED" | "FAILED";
  /** true si vimos ActivationFailed antes de crear la cuenta (caso borde). */
  failureSeen?: boolean;
  createdAt: string;
  updatedAt: string;
}

@Injectable()
export class BillingConsumer implements OnModuleInit {
  private readonly logger = new Logger(BillingConsumer.name);

  constructor(
    @Inject(BUS) private readonly bus: Bus,
    private readonly mongo: MongoHolder,
    private readonly processed: ProcessedEvents,
    private readonly publisher: EventPublisher,
  ) {}

  async onModuleInit() {
    await this.mongo.ensureCollections("billing_accounts");
    await startEventConsumer({
      bus: this.bus,
      groupId: config.groupId || CONSUMER_GROUPS.BILLING,
      topics: [TOPICS.ACTIVATION_REQUESTED, TOPICS.ACTIVATION_EVENTS],
      idempotency: this.processed,
      handler: (event, ctx) => this.handle(event, ctx),
    });
  }

  private accounts() {
    return this.mongo.db.collection<BillingAccountDoc>("billing_accounts");
  }

  private async handle(event: DomainEvent, ctx: HandlerContext) {
    switch (event.eventType) {
      case "ActivationRequested":
        return this.onRequested(event, ctx);
      case "ActivationFailed":
        return this.onFailed(event, ctx);
      default:
        return; // otros eventos no le interesan a billing
    }
  }

  /** Crea la cuenta de facturación simulada (o falla a pedido). */
  private async onRequested(event: ActivationRequested, ctx: HandlerContext) {
    await sleep(randomBetween(config.delayMinMs, config.delayMaxMs));
    const session = ctx.session;
    const id = event.correlationId;
    const now = new Date().toISOString();
    const existing = await this.accounts().findOne({ _id: id }, { session });

    if (event.payload.simulateFailure === "billing") {
      await this.accounts().updateOne(
        { _id: id },
        { $set: { customerId: event.customerId, status: "FAILED", updatedAt: now }, $setOnInsert: { createdAt: now } },
        { upsert: true, session },
      );
      await this.publisher.publish(
        createEvent({
          eventType: "BillingFailed" as const,
          eventId: deriveEventId(event.eventId, "BillingFailed"),
          correlationId: id,
          customerId: event.customerId,
          source: "billing" as const,
          payload: { reason: "Fallo simulado en billing" },
        }),
      );
      this.logger.warn(`[${id}] billing falla a pedido (simulateFailure=billing)`);
      return;
    }

    const accountId = `ACC-${id}`;
    await this.accounts().updateOne(
      { _id: id },
      { $set: { customerId: event.customerId, accountId, status: "CREATED", updatedAt: now }, $setOnInsert: { createdAt: now } },
      { upsert: true, session },
    );
    await this.publisher.publish(
      createEvent({
        eventType: "BillingAccountCreated" as const,
        eventId: deriveEventId(event.eventId, "BillingAccountCreated"),
        correlationId: id,
        customerId: event.customerId,
        source: "billing" as const,
        payload: { accountId },
      }),
    );
    this.logger.log(`[${id}] cuenta de facturación creada (${accountId})`);

    // Caso borde: el fallo (ActivationFailed) llegó ANTES de que se creara la cuenta.
    if (existing?.failureSeen) {
      await this.cancel(event, accountId, "Activación fallida (el fallo llegó antes de crear la cuenta)", ctx);
    }
  }

  /** Compensación: anula la cuenta si la había creado. */
  private async onFailed(event: ActivationFailed, ctx: HandlerContext) {
    const session = ctx.session;
    const id = event.correlationId;
    const existing = await this.accounts().findOne({ _id: id }, { session });

    if (existing?.status === "CREATED" && existing.accountId) {
      await this.cancel(event, existing.accountId, event.payload.reason, ctx);
    } else if (!existing || (!existing.status && !existing.failureSeen)) {
      // Todavía no se creó la cuenta: dejar marcado que ya vimos el fallo.
      const now = new Date().toISOString();
      await this.accounts().updateOne(
        { _id: id },
        { $set: { customerId: event.customerId, failureSeen: true, updatedAt: now }, $setOnInsert: { createdAt: now } },
        { upsert: true, session },
      );
      this.logger.warn(`[${id}] ActivationFailed llegó antes de crear la cuenta; se anulará al crearla`);
    }
    // status FAILED (billing falló solo) o CANCELLED: no hay nada que anular.
  }

  private async cancel(parent: DomainEvent, accountId: string, reason: string, ctx: HandlerContext) {
    const id = parent.correlationId;
    await this.accounts().updateOne(
      { _id: id },
      { $set: { status: "CANCELLED", updatedAt: new Date().toISOString() } },
      { session: ctx.session },
    );
    await this.publisher.publish(
      createEvent({
        eventType: "BillingAccountCancelled" as const,
        eventId: deriveEventId(parent.eventId, "BillingAccountCancelled"),
        correlationId: id,
        customerId: parent.customerId,
        source: "billing" as const,
        payload: { accountId, reason },
      }),
    );
    this.logger.log(`[${id}] cuenta ${accountId} ANULADA (compensación)`);
  }
}
