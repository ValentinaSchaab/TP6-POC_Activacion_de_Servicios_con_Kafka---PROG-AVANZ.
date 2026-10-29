import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ActivationRequested, CONSUMER_GROUPS, DomainEvent, TOPICS, createEvent, deriveEventId } from "@poc/contracts";
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

interface ProvisioningOrderDoc {
  _id: string; // activationId (único)
  customerId: string;
  orderId?: string;
  status: "COMPLETED" | "FAILED";
  createdAt: string;
}

@Injectable()
export class ProvisioningConsumer implements OnModuleInit {
  private readonly logger = new Logger(ProvisioningConsumer.name);

  constructor(
    @Inject(BUS) private readonly bus: Bus,
    private readonly mongo: MongoHolder,
    private readonly processed: ProcessedEvents,
    private readonly publisher: EventPublisher,
  ) {}

  async onModuleInit() {
    await this.mongo.ensureCollections("provisioning_orders");
    await startEventConsumer({
      bus: this.bus,
      groupId: config.groupId || CONSUMER_GROUPS.PROVISIONING,
      topics: [TOPICS.ACTIVATION_REQUESTED],
      idempotency: this.processed,
      handler: (event, ctx) => this.handle(event, ctx),
    });
  }

  private async handle(event: DomainEvent, ctx: HandlerContext) {
    if (event.eventType !== "ActivationRequested") return;
    await this.onRequested(event, ctx);
  }

  /** Simula el aprovisionamiento con un retardo de 1 a 3 segundos. Puede fallar a pedido. */
  private async onRequested(event: ActivationRequested, ctx: HandlerContext) {
    const id = event.correlationId;
    await sleep(randomBetween(config.delayMinMs, config.delayMaxMs));
    const orders = this.mongo.db.collection<ProvisioningOrderDoc>("provisioning_orders");
    const now = new Date().toISOString();

    if (event.payload.simulateFailure === "provisioning") {
      await orders.updateOne(
        { _id: id },
        { $set: { customerId: event.customerId, status: "FAILED" }, $setOnInsert: { createdAt: now } },
        { upsert: true, session: ctx.session },
      );
      await this.publisher.publish(
        createEvent({
          eventType: "ProvisioningFailed" as const,
          eventId: deriveEventId(event.eventId, "ProvisioningFailed"),
          correlationId: id,
          customerId: event.customerId,
          source: "provisioning" as const,
          payload: { reason: "Fallo simulado en provisioning" },
        }),
      );
      this.logger.warn(`[${id}] provisioning falla a pedido (simulateFailure=provisioning)`);
      return;
    }

    const orderId = `ORD-${id}`;
    await orders.updateOne(
      { _id: id },
      { $set: { customerId: event.customerId, orderId, status: "COMPLETED" }, $setOnInsert: { createdAt: now } },
      { upsert: true, session: ctx.session },
    );
    await this.publisher.publish(
      createEvent({
        eventType: "ProvisioningCompleted" as const,
        eventId: deriveEventId(event.eventId, "ProvisioningCompleted"),
        correlationId: id,
        customerId: event.customerId,
        source: "provisioning" as const,
        payload: { orderId },
      }),
    );
    this.logger.log(`[${id}] línea aprovisionada (${orderId})`);
  }
}
