import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { DomainEvent, TOPICS } from "@poc/contracts";
import {
  BUS,
  Bus,
  HandlerContext,
  MongoHolder,
  ProcessedEvents,
  startEventConsumer,
} from "@poc/kafka-toolkit";
import { config } from "./config";

const POINTS_PER_ACTIVATION = 100;

/**
 * Escenario 5: un servicio NUEVO con su propio consumer group que lee el log desde
 * el principio y procesa todo el historial sin tocar a los demás servicios.
 */
@Injectable()
export class LoyaltyConsumer implements OnModuleInit {
  private readonly logger = new Logger(LoyaltyConsumer.name);

  constructor(
    @Inject(BUS) private readonly bus: Bus,
    private readonly mongo: MongoHolder,
    private readonly processed: ProcessedEvents,
  ) {}

  async onModuleInit() {
    await this.mongo.ensureCollections("loyalty_points");
    await startEventConsumer({
      bus: this.bus,
      groupId: config.groupId,
      topics: [TOPICS.ACTIVATION_EVENTS],
      fromBeginning: true, // auto.offset.reset=earliest
      idempotency: this.processed,
      handler: (event, ctx) => this.handle(event, ctx),
    });
  }

  private async handle(event: DomainEvent, ctx: HandlerContext) {
    if (event.eventType !== "ActivationCompleted") return;
    await this.mongo.db.collection<{ _id: string }>("loyalty_points").insertOne(
      {
        _id: event.correlationId,
        customerId: event.customerId,
        planId: event.payload.planId,
        points: POINTS_PER_ACTIVATION,
        awardedAt: new Date().toISOString(),
      } as never,
      { session: ctx.session },
    );
    this.logger.log(`[${event.correlationId}] +${POINTS_PER_ACTIVATION} puntos para ${event.customerId} (historial reprocesado)`);
  }
}
