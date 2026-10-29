import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ALL_TOPICS, CONSUMER_GROUPS, DomainEvent } from "@poc/contracts";
import {
  BUS,
  Bus,
  HandlerContext,
  MongoHolder,
  ProcessedEvents,
  startEventConsumer,
} from "@poc/kafka-toolkit";
import { config } from "./config";

/**
 * Consume TODOS los topics y no publica nada: demuestra que se puede sumar un
 * consumidor sin tocar a los demás. Lee desde el inicio (RF-10): para reprocesar
 * el historial, levantarlo con otro CRM_GROUP_ID o resetear offsets en Kafka UI.
 */
@Injectable()
export class CrmAnalyticsConsumer implements OnModuleInit {
  private readonly logger = new Logger(CrmAnalyticsConsumer.name);

  constructor(
    @Inject(BUS) private readonly bus: Bus,
    private readonly mongo: MongoHolder,
    private readonly processed: ProcessedEvents,
  ) {}

  async onModuleInit() {
    await this.mongo.ensureCollections("event_log");
    await this.mongo.db.collection("event_log").createIndex({ correlationId: 1 });
    await startEventConsumer({
      bus: this.bus,
      groupId: config.groupId || CONSUMER_GROUPS.CRM_ANALYTICS,
      topics: ALL_TOPICS,
      fromBeginning: true,
      idempotency: this.processed,
      handler: (event, ctx) => this.handle(event, ctx),
    });
  }

  private async handle(event: DomainEvent, ctx: HandlerContext) {
    const { eventId, ...rest } = event;
    await this.mongo.db.collection<{ _id: string }>("event_log").insertOne(
      {
        _id: eventId,
        eventId,
        ...rest,
        topic: ctx.message.topic,
        partition: ctx.message.partition,
        offset: ctx.message.offset,
        receivedAt: new Date().toISOString(),
      } as never,
      { session: ctx.session },
    );
    this.logger.log(`[${event.correlationId}] registrado ${event.eventType} (${ctx.message.topic})`);
  }
}
