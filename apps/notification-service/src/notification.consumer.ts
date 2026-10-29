import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { ActivationCompleted, ActivationFailed, CONSUMER_GROUPS, DomainEvent, TOPICS } from "@poc/contracts";
import {
  BUS,
  Bus,
  HandlerContext,
  MongoHolder,
  ProcessedEvents,
  startEventConsumer,
} from "@poc/kafka-toolkit";
import * as nodemailer from "nodemailer";
import { config } from "./config";

interface NotificationDoc {
  _id: string; // `${activationId}:${eventType}` (único)
  activationId: string;
  eventType: string;
  customerId: string;
  to: string;
  subject: string;
  sentAt: string;
}

@Injectable()
export class NotificationConsumer implements OnModuleInit {
  private readonly logger = new Logger(NotificationConsumer.name);
  private readonly transport = nodemailer.createTransport({
    host: config.smtpHost,
    port: config.smtpPort,
    secure: false,
    ignoreTLS: true,
  });

  constructor(
    @Inject(BUS) private readonly bus: Bus,
    private readonly mongo: MongoHolder,
    private readonly processed: ProcessedEvents,
  ) {}

  async onModuleInit() {
    await this.mongo.ensureCollections("notifications");
    await startEventConsumer({
      bus: this.bus,
      groupId: config.groupId || CONSUMER_GROUPS.NOTIFICATION,
      topics: [TOPICS.ACTIVATION_EVENTS],
      idempotency: this.processed,
      handler: (event, ctx) => this.handle(event, ctx),
    });
  }

  private async handle(event: DomainEvent, ctx: HandlerContext) {
    if (event.eventType === "ActivationCompleted") return this.send(event, ctx, this.welcome(event));
    if (event.eventType === "ActivationFailed") return this.send(event, ctx, this.failure(event));
  }

  private welcome(e: ActivationCompleted) {
    return {
      subject: `¡Bienvenido! Tu plan ${e.payload.planId} ya está activo`,
      text: `Hola ${e.customerId},\n\nTu plan "${e.payload.planId}" quedó activo.\nCódigo de activación: ${e.correlationId}\n\n¡Gracias por elegirnos!`,
    };
  }

  private failure(e: ActivationFailed) {
    return {
      subject: "No pudimos activar tu plan",
      text: `Hola ${e.customerId},\n\nNo pudimos completar la activación ${e.correlationId}.\nMotivo: ${e.payload.reason} (paso: ${e.payload.failedStep}).\n\nNo se te cobró nada. Podés intentarlo de nuevo.`,
    };
  }

  private async send(
    event: ActivationCompleted | ActivationFailed,
    ctx: HandlerContext,
    mail: { subject: string; text: string },
  ) {
    const to = `${event.customerId.toLowerCase()}@clientes.local`;
    await this.transport.sendMail({ from: config.mailFrom, to, subject: mail.subject, text: mail.text });
    await this.mongo.db.collection<NotificationDoc>("notifications").insertOne(
      {
        _id: `${event.correlationId}:${event.eventType}`,
        activationId: event.correlationId,
        eventType: event.eventType,
        customerId: event.customerId,
        to,
        subject: mail.subject,
        sentAt: new Date().toISOString(),
      },
      { session: ctx.session },
    );
    this.logger.log(`[${event.correlationId}] email enviado a ${to}: ${mail.subject}`);
  }
}
