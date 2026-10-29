import { Injectable, Logger, NotFoundException, ServiceUnavailableException } from "@nestjs/common";
import { randomUUID } from "crypto";
import { ActivationRequested, createEvent } from "@poc/contracts";
import { EventPublisher } from "@poc/kafka-toolkit";
import { ActivationDoc, toView } from "./activation.types";
import { ActivationsGateway } from "./activations.gateway";
import { ActivationsRepository } from "./activations.repository";
import { CreateActivationDto } from "./dto/create-activation.dto";

@Injectable()
export class ActivationsService {
  private readonly logger = new Logger(ActivationsService.name);

  constructor(
    private readonly repo: ActivationsRepository,
    private readonly publisher: EventPublisher,
    private readonly gateway: ActivationsGateway,
  ) {}

  async create(dto: CreateActivationDto) {
    const activationId = `act-${randomUUID().slice(0, 8)}`;
    const simulateFailure = dto.simulateFailure ?? "none";
    const channel = dto.channel ?? "web";

    const event: ActivationRequested = createEvent({
      eventType: "ActivationRequested" as const,
      correlationId: activationId,
      customerId: dto.customerId,
      source: "activation-api" as const,
      payload: { planId: dto.planId, channel, simulateFailure },
    });

    // 1) Guardar la activación en estado PENDING
    const doc: ActivationDoc = {
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      channel,
      status: "PENDING",
      simulateFailure,
      steps: {},
      history: [{ eventType: event.eventType, at: event.occurredAt, source: event.source }],
      createdAt: event.occurredAt,
      updatedAt: event.occurredAt,
    };
    await this.repo.collection().insertOne(doc);

    // 2) Publicar el evento. Fases 1-3: se publica directo después de guardar.
    //    RIESGO conocido: si la API se cae entre 1) y 2), el evento se pierde.
    //    La fase 4 (opcional) lo resuelve con el outbox transaccional.
    try {
      await this.publisher.publish(event);
    } catch (err) {
      this.logger.error(`No se pudo publicar ActivationRequested de ${activationId}`, err as Error);
      throw new ServiceUnavailableException(
        `La activación ${activationId} se guardó pero no se pudo publicar el evento`,
      );
    }
    this.logger.log(`[${activationId}] ActivationRequested publicado`);
    this.gateway.emitUpdate(doc);

    return { activationId, status: doc.status };
  }

  async findOne(id: string) {
    const doc = await this.repo.findById(id);
    if (!doc) throw new NotFoundException(`Activación ${id} no encontrada`);
    return toView(doc);
  }
}
