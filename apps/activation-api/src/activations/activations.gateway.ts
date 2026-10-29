import { ConnectedSocket, MessageBody, SubscribeMessage, WebSocketGateway, WebSocketServer } from "@nestjs/websockets";
import { Server, Socket } from "socket.io";
import { ActivationDoc, toView } from "./activation.types";
import { ActivationsRepository } from "./activations.repository";

/**
 * RF-08: envía cada cambio de una activación a la UI.
 * La UI manda "subscribe" con el activationId y recibe "activation.updated"
 * (primero el estado actual, así no se pierde ningún evento anterior).
 */
@WebSocketGateway({ cors: { origin: "*" } })
export class ActivationsGateway {
  @WebSocketServer() server: Server;

  constructor(private readonly repo: ActivationsRepository) {}

  @SubscribeMessage("subscribe")
  async onSubscribe(@MessageBody() activationId: string, @ConnectedSocket() client: Socket) {
    await client.join(activationId);
    const doc = await this.repo.findById(activationId);
    if (doc) client.emit("activation.updated", toView(doc));
  }

  emitUpdate(doc: ActivationDoc) {
    this.server?.to(doc._id).emit("activation.updated", toView(doc));
  }
}
