/**
 * Abstracción mínima del bus de mensajes. Todo el resto del sistema habla con esta
 * interfaz; solo kafka-bus.ts conoce kafkajs. Si hay que reemplazar kafkajs (riesgo
 * de mantenimiento del documento), se cambia SOLO ese archivo.
 */
export interface BusMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

export interface PublishInput {
  topic: string;
  key: string | null;
  value: string;
  headers?: Record<string, string>;
}

export interface SubscribeInput {
  groupId: string;
  topics: string[];
  /** true: un grupo nuevo lee todo el historial (replay del log). */
  fromBeginning?: boolean;
  /**
   * Procesa un mensaje. El offset se confirma (commit manual) SOLO si la promesa
   * se resuelve; si rechaza, el mensaje se vuelve a entregar.
   */
  handler: (message: BusMessage) => Promise<void>;
}

export interface Bus {
  publish(input: PublishInput): Promise<void>;
  subscribe(input: SubscribeInput): Promise<void>;
  disconnect(): Promise<void>;
}
