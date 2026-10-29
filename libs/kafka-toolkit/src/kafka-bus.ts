import { Consumer, Kafka, Partitioners, Producer, logLevel } from "kafkajs";
import { Bus, BusMessage, PublishInput, SubscribeInput } from "./bus";

export interface KafkaBusOptions {
  brokers: string[];
  clientId: string;
}

/** Implementación del Bus sobre kafkajs (único lugar que depende de kafkajs). */
export class KafkaBus implements Bus {
  private readonly kafka: Kafka;
  private producer?: Producer;
  private producerReady?: Promise<void>;
  private readonly consumers: Consumer[] = [];

  constructor(opts: KafkaBusOptions) {
    this.kafka = new Kafka({
      clientId: opts.clientId,
      brokers: opts.brokers,
      logLevel: logLevel.WARN,
      retry: { initialRetryTime: 300, retries: 10 },
    });
  }

  private async getProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = this.kafka.producer({
        createPartitioner: Partitioners.DefaultPartitioner,
        allowAutoTopicCreation: false,
      });
      this.producerReady = this.producer.connect();
    }
    await this.producerReady;
    return this.producer;
  }

  async publish(input: PublishInput): Promise<void> {
    const producer = await this.getProducer();
    await producer.send({
      topic: input.topic,
      messages: [{ key: input.key, value: input.value, headers: input.headers }],
    });
  }

  async subscribe(input: SubscribeInput): Promise<void> {
    const consumer = this.kafka.consumer({
      groupId: input.groupId,
      sessionTimeout: 15000,
      heartbeatInterval: 3000,
      allowAutoTopicCreation: false,
    });
    this.consumers.push(consumer);
    await consumer.connect();
    for (const topic of input.topics) {
      await consumer.subscribe({ topic, fromBeginning: input.fromBeginning ?? false });
    }
    await consumer.run({
      autoCommit: false, // commit manual: SOLO después de procesar, nunca antes
      eachMessage: async ({ topic, partition, message }) => {
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) {
          if (v !== undefined) headers[k] = Array.isArray(v) ? v.map(String).join(",") : v.toString();
        }
        const msg: BusMessage = {
          topic,
          partition,
          offset: message.offset,
          key: message.key ? message.key.toString() : null,
          value: message.value ? message.value.toString() : "",
          headers,
        };
        await input.handler(msg); // si lanza, kafkajs reintenta y NO se confirma el offset
        await consumer.commitOffsets([
          { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
        ]);
      },
    });
  }

  async disconnect(): Promise<void> {
    for (const c of this.consumers) await c.disconnect().catch(() => undefined);
    await this.producer?.disconnect().catch(() => undefined);
  }

  /** Hook de Nest. */
  async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }
}
