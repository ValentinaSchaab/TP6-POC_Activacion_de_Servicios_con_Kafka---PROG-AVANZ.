import { Bus, BusMessage, PublishInput, SubscribeInput } from "./bus";

interface Sub {
  groupId: string;
  topics: Set<string>;
  handler: (m: BusMessage) => Promise<void>;
  next: Map<string, number>; // "topic:partition" -> próximo índice a leer
  draining: boolean;
  again: boolean;
}

/**
 * Bus en memoria con la misma semántica básica de Kafka (particiones, offsets por
 * consumer group, replay desde el inicio). Solo para pruebas automáticas.
 */
export class InMemoryBus implements Bus {
  private readonly partitions = 3;
  private readonly logs = new Map<string, BusMessage[][]>();
  private readonly subs: Sub[] = [];
  private active = 0;

  private log(topic: string): BusMessage[][] {
    let l = this.logs.get(topic);
    if (!l) {
      l = Array.from({ length: this.partitions }, () => []);
      this.logs.set(topic, l);
    }
    return l;
  }

  private partitionOf(key: string | null): number {
    if (!key) return 0;
    let h = 0;
    for (const c of key) h = (h * 31 + c.charCodeAt(0)) >>> 0;
    return h % this.partitions;
  }

  /** Todos los mensajes de un topic (para aserciones en tests). */
  messages(topic: string): BusMessage[] {
    return this.log(topic).flat();
  }

  async publish(input: PublishInput): Promise<void> {
    const partition = this.partitionOf(input.key);
    const p = this.log(input.topic)[partition];
    p.push({
      topic: input.topic,
      partition,
      offset: String(p.length),
      key: input.key,
      value: input.value,
      headers: input.headers ?? {},
    });
    this.kick();
  }

  async subscribe(input: SubscribeInput): Promise<void> {
    const sub: Sub = {
      groupId: input.groupId,
      topics: new Set(input.topics),
      handler: input.handler,
      next: new Map(),
      draining: false,
      again: false,
    };
    for (const t of input.topics) {
      this.log(t).forEach((part, i) =>
        sub.next.set(`${t}:${i}`, input.fromBeginning ? 0 : part.length),
      );
    }
    this.subs.push(sub);
    this.kick();
  }

  private kick() {
    for (const s of this.subs) {
      if (s.draining) {
        s.again = true;
        continue;
      }
      s.draining = true;
      this.active++;
      void this.drain(s);
    }
  }

  private async drain(s: Sub) {
    try {
      do {
        s.again = false;
        for (const t of s.topics) {
          const parts = this.log(t);
          for (let i = 0; i < parts.length; i++) {
            const k = `${t}:${i}`;
            let n = s.next.get(k) ?? 0;
            while (n < parts[i].length) {
              try {
                await s.handler(parts[i][n]);
                n++;
                s.next.set(k, n);
              } catch {
                await new Promise((r) => setTimeout(r, 20)); // como kafkajs: reintenta
              }
            }
          }
        }
      } while (s.again);
    } finally {
      s.draining = false;
      this.active--;
    }
  }

  /** Espera a que todos los consumidores terminen de procesar lo publicado. */
  async whenIdle(quietMs = 60): Promise<void> {
    let quiet = 0;
    while (quiet < quietMs) {
      await new Promise((r) => setTimeout(r, 10));
      quiet = this.active === 0 ? quiet + 10 : 0;
    }
  }

  async disconnect(): Promise<void> {
    /* nada */
  }
}
