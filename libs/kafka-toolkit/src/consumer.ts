import { Logger } from "@nestjs/common";
import { DomainEvent, dlqTopic, isDomainEvent } from "@poc/contracts";
import { ClientSession } from "mongodb";
import { Bus, BusMessage } from "./bus";
import { ProcessedEvents } from "./idempotency";

export interface HandlerContext {
  /** Sesión de la transacción de Mongo (si el consumidor usa idempotencia). */
  session?: ClientSession;
  message: BusMessage;
  /** Ejecuta `cb` solo después de que la transacción hizo commit (p. ej. avisar por WebSocket). */
  afterCommit(cb: () => void | Promise<void>): void;
}

export type EventHandler = (event: DomainEvent, ctx: HandlerContext) => Promise<void>;

export interface EventConsumerOptions {
  bus: Bus;
  groupId: string;
  topics: string[];
  handler: EventHandler;
  idempotency?: ProcessedEvents;
  fromBeginning?: boolean;
  /** Esperas entre reintentos. Por defecto 1 s, 2 s, 4 s (3 reintentos). */
  backoffMs?: number[];
}

function defaultBackoff(): number[] {
  const env = process.env.RETRY_BACKOFF_MS;
  if (env) return env.split(",").map((n) => Number(n.trim()));
  return [1000, 2000, 4000];
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function startEventConsumer(opts: EventConsumerOptions): Promise<void> {
  const logger = new Logger(`Consumer[${opts.groupId}]`);
  await opts.bus.subscribe({
    groupId: opts.groupId,
    topics: opts.topics,
    fromBeginning: opts.fromBeginning,
    handler: (m) => processMessage(opts, m, logger),
  });
  logger.log(`Escuchando ${opts.topics.join(", ")}${opts.fromBeginning ? " (desde el inicio)" : ""}`);
}

/**
 * Procesa un mensaje con reintentos y DLQ (RNF-04):
 *  - JSON/contrato inválido  -> DLQ directo (reintentar no lo arregla).
 *  - Error al procesar       -> reintenta con espera creciente; si sigue fallando, DLQ.
 *  - En ambos casos retorna normal, así se confirma el offset y la partición sigue.
 *  - Solo lanza si no se pudo escribir en la DLQ (el mensaje se vuelve a entregar).
 */
export async function processMessage(
  opts: EventConsumerOptions,
  msg: BusMessage,
  logger: Logger = new Logger(`Consumer[${opts.groupId}]`),
): Promise<void> {
  let event: DomainEvent;
  try {
    const parsed: unknown = JSON.parse(msg.value);
    if (!isDomainEvent(parsed)) throw new Error("el mensaje no cumple el contrato de eventos");
    event = parsed;
  } catch (err) {
    logger.error(`Mensaje inválido en ${msg.topic}[${msg.partition}]@${msg.offset}: ${(err as Error).message}`);
    await sendToDlq(opts, msg, err as Error);
    return;
  }

  const backoff = opts.backoffMs ?? defaultBackoff();
  let lastError: Error = new Error("desconocido");
  for (let attempt = 0; attempt <= backoff.length; attempt++) {
    try {
      await runOnce(opts, event, msg, logger);
      return;
    } catch (err) {
      lastError = err as Error;
      if (attempt < backoff.length) {
        logger.warn(
          `Falló ${event.eventType} (${event.eventId}), intento ${attempt + 1}/${backoff.length + 1}: ${lastError.message}. Reintento en ${backoff[attempt]} ms`,
        );
        await sleep(backoff[attempt]);
      }
    }
  }
  logger.error(`${event.eventType} (${event.eventId}) falló ${backoff.length + 1} veces; va a la DLQ. Motivo: ${lastError.message}`);
  await sendToDlq(opts, msg, lastError);
}

async function runOnce(
  opts: EventConsumerOptions,
  event: DomainEvent,
  msg: BusMessage,
  logger: Logger,
): Promise<void> {
  const after: Array<() => void | Promise<void>> = [];
  const ctxBase = { message: msg, afterCommit: (cb: () => void | Promise<void>) => void after.push(cb) };

  if (opts.idempotency) {
    const result = await opts.idempotency.runOnce(event.eventId, async (session) => {
      after.length = 0; // si la transacción se reintenta, no acumular callbacks viejos
      await opts.handler(event, { ...ctxBase, session });
    });
    if (result === "duplicate") {
      logger.warn(`Evento duplicado descartado: ${event.eventType} (${event.eventId})`);
      return;
    }
  } else {
    await opts.handler(event, ctxBase);
  }
  for (const cb of after) await cb();
}

async function sendToDlq(opts: EventConsumerOptions, msg: BusMessage, err: Error): Promise<void> {
  await opts.bus.publish({
    topic: dlqTopic(msg.topic),
    key: msg.key,
    value: msg.value, // el mensaje original, intacto
    headers: {
      "x-error": err.message.slice(0, 500),
      "x-original-topic": msg.topic,
      "x-original-partition": String(msg.partition),
      "x-original-offset": msg.offset,
      "x-consumer-group": opts.groupId,
      "x-failed-at": new Date().toISOString(),
    },
  });
}
