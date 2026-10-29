import { ClientSession, Collection, Db, MongoClient } from "mongodb";

const SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60; // igual a la retención de los topics

interface ProcessedDoc {
  _id: string; // eventId (índice único)
  at: Date;
}

/**
 * Idempotencia (RNF-03): el eventId se inserta en `processed_events` EN LA MISMA
 * TRANSACCIÓN que el efecto del evento. Si el efecto falla, el eventId tampoco queda
 * registrado y el reintento vuelve a procesar. Si el evento ya estaba, se descarta.
 */
export class ProcessedEvents {
  private readonly col: Collection<ProcessedDoc>;

  constructor(private readonly client: MongoClient, db: Db) {
    this.col = db.collection<ProcessedDoc>("processed_events");
  }

  async init(): Promise<void> {
    await this.col.createIndex({ at: 1 }, { expireAfterSeconds: SEVEN_DAYS_SECONDS });
  }

  async runOnce(
    eventId: string,
    work: (session: ClientSession) => Promise<void>,
  ): Promise<"processed" | "duplicate"> {
    const session = this.client.startSession();
    try {
      let duplicate = false;
      await session.withTransaction(async () => {
        duplicate = false;
        const seen = await this.col.findOne({ _id: eventId }, { session });
        if (seen) {
          duplicate = true;
          return;
        }
        await this.col.insertOne({ _id: eventId, at: new Date() }, { session });
        await work(session);
      });
      return duplicate ? "duplicate" : "processed";
    } finally {
      await session.endSession();
    }
  }
}
