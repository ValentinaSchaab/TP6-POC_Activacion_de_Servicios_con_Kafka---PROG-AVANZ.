import { ClientSession, Db, MongoClient } from "mongodb";

/** Conexión a Mongo compartida por servicio (Nest llama onModuleDestroy al cerrar). */
export class MongoHolder {
  private constructor(readonly client: MongoClient, readonly db: Db) {}

  static async connect(uri: string): Promise<MongoHolder> {
    const client = new MongoClient(uri);
    await client.connect();
    return new MongoHolder(client, client.db());
  }

  /**
   * Crea las colecciones si no existen. Las transacciones de Mongo no pueden crear
   * colecciones nuevas (en 4.2 falla), así que cada servicio las crea al arrancar.
   */
  async ensureCollections(...names: string[]): Promise<void> {
    for (const name of names) {
      try {
        await this.db.createCollection(name);
      } catch (err) {
        if ((err as { code?: number }).code !== 48) throw err; // 48 = ya existe
      }
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.client.close();
  }
}

export type { ClientSession };
