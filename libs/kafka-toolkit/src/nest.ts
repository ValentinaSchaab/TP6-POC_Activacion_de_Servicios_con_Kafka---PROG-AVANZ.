import { DynamicModule, Global, Module } from "@nestjs/common";
import { ServiceName } from "@poc/contracts";
import { Bus } from "./bus";
import { ProcessedEvents } from "./idempotency";
import { KafkaBus } from "./kafka-bus";
import { MongoHolder } from "./mongo";
import { EventPublisher } from "./publisher";

/** Token para inyectar el Bus: @Inject(BUS) bus: Bus */
export const BUS = "POC_KAFKA_BUS";

export interface ToolkitOptions {
  source: ServiceName;
  brokers: string[];
  clientId: string;
  mongoUri: string;
}

/** Módulo global: cada servicio lo importa una vez y obtiene Bus, Mongo, idempotencia y publisher. */
@Global()
@Module({})
export class ToolkitModule {
  static forRoot(o: ToolkitOptions): DynamicModule {
    return {
      module: ToolkitModule,
      providers: [
        { provide: BUS, useFactory: (): Bus => new KafkaBus({ brokers: o.brokers, clientId: o.clientId }) },
        { provide: MongoHolder, useFactory: () => MongoHolder.connect(o.mongoUri) },
        {
          provide: ProcessedEvents,
          useFactory: async (m: MongoHolder) => {
            const p = new ProcessedEvents(m.client, m.db);
            await p.init();
            return p;
          },
          inject: [MongoHolder],
        },
        { provide: EventPublisher, useFactory: (bus: Bus) => new EventPublisher(bus), inject: [BUS] },
      ],
      exports: [BUS, MongoHolder, ProcessedEvents, EventPublisher],
    };
  }
}
