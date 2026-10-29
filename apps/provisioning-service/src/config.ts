      export const config = {
        kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
        kafkaClientId: process.env.KAFKA_CLIENT_ID ?? "provisioning-service",
        groupId: process.env.KAFKA_GROUP_ID ?? process.env.KAFKA_GROUP_ID ?? "provisioning-svc",
        // directConnection=true: para correr fuera de Docker contra el replica set (que se anuncia como "mongo:27017").
        mongoUri: process.env.MONGO_URI ?? "mongodb://localhost:27017/provisioning?directConnection=true",
      delayMinMs: Number(process.env.PROVISIONING_DELAY_MIN_MS ?? 1000),
delayMaxMs: Number(process.env.PROVISIONING_DELAY_MAX_MS ?? 3000),
      };

      export const randomBetween = (min: number, max: number) => min + Math.floor(Math.random() * (max - min + 1));
      export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
