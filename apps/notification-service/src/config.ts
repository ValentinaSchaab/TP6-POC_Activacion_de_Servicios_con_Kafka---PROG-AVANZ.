      export const config = {
        kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
        kafkaClientId: process.env.KAFKA_CLIENT_ID ?? "notification-service",
        groupId: process.env.KAFKA_GROUP_ID ?? process.env.KAFKA_GROUP_ID ?? "notification-svc",
        // directConnection=true: para correr fuera de Docker contra el replica set (que se anuncia como "mongo:27017").
        mongoUri: process.env.MONGO_URI ?? "mongodb://localhost:27017/notification?directConnection=true",
      smtpHost: process.env.SMTP_HOST ?? "localhost",
smtpPort: Number(process.env.SMTP_PORT ?? 1025),
mailFrom: process.env.MAIL_FROM ?? "no-reply@activation-poc.local",
      };

      export const randomBetween = (min: number, max: number) => min + Math.floor(Math.random() * (max - min + 1));
      export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
