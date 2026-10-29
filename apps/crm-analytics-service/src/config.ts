export const config = {
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? "crm-analytics-service",
  groupId: process.env.CRM_GROUP_ID ?? process.env.KAFKA_GROUP_ID ?? "crm-analytics",
  // directConnection=true: para correr fuera de Docker contra el replica set (que se anuncia como "mongo:27017").
  mongoUri: process.env.MONGO_URI ?? "mongodb://localhost:27017/crm_analytics?directConnection=true",

};

export const randomBetween = (min: number, max: number) => min + Math.floor(Math.random() * (max - min + 1));
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
