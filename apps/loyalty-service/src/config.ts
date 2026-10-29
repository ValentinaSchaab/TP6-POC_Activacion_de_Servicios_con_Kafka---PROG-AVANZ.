export const config = {
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? "loyalty-service",
  groupId: process.env.KAFKA_GROUP_ID ?? process.env.KAFKA_GROUP_ID ?? "loyalty",
  // directConnection=true: para correr fuera de Docker contra el replica set (que se anuncia como "mongo:27017").
  mongoUri: process.env.MONGO_URI ?? "mongodb://localhost:27017/loyalty?directConnection=true",

};

export const randomBetween = (min: number, max: number) => min + Math.floor(Math.random() * (max - min + 1));
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
