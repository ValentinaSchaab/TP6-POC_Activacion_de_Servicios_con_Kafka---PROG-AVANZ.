/** Configuración leída de variables de entorno (con valores para correr fuera de Docker). */
export const config = {
  port: Number(process.env.PORT ?? 3000),
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? "activation-api",
  groupId: process.env.KAFKA_GROUP_ID ?? "activation-api",
  // directConnection=true: el replica set se anuncia como "mongo:27017", que desde
  // tu máquina no se resuelve. Con esto el driver se conecta directo a localhost.
  mongoUri: process.env.MONGO_URI ?? "mongodb://localhost:27017/activation_api?directConnection=true",
};
