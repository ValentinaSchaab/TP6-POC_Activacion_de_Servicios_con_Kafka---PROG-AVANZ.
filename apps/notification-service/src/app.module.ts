import { Module } from "@nestjs/common";
import { ToolkitModule } from "@poc/kafka-toolkit";
import { config } from "./config";
import { NotificationConsumer } from "./notification.consumer";

@Module({
  imports: [
    ToolkitModule.forRoot({
      source: "notification",
      brokers: config.kafkaBrokers,
      clientId: config.kafkaClientId,
      mongoUri: config.mongoUri,
    }),
  ],
  providers: [NotificationConsumer],
})
export class AppModule {}
