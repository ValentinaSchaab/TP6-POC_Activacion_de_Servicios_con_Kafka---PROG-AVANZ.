import { Module } from "@nestjs/common";
import { ToolkitModule } from "@poc/kafka-toolkit";
import { config } from "./config";
import { LoyaltyConsumer } from "./loyalty.consumer";

@Module({
  imports: [
    ToolkitModule.forRoot({
      source: "loyalty",
      brokers: config.kafkaBrokers,
      clientId: config.kafkaClientId,
      mongoUri: config.mongoUri,
    }),
  ],
  providers: [LoyaltyConsumer],
})
export class AppModule {}
