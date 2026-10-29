import { Module } from "@nestjs/common";
import { ToolkitModule } from "@poc/kafka-toolkit";
import { config } from "./config";
import { BillingConsumer } from "./billing.consumer";

@Module({
  imports: [
    ToolkitModule.forRoot({
      source: "billing",
      brokers: config.kafkaBrokers,
      clientId: config.kafkaClientId,
      mongoUri: config.mongoUri,
    }),
  ],
  providers: [BillingConsumer],
})
export class AppModule {}
