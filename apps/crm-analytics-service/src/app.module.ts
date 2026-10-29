import { Module } from "@nestjs/common";
import { ToolkitModule } from "@poc/kafka-toolkit";
import { config } from "./config";
import { CrmAnalyticsConsumer } from "./crm-analytics.consumer";

@Module({
  imports: [
    ToolkitModule.forRoot({
      source: "crm-analytics",
      brokers: config.kafkaBrokers,
      clientId: config.kafkaClientId,
      mongoUri: config.mongoUri,
    }),
  ],
  providers: [CrmAnalyticsConsumer],
})
export class AppModule {}
