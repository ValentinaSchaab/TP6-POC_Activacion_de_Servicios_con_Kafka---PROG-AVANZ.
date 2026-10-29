import { Module } from "@nestjs/common";
import { ToolkitModule } from "@poc/kafka-toolkit";
import { config } from "./config";
import { ProvisioningConsumer } from "./provisioning.consumer";

@Module({
  imports: [
    ToolkitModule.forRoot({
      source: "provisioning",
      brokers: config.kafkaBrokers,
      clientId: config.kafkaClientId,
      mongoUri: config.mongoUri,
    }),
  ],
  providers: [ProvisioningConsumer],
})
export class AppModule {}
