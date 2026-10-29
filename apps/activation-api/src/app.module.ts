import { Module } from "@nestjs/common";
import { ToolkitModule } from "@poc/kafka-toolkit";
import { ActivationsController } from "./activations/activations.controller";
import { ActivationsGateway } from "./activations/activations.gateway";
import { ActivationsRepository } from "./activations/activations.repository";
import { ActivationsService } from "./activations/activations.service";
import { config } from "./config";
import { AggregatorConsumer } from "./saga/aggregator.consumer";

@Module({
  imports: [
    ToolkitModule.forRoot({
      source: "activation-api",
      brokers: config.kafkaBrokers,
      clientId: config.kafkaClientId,
      mongoUri: config.mongoUri,
    }),
  ],
  controllers: [ActivationsController],
  providers: [ActivationsRepository, ActivationsGateway, ActivationsService, AggregatorConsumer],
})
export class AppModule {}
