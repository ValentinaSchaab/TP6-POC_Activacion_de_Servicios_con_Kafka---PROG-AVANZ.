import "reflect-metadata";
import { Logger, ValidationPipe } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";
import { config } from "./config";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors(); // la Demo UI (otro puerto) llama a esta API
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  await app.listen(config.port);
  new Logger("Bootstrap").log(`activation-api escuchando en http://localhost:${config.port}`);
}
bootstrap();
