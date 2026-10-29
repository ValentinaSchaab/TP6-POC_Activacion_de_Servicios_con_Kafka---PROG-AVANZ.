import "reflect-metadata";
import { Logger } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();
  new Logger("Bootstrap").log("billing-service iniciado");
}
bootstrap().catch((err) => {
  console.error(err);
  process.exit(1); // Docker (restart: unless-stopped) lo vuelve a levantar
});
