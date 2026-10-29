# POC Activación de Servicios con Kafka

Un cliente contrata un plan desde una UI en React; eso publica **un solo evento** en Kafka y
dispara facturación, aprovisionamiento, notificación y registro en CRM, sin que ningún
servicio llame directamente a otro.

```
Demo UI ──POST /activations──▶ activation-api ──ActivationRequested──▶ Kafka
                                     ▲                                   │
                                     │ billing.events / provisioning.events
                                     │                    ┌──────────────┼───────────────┐
                                     └──── resultados ◀── billing   provisioning   crm-analytics
                                                          (cuenta)   (línea 1-3 s)   (escucha todo)
              ActivationCompleted / ActivationFailed ───▶ notification (email a Mailhog)
```

## Requisitos

- Docker Desktop abierto
- Node.js 20+ y `pnpm` (solo para correr fuera de Docker)

## Opción A — Todo con Docker (RNF-01)

```powershell
docker compose up -d --build
docker compose ps -a
```

La primera vez tarda varios minutos (construye las imágenes). Después:

| Qué | URL |
|---|---|
| **Demo UI** | http://localhost:5173 |
| API | http://localhost:3000 |
| Kafka UI | http://localhost:8080 |
| Mailhog (emails) | http://localhost:8025 |

Apagar: `docker compose down` (con `-v` también borra los datos de Mongo).

## Opción B — Infraestructura en Docker, servicios en tu máquina

Es más rápida para desarrollar y para ver los logs de cada servicio.

```powershell
docker compose up -d kafka kafka-init kafka-ui mongo mailhog
pnpm install
pnpm run build:services
pnpm run start:services     # activation-api + billing + provisioning + notification + crm
pnpm run dev:ui             # en otra terminal: http://localhost:5173
```

## Qué hay en cada carpeta

```
apps/
  activation-api/          REST + WebSocket + agregador de la saga
  billing-service/         crea la cuenta; la anula si la activación falla (compensación)
  provisioning-service/    aprovisiona (retardo 1-3 s); puede fallar a pedido
  notification-service/    email de bienvenida o de error a Mailhog
  crm-analytics-service/   consume todos los topics y registra cada evento
  loyalty-service/         escenario 5: consumidor nuevo que lee el historial
  demo-ui/                 React + Vite + socket.io-client
libs/
  contracts/               tipos de eventos, sobre común, nombres de topics, versión
  kafka-toolkit/           cliente Kafka aislado + idempotencia, reintentos, DLQ
docker/                    create-topics.sh y Dockerfiles
docs/demo-script.md        guión de la demo (5 escenarios)
```

## Cómo se cumplen los requisitos de confiabilidad

| Requisito | Dónde / cómo |
|---|---|
| RNF-02 orden por cliente | `EventPublisher` usa `customerId` como clave del mensaje |
| RNF-03 idempotencia | `ProcessedEvents.runOnce`: inserta el `eventId` en `processed_events` **en la misma transacción de Mongo** que el efecto. Los eventos que un servicio publica en respuesta llevan un `eventId` determinístico (`deriveEventId`), así un reproceso no genera duplicados |
| RNF-04 reintentos + DLQ | `processMessage`: 3 reintentos con espera 1 s / 2 s / 4 s; si sigue fallando va a `<topic>.dlq` con el motivo en los headers. Los mensajes que no cumplen el contrato van directo a la DLQ |
| RNF-05 servicio caído | commit de offset **manual, después de procesar** (`autoCommit: false`); al volver, retoma desde el último offset confirmado |
| RNF-06 correlationId | todo evento lo lleva (= `activationId`); `event_log` del CRM lo indexa |
| RNF-07 contratos versionados | `libs/contracts`, campo `version` en el sobre |

## Decisiones que conviene saber (y defender)

- **Cliente Kafka**: el documento propone `@nestjs/microservices`. Se implementó con **kafkajs directo
  dentro de `libs/kafka-toolkit`** (`KafkaBus`, el único archivo que conoce kafkajs). Motivo: el commit
  manual de offsets, los reintentos con espera y la DLQ por consumidor se controlan de forma explícita,
  y queda cumplida la mitigación de riesgo del propio documento ("aislar el cliente para poder
  reemplazarlo").
- **Publicación directa** (fases 1 a 3): se guarda en Mongo y después se publica. Si `activation-api` se
  cae justo en medio, el evento se pierde. Es el riesgo documentado que resuelve el **outbox
  transaccional (fase 4, opcional, no implementado)**.
- **Mongo 4.4**: última versión que corre en CPUs sin AVX; soporta transacciones en replica set.
- **Colecciones**: Mongo no permite crear colecciones dentro de una transacción, por eso cada servicio las
  crea al arrancar (`ensureCollections`).

## Problemas comunes

- `kafka` tarda en estar *healthy* (hasta ~2 min en equipos lentos): es normal; los servicios esperan y
  se reintentan solos.
- Un servicio sale con error al arrancar: `docker compose logs <servicio>`; se reinicia solo
  (`restart: unless-stopped`).
- Puerto ocupado (3000, 5173, 8080, 8025, 9092, 27017): cerrá lo que lo use.
