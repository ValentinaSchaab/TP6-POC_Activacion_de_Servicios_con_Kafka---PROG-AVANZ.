# Guión de la demo (15 minutos)

Pantalla dividida: **Demo UI** (http://localhost:5173) a la izquierda, **Kafka UI** (http://localhost:8080) a la derecha.

| # | Escenario | Qué se hace | Qué se ve | Concepto |
|---|---|---|---|---|
| 1 | Camino feliz | Contratar un plan sin fallo | Timeline con `ActivationRequested`, `BillingAccountCreated`, `ProvisioningCompleted`, `ActivationCompleted` en verde; email en Mailhog | Fan-out: un evento, varias reacciones |
| 2 | Fallo y compensación | Contratar con fallo en provisioning | `ProvisioningFailed`, `ActivationFailed`, `BillingAccountCancelled`; email de error | Saga sin llamadas directas |
| 3 | Servicio caído | Detener notification, crear 3 activaciones, volver a levantarlo | Kafka UI muestra lag de 3; al volver, llegan los 3 emails | Offset: nadie pierde mensajes |
| 4 | Escalar | Levantar una segunda instancia de billing | Kafka UI reparte las 3 particiones entre 2 consumidores | Consumer group y particiones |
| 5 | Nuevo consumidor | Levantar loyalty con un group nuevo que lee desde el inicio | Procesa todo el historial sin tocar a los demás | Replay del log |

## Comandos

```powershell
# Escenario 3
docker compose stop notification-service
#   ... crear 3 activaciones en la UI; en Kafka UI > Consumers > notification-svc se ve el lag ...
docker compose start notification-service

# Escenario 4 (el rebalanceo tarda unos segundos: explicarlo)
docker compose up -d --scale billing-service=2
#   Kafka UI > Consumers > billing-svc: 2 miembros repartiéndose las particiones

# Escenario 5 (loyalty usa auto.offset.reset=earliest)
docker compose --profile loyalty up -d loyalty-service
docker compose logs loyalty-service
```

## Extras para mostrar

- **Idempotencia**: en Kafka UI, *Topics > activation.events > Messages*, copiar un mensaje y volver a producirlo
  (*Produce Message*): no se genera otro email ni otra cuenta. El log de notification dice "Evento duplicado descartado".
- **DLQ**: producir en `activation.requested` un mensaje que no sea JSON válido: aparece en `activation.requested.dlq`
  con el motivo en los headers (`x-error`), y las activaciones siguientes siguen funcionando.
- **Trazabilidad**: todos los eventos de una activación comparten `correlationId` (= `activationId`).
- **Orden por cliente**: crear varias activaciones con el mismo `customerId`; en Kafka UI van siempre a la misma partición.

## Plan B

Grabar un video de los 5 escenarios por si falla la red o Docker en vivo.
