/** Nombres de topics de negocio (convención: dominio.tipo). */
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];

/** Todos los topics de negocio (útil para crm-analytics, que los consume todos). */
export const ALL_TOPICS: Topic[] = Object.values(TOPICS);

/** Topic de mensajes muertos asociado a un topic de negocio. */
export const dlqTopic = (topic: string): string => `${topic}.dlq`;

/** Consumer groups (uno por servicio). */
export const CONSUMER_GROUPS = {
  BILLING: 'billing-svc',
  PROVISIONING: 'provisioning-svc',
  ACTIVATION_API: 'activation-api',
  NOTIFICATION: 'notification-svc',
  CRM_ANALYTICS: 'crm-analytics',
} as const;
