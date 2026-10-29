import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { Socket, io } from "socket.io-client";

const API_URL = (import.meta.env.VITE_API_URL as string | undefined) ?? "http://localhost:3000";
const KAFKA_UI_URL = (import.meta.env.VITE_KAFKA_UI_URL as string | undefined) ?? "http://localhost:8080";
const MAILHOG_URL = (import.meta.env.VITE_MAILHOG_URL as string | undefined) ?? "http://localhost:8025";

type Status = "PENDING" | "IN_PROGRESS" | "ACTIVE" | "FAILED";
interface Step { status: "OK" | "FAILED"; at: string }
interface HistoryEntry { eventType: string; at: string; source?: string; detail?: string }
interface Activation {
  activationId: string;
  customerId: string;
  planId: string;
  status: Status;
  simulateFailure: string;
  steps: { billing?: Step; provisioning?: Step };
  history: HistoryEntry[];
}

const PLANS = ["Plan Digital", "Plan Pro", "Plan Familiar"];
const FAILURES = [
  { value: "none", label: "Ninguno (camino feliz)" },
  { value: "billing", label: "Forzar fallo en billing" },
  { value: "provisioning", label: "Forzar fallo en provisioning" },
];

/** Color de cada evento en la timeline. */
function tone(eventType: string): "ok" | "bad" | "warn" | "info" {
  if (eventType.includes("Failed")) return "bad";
  if (eventType.includes("Cancelled")) return "warn";
  if (eventType === "ActivationCompleted" || eventType === "BillingAccountCreated" || eventType === "ProvisioningCompleted") return "ok";
  return "info";
}

const time = (iso: string) => new Date(iso).toLocaleTimeString("es-AR", { hour12: false }) + "." + String(new Date(iso).getMilliseconds()).padStart(3, "0");

export default function App() {
  const [customerId, setCustomerId] = useState("C-1234");
  const [planId, setPlanId] = useState(PLANS[0]);
  const [simulateFailure, setSimulateFailure] = useState("none");
  const [activations, setActivations] = useState<Record<string, Activation>>({});
  const [order, setOrder] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const socketRef = useRef<Socket | null>(null);
  const knownIds = useRef<string[]>([]);

  useEffect(() => {
    const socket = io(API_URL, { transports: ["websocket"] });
    socketRef.current = socket;
    socket.on("connect", () => {
      setConnected(true);
      knownIds.current.forEach((id) => socket.emit("subscribe", id)); // re-suscribir tras reconectar
    });
    socket.on("disconnect", () => setConnected(false));
    socket.on("activation.updated", (a: Activation) => setActivations((prev) => ({ ...prev, [a.activationId]: a })));
    return () => { socket.close(); };
  }, []);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setSending(true);
    try {
      const res = await fetch(`${API_URL}/activations`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ customerId, planId, simulateFailure }),
      });
      if (!res.ok) throw new Error(`La API respondió ${res.status}`);
      const { activationId } = (await res.json()) as { activationId: string };
      knownIds.current.push(activationId);
      setOrder((o) => [activationId, ...o]);
      setSelected(activationId);
      socketRef.current?.emit("subscribe", activationId);
    } catch (err) {
      setError((err as Error).message + " — ¿está corriendo activation-api en " + API_URL + "?");
    } finally {
      setSending(false);
    }
  }

  const current = selected ? activations[selected] : undefined;
  const sorted = useMemo(() => (current ? [...current.history].sort((a, b) => a.at.localeCompare(b.at)) : []), [current]);

  return (
    <div className="page">
      <header>
        <h1>POC Activación de Servicios con Kafka</h1>
        <p className="sub">Un solo evento dispara toda la activación. Ningún servicio llama directamente a otro.</p>
        <div className="links">
          <span className={`dot ${connected ? "on" : "off"}`} /> {connected ? "WebSocket conectado" : "WebSocket desconectado"}
          <a href={KAFKA_UI_URL} target="_blank" rel="noreferrer">Kafka UI ↗</a>
          <a href={MAILHOG_URL} target="_blank" rel="noreferrer">Mailhog ↗</a>
        </div>
      </header>

      <main>
        <section className="card">
          <h2>Contratar plan</h2>
          <form onSubmit={onSubmit}>
            <label>Cliente (customerId)
              <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required />
            </label>
            <label>Plan
              <select value={planId} onChange={(e) => setPlanId(e.target.value)}>
                {PLANS.map((p) => <option key={p}>{p}</option>)}
              </select>
            </label>
            <label>Fallo forzado (solo demo)
              <select value={simulateFailure} onChange={(e) => setSimulateFailure(e.target.value)}>
                {FAILURES.map((f) => <option key={f.value} value={f.value}>{f.label}</option>)}
              </select>
            </label>
            <button type="submit" disabled={sending || !customerId.trim()}>{sending ? "Enviando…" : "Contratar plan"}</button>
            {error && <p className="error">{error}</p>}
          </form>

          <h3>Activaciones de esta sesión</h3>
          {order.length === 0 && <p className="muted">Todavía no contrataste ningún plan.</p>}
          <ul className="list">
            {order.map((id) => {
              const a = activations[id];
              return (
                <li key={id} className={id === selected ? "sel" : ""} onClick={() => setSelected(id)}>
                  <code>{id}</code>
                  <span>{a ? `${a.customerId} · ${a.planId}` : "…"}</span>
                  <span className={`badge ${a?.status ?? "PENDING"}`}>{a?.status ?? "PENDING"}</span>
                </li>
              );
            })}
          </ul>
        </section>

        <section className="card wide">
          <h2>Timeline de eventos en vivo</h2>
          {!current && <p className="muted">Contratá un plan para ver los eventos aparecer en tiempo real.</p>}
          {current && (
            <>
              <div className="summary">
                <code>{current.activationId}</code>
                <span className={`badge ${current.status}`}>{current.status}</span>
                <span className="step">billing: <b className={current.steps.billing?.status ?? "wait"}>{current.steps.billing?.status ?? "…"}</b></span>
                <span className="step">provisioning: <b className={current.steps.provisioning?.status ?? "wait"}>{current.steps.provisioning?.status ?? "…"}</b></span>
              </div>
              <ol className="timeline">
                {sorted.map((h, i) => (
                  <li key={i} className={tone(h.eventType)}>
                    <div className="t">{time(h.at)}</div>
                    <div className="body">
                      <b>{h.eventType}</b>
                      <span className="src">{h.source}</span>
                      {h.detail && <div className="detail">{h.detail}</div>}
                    </div>
                  </li>
                ))}
                {(current.status === "PENDING" || current.status === "IN_PROGRESS") && (
                  <li className="pending"><div className="t">…</div><div className="body muted">esperando a los demás servicios</div></li>
                )}
              </ol>
            </>
          )}
        </section>
      </main>
    </div>
  );
}
