import { useEffect, useRef, useState } from "react";

export function useCerfaWebSocket() {
  const ws = useRef<WebSocket | null>(null);
  const [step, setStep] = useState(0);
  const [label, setLabel] = useState("");
  const [status, setStatus] = useState<"idle" | "running" | "waiting_user" | "done" | "error">("idle");
  const [preanalyse, setPreanalyse] = useState<any>(null);
  const [cerfa, setCerfa] = useState<any>(null);

  const start = (pdfBase64: string) => {
    ws.current = new WebSocket("ws://localhost:5002/ws/pipeline");

    ws.current.onopen = () => {
      setStep(1);
      setStatus("running");
      setLabel("Pré-analyse du CERFA…");

      ws.current?.send(
        JSON.stringify({
          action: "start_preanalyse",
          pdf: pdfBase64,
        })
      );
    };

    ws.current.onmessage = (event) => {
      const msg = JSON.parse(event.data);

      if (msg.event === "progress") {
        setStep(msg.step);
        setLabel(msg.label);
      }

      if (msg.event === "preanalyse_result") {
        setPreanalyse(msg.preanalyse);
        setStatus("waiting_user");
        setLabel("En attente validation utilisateur");
      }

      if (msg.event === "cerfa_done") {
        setCerfa(msg.cerfa);
        setStep(3);
        setStatus("done");
      }
    };
  };

  const validatePreanalyse = (overrides: any) => {
    if (!ws.current) return;

    setStatus("running");
    setStep(2);
    setLabel("Analyse complète du CERFA…");

    ws.current.send(
      JSON.stringify({
        action: "confirm_preanalyse",
        ...overrides,
        pdf_path: preanalyse.pdf_path,
      })
    );
  };

  return {
    step,
    status,
    label,
    preanalyse,
    cerfa,
    start,
    validatePreanalyse,
  };
}
