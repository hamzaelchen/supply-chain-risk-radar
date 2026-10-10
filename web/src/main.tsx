import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { DataProvider } from "./data/context";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <DataProvider>
      <App />
    </DataProvider>
  </StrictMode>,
);
