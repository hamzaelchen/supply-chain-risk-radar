import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// base relative : le site peut être servi depuis un sous-dossier (GitHub Pages).
export default defineConfig({
  base: "./",
  plugins: [react()],
  build: { target: "es2022" },
  test: { environment: "node", include: ["src/**/*.test.ts"] },
});
