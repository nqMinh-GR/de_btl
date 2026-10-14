import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const apiProxy = {
  "/api": {
    target: "http://127.0.0.1:8000",
    changeOrigin: true,
  },
};

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    host: true,
    proxy: apiProxy,
  },
  preview: {
    port: 3000,
    host: "127.0.0.1",
    proxy: apiProxy,
  },
});
