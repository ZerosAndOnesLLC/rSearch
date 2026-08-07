import type { Metadata } from "next";
import { IBM_Plex_Mono, Inter } from "next/font/google";
import Script from "next/script";
import "./globals.css";
import { Nav } from "./nav";

const mono = IBM_Plex_Mono({
  weight: ["400", "500", "600"],
  subsets: ["latin"],
  variable: "--font-mono",
});
const body = Inter({ subsets: ["latin"], variable: "--font-body" });

export const metadata: Metadata = {
  title: "rSearch",
  description: "FIPS-compliant log search",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${mono.variable} ${body.variable}`}>
        {/* Runtime API base override — see public/env.js. */}
        <Script src="/env.js" strategy="beforeInteractive" />
        <div className="flex min-h-screen">
          <aside
            className="w-48 shrink-0 border-r p-4 flex flex-col gap-6"
            style={{ borderColor: "var(--line)" }}
          >
            <div className="mono text-lg font-semibold tracking-tight">
              r<span style={{ color: "var(--accent)" }}>Search</span>
            </div>
            <Nav />
            <div className="mt-auto text-xs" style={{ color: "var(--muted)" }}>
              <span className="mono">FIPS 140-3</span> validated crypto
            </div>
          </aside>
          <main className="flex-1 p-6 min-w-0">{children}</main>
        </div>
      </body>
    </html>
  );
}
