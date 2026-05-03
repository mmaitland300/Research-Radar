import type { Metadata, Viewport } from "next";
import { IBM_Plex_Mono, IBM_Plex_Sans, Source_Serif_4 } from "next/font/google";
import Link from "next/link";
import Script from "next/script";
import { SiteNav } from "./site-nav";
import "./globals.css";

export const metadata: Metadata = {
  title: "Research Radar",
  description:
    "Emerging, bridge-candidate, and undercited papers in a curated MIR + audio-ML corpus.",
  manifest: "/site.webmanifest",
  icons: {
    icon: [
      { url: "/favicon.ico", sizes: "16x16 32x32" },
      { url: "/favicon.svg", type: "image/svg+xml" }
    ],
    apple: [{ url: "/icon.svg", type: "image/svg+xml" }]
  }
};

export const viewport: Viewport = {
  themeColor: "#0B1F3B"
};

const navItems = [
  { href: "/", label: "Overview" },
  { href: "/search", label: "Search" },
  { href: "/recommended", label: "Recommended" },
  { href: "/trends", label: "Trends" },
  { href: "/evaluation", label: "Evaluation" }
];

const displayFont = Source_Serif_4({
  subsets: ["latin"],
  variable: "--font-display",
  weight: ["400", "500", "600", "700"]
});

const uiFont = IBM_Plex_Sans({
  subsets: ["latin"],
  variable: "--font-ui",
  weight: ["400", "500", "600", "700"]
});

const monoFont = IBM_Plex_Mono({
  subsets: ["latin"],
  variable: "--font-mono",
  weight: ["400", "500", "600"]
});

const themeInitScript = `
  (() => {
    try {
      const storageKey = "rr-theme";
      const stored = window.localStorage.getItem(storageKey);
      const theme =
        stored === "light" || stored === "dark"
          ? stored
          : (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
      document.documentElement.dataset.theme = theme;
      document.documentElement.style.colorScheme = theme;
    } catch {
      document.documentElement.dataset.theme = "light";
      document.documentElement.style.colorScheme = "light";
    }
  })();
`;

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`${displayFont.variable} ${uiFont.variable} ${monoFont.variable}`}>
        <Script id="rr-theme-init" strategy="beforeInteractive">
          {themeInitScript}
        </Script>
        <div className="shell">
          <header className="site-header">
            <div className="masthead">
              <div className="brand-block">
                <Link className="brand-mark" href="/">
                  <span className="brand-kicker">Audio ML paper discovery</span>
                  <span className="brand-name">Research Radar</span>
                </Link>
                <p className="brand-copy">
                  Detect emerging, bridge-candidate, and undercited papers inside a curated audio-ML corpus,
                  then expose the signals behind every recommendation.
                </p>
              </div>
              <div className="status-cluster" aria-label="Product focus">
                <span className="stamp">Curated corpus</span>
                <span className="stamp">Explainable ranking</span>
                <span className="stamp">Emerging, bridge-candidate, and undercited papers</span>
              </div>
            </div>
            <SiteNav items={navItems} />
          </header>
          <div className="shell-content">
            {children}
          </div>
        </div>
      </body>
    </html>
  );
}
