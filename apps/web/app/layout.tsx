import type { Metadata } from "next";
import Link from "next/link";
import Script from "next/script";
import { ThemeToggle } from "./theme-toggle";
import "./globals.css";

export const metadata: Metadata = {
  title: "Research Radar",
  description: "Emerging and bridge papers in audio ML."
};

const navItems = [
  { href: "/", label: "Overview" },
  { href: "/search", label: "Search" },
  { href: "/recommended", label: "Recommended" },
  { href: "/trends", label: "Trends" },
  { href: "/evaluation", label: "Evaluation" }
];

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
      <body>
        <Script id="rr-theme-init" strategy="beforeInteractive">
          {themeInitScript}
        </Script>
        <div className="shell">
          <nav className="nav" aria-label="Primary">
            <div className="nav-links">
              {navItems.map((item) => (
                <Link key={item.href} href={item.href}>
                  {item.label}
                </Link>
              ))}
            </div>
            <ThemeToggle />
          </nav>
          {children}
        </div>
      </body>
    </html>
  );
}
