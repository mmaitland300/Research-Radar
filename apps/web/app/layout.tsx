import type { Metadata } from "next";
import Link from "next/link";
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

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <div className="shell">
          <nav className="nav" aria-label="Primary">
            {navItems.map((item) => (
              <Link key={item.href} href={item.href}>
                {item.label}
              </Link>
            ))}
          </nav>
          {children}
        </div>
      </body>
    </html>
  );
}
