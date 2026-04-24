"use client";

import { useEffect, useState } from "react";

const STORAGE_KEY = "rr-theme";

type Theme = "light" | "dark";

function applyTheme(theme: Theme) {
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
}

export function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>("light");
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    const activeTheme =
      document.documentElement.dataset.theme === "dark" ? "dark" : "light";
    setTheme(activeTheme);
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!mounted) return;

    applyTheme(theme);
    try {
      window.localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      // Ignore storage failures and keep the session theme in memory.
    }
  }, [mounted, theme]);

  const nextTheme: Theme = theme === "dark" ? "light" : "dark";

  return (
    <button
      type="button"
      className="theme-toggle"
      onClick={() => setTheme(nextTheme)}
      aria-label={
        mounted
          ? `Switch to ${nextTheme} mode`
          : "Toggle between light mode and dark mode"
      }
      aria-pressed={mounted ? theme === "dark" : false}
      title={mounted ? `Switch to ${nextTheme} mode` : "Toggle theme"}
    >
      <span className="theme-toggle-icon" aria-hidden="true">
        {mounted ? (theme === "dark" ? "☼" : "◐") : "◐"}
      </span>
      <span className="theme-toggle-text">
        {mounted
          ? `${nextTheme[0].toUpperCase()}${nextTheme.slice(1)} mode`
          : "Theme"}
      </span>
    </button>
  );
}
