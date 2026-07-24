"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { getToken, setToken } from "@/lib/api";

const PAGES = [
  { href: "/", label: "Search" },
  { href: "/streams", label: "Streams" },
  { href: "/alerts", label: "Alerts" },
  { href: "/access", label: "Access" },
];

export function Nav() {
  const pathname = usePathname();
  const router = useRouter();
  return (
    <nav className="flex flex-col gap-1">
      {PAGES.map((page) => (
        <Link
          key={page.href}
          href={page.href}
          className={`nav-link ${pathname === page.href ? "active" : ""}`}
        >
          {page.label}
        </Link>
      ))}
      <button
        className="nav-link text-left cursor-pointer"
        onClick={() => {
          setToken(null);
          router.push("/login");
        }}
      >
        {typeof window !== "undefined" && getToken() ? "Sign out" : "Sign in"}
      </button>
    </nav>
  );
}
