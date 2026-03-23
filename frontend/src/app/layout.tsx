import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { AuthProvider } from "@/context/auth-context";

const inter = Inter({ subsets: ["latin"], variable: "--font-sans" });

export const metadata: Metadata = {
  title: "Deal Dashboard",
  description: "Private deal-finding dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.variable} min-h-screen bg-zinc-950 text-zinc-100 antialiased`}>
        <AuthProvider>{children}</AuthProvider>
      </body>
    </html>
  );
}
