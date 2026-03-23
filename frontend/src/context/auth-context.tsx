"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { fetchMe, type User as ApiUser } from "@/lib/api";

const TOKEN_KEY = "deal_dashboard_token";

export type User = ApiUser;

type AuthContextValue = {
  token: string | null;
  user: User | null;
  ready: boolean;
  setSession: (token: string, user: User) => void;
  logout: () => void;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setToken] = useState<string | null>(null);
  const [user, setUser] = useState<User | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    const t = typeof window !== "undefined" ? localStorage.getItem(TOKEN_KEY) : null;
    setToken(t);
    if (!t) {
      setReady(true);
      return;
    }
    fetchMe(t)
      .then(setUser)
      .catch(() => {
        localStorage.removeItem(TOKEN_KEY);
        setToken(null);
        setUser(null);
      })
      .finally(() => setReady(true));
  }, []);

  const setSession = useCallback((tok: string, u: User) => {
    localStorage.setItem(TOKEN_KEY, tok);
    setToken(tok);
    setUser(u);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem(TOKEN_KEY);
    setToken(null);
    setUser(null);
  }, []);

  const value = useMemo(
    () => ({ token, user, ready, setSession, logout }),
    [token, user, ready, setSession, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}

export { TOKEN_KEY };
