#!/usr/bin/env python3
"""Extrait les emails de toutes les pages de chaque site (même domaine).

Usage: python crawl_emails.py agences_immobilieres_final.csv --shard 0 --shards 10 --max-minutes 320

Sorties (dans results/) :
  emails_<shard>.csv       siren,nom,site_web,email,page_url
  done_<shard>.txt         sirens déjà traités (checkpoint → reprise auto)
  failed_<shard>.txt       sites injoignables (siren,raison)
"""
import argparse, asyncio, csv, html, re, time
import urllib.parse as up
from pathlib import Path

import httpx

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9\-]+(?:\.[a-zA-Z0-9\-]+)*\.[a-zA-Z]{2,}")
HREF_RE = re.compile(r'href=["\']([^"\'#\s]+)', re.I)
CF_RE = re.compile(r'data-cfemail="([0-9a-f]+)"')
AT_RE = re.compile(r"\s*[\[\(]\s*(?:at|arobase)\s*[\]\)]\s*", re.I)
BAD_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js", ".pdf", ".zip",
           ".ico", ".woff", ".woff2", ".ttf", ".mp4", ".mp3", ".xml", ".json", ".avif")
BAD_MAIL_DOMAINS = ("sentry", "wixpress", "example.", "domain.", "votredomaine", "email.com",
                    "yourdomain", "godaddy", "schema.org")
PRIORITY = ("contact", "mention", "legal", "equipe", "agence", "propos", "about", "cgv", "confidentialit", "honorair")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EmailExtractor/1.0)"}


def cf_decode(h: str) -> str:
    k = int(h[:2], 16)
    return "".join(chr(int(h[i:i + 2], 16) ^ k) for i in range(2, len(h), 2))


def emails_from(text: str) -> set:
    found = set()
    for m in CF_RE.findall(text):
        try:
            d = cf_decode(m).lower()
            if EMAIL_RE.fullmatch(d):
                found.add(d)
        except ValueError:
            pass
    text = up.unquote(html.unescape(text))
    text = AT_RE.sub("@", text)
    for e in EMAIL_RE.findall(text):
        e = e.lower().strip(".")
        if e.endswith(BAD_EXT) or "@" not in e:
            continue
        if any(b in e.split("@")[1] for b in BAD_MAIL_DOMAINS):
            continue
        found.add(e)
    return found


def norm_host(h: str) -> str:
    h = (h or "").lower().split(":")[0]
    return h[4:] if h.startswith("www.") else h


async def crawl_site(client, row, max_pages):
    start = row["site_web"].strip()
    if not start.startswith("http"):
        start = "http://" + start
    seen, queue, out, n, host = {start}, [start], {}, 0, None
    while queue and n < max_pages:
        url = queue.pop(0)
        try:
            r = await client.get(url)
        except Exception as e:
            if n == 0:
                return out, type(e).__name__
            continue
        n += 1
        if host is None:
            host = norm_host(r.url.host)
        if "html" not in r.headers.get("content-type", "html").lower():
            continue
        text = r.text
        for e in emails_from(text):
            out.setdefault(e, str(r.url))
        for href in HREF_RE.findall(text):
            if href.startswith(("mailto:", "tel:", "javascript:")):
                continue
            link = up.urljoin(str(r.url), href).split("#")[0]
            p = up.urlparse(link)
            if p.scheme not in ("http", "https") or norm_host(p.netloc) != host:
                continue
            if p.path.lower().endswith(BAD_EXT) or link in seen:
                continue
            seen.add(link)
            if any(k in link.lower() for k in PRIORITY):
                queue.insert(0, link)
            else:
                queue.append(link)
    return out, None if n else "no_page"


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--max-pages", type=int, default=60)
    ap.add_argument("--concurrency", type=int, default=40)
    ap.add_argument("--max-minutes", type=float, default=320)
    a = ap.parse_args()

    res = Path("results")
    res.mkdir(exist_ok=True)
    f_out, f_done, f_fail = (res / f"{n}_{a.shard}.{e}" for n, e in
                             (("emails", "csv"), ("done", "txt"), ("failed", "txt")))
    done = set(f_done.read_text().split()) if f_done.exists() else set()

    with open(a.csv, newline="", encoding="utf-8-sig") as f:
        rows = [r for i, r in enumerate(csv.DictReader(f)) if i % a.shards == a.shard]
    todo = [r for r in rows if r["siren"] not in done and r["site_web"].strip()]
    print(f"shard {a.shard}/{a.shards}: {len(rows)} sites, {len(done)} déjà faits, {len(todo)} à faire")

    new_file = not f_out.exists()
    fo = open(f_out, "a", newline="", encoding="utf-8")
    fd, ff = open(f_done, "a"), open(f_fail, "a")
    w = csv.writer(fo)
    if new_file:
        w.writerow(["siren", "nom", "site_web", "email", "page_url"])

    deadline = time.time() + a.max_minutes * 60
    sem = asyncio.Semaphore(a.concurrency)
    count = 0

    async with httpx.AsyncClient(headers=HEADERS, timeout=15, follow_redirects=True, verify=False,
                                 limits=httpx.Limits(max_connections=a.concurrency * 2)) as client:
        async def work(row):
            nonlocal count
            async with sem:
                if time.time() > deadline:
                    return
                try:
                    found, err = await asyncio.wait_for(crawl_site(client, row, a.max_pages), 180)
                except asyncio.TimeoutError:
                    found, err = {}, "timeout"
                for email, page in sorted(found.items()):
                    w.writerow([row["siren"], row["nom"], row["site_web"], email, page])
                if err:
                    ff.write(f"{row['siren']},{err}\n")
                fd.write(row["siren"] + "\n")
                for x in (fo, fd, ff):
                    x.flush()
                count += 1
                if count % 50 == 0:
                    print(f"{count}/{len(todo)}", flush=True)

        await asyncio.gather(*(work(r) for r in todo))
    print("terminé (ou budget temps atteint) — relancer pour reprendre")


if __name__ == "__main__":
    asyncio.run(main())
