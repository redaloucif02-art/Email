#!/usr/bin/env python3
"""Analyse contacts.jsonl avec gpt-oss-120b (Cerebras) -> enriched.jsonl

Pour chaque agence : gérant (nom, poste, email, tel) + agence (email, tel, adresse).
Toute valeur renvoyée par l'IA est revérifiée dans les données source : si elle n'y figure pas, elle est mise à null.

Clés : variable d'environnement CEREBRAS_API_KEYS (une clé par ligne ou séparées par des virgules).
La 1re clé sert d'abord ; si elle est refusée (crédit épuisé / invalide), on passe à la suivante.

Usage : python analyze_contacts.py contacts.jsonl -o enriched.jsonl [--limit 30] [--batch 12] [--workers 6]
"""
import argparse, json, os, re, sys, threading, time, unicodedata
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

URL = "https://api.cerebras.ai/v1/chat/completions"
MODEL = "gpt-oss-120b"
MAX_CHARS_BATCH = 24000      # ~7k tokens d'entrée par requête
MAX_CONTACTS = 40            # contacts gardés par agence dans le prompt
MAX_TXT = 600                # caractères de texte par contact

SYSTEM = """Tu extrais des coordonnées d'agences immobilières françaises à partir de données déjà collectées sur leur site.
Règles strictes :
- Utilise UNIQUEMENT les données fournies. N'invente rien. Si une info n'est pas clairement présente, mets null.
- Un email ou un téléphone doit être recopié tel quel depuis les données.
- "gerant" = dirigeant, gérant, directeur, fondateur ou responsable de l'agence (pas un simple négociateur ou assistant, sauf s'il est seul). Son email/tel personnel seulement s'ils lui sont clairement rattachés dans le même bloc, sinon null.
- "agence" = coordonnées générales de l'agence : email générique (contact@, agence@...), téléphone principal, adresse postale de l'agence elle-même.
- Ignore les adresses/emails qui ne sont pas ceux de l'agence (notaire, garant financier, préfecture, médiateur, hébergeur, éditeur du site).
- confiance : "haute" si les infos sont claires et cohérentes, "moyenne" si partielles/ambiguës, "faible" si presque rien d'exploitable.
Réponds UNIQUEMENT par du JSON valide, sans texte autour, de la forme :
{"resultats":[{"id":0,"gerant":{"nom":null,"poste":null,"email":null,"tel":null},"agence":{"email":null,"tel":null,"adresse":null},"confiance":"moyenne","remarques":""}]}
Un objet par agence fournie, avec le même "id"."""

lock = threading.Lock()


# ---------- clés ----------
class Keys:
    def __init__(self, raw):
        self.keys = [k.strip() for k in re.split(r"[\n,;]+", raw or "") if k.strip()]
        self.i = 0
        self.dead = set()
        self.l = threading.Lock()

    def current(self):
        with self.l:
            while self.i < len(self.keys) and self.i in self.dead:
                self.i += 1
            return (self.keys[self.i], self.i) if self.i < len(self.keys) else (None, None)

    def kill(self, idx, why):
        with self.l:
            if idx not in self.dead:
                self.dead.add(idx)
                print(f"[clé #{idx + 1}] désactivée ({why}) ; {len(self.keys) - len(self.dead)} restante(s)", file=sys.stderr)


class NoKeyLeft(Exception):
    pass


USAGE = {"in": 0, "out": 0, "calls": 0}


def call_api(keys, user_msg):
    """Appelle Cerebras. Gère 429 (attente) et 401/402/403 (changement de clé)."""
    body = {"model": MODEL, "temperature": 0.1, "max_completion_tokens": 12000,
            "reasoning_effort": "low",
            "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user_msg}]}
    for attempt in range(8):
        key, idx = keys.current()
        if key is None:
            raise NoKeyLeft()
        try:
            r = requests.post(URL, headers={"Authorization": f"Bearer {key}"}, json=body, timeout=180)
        except requests.RequestException:
            time.sleep(5 * (attempt + 1)); continue
        if r.status_code == 200:
            j = r.json()
            u = j.get("usage", {})
            with lock:
                USAGE["in"] += u.get("prompt_tokens", 0); USAGE["out"] += u.get("completion_tokens", 0); USAGE["calls"] += 1
            return j["choices"][0]["message"]["content"] or ""
        if r.status_code in (401, 402, 403):
            keys.kill(idx, f"HTTP {r.status_code}"); continue
        if r.status_code == 429:
            wait = float(r.headers.get("retry-after", 0) or 0) or min(60, 5 * (attempt + 1))
            time.sleep(wait); continue
        if r.status_code in (400, 422) and "reasoning_effort" in body:
            body.pop("reasoning_effort"); continue   # paramètre non accepté -> on retire
        if r.status_code >= 500:
            time.sleep(5 * (attempt + 1)); continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    raise RuntimeError("trop d'échecs")


# ---------- préparation ----------
def lean(r):
    cs = []
    for c in r["contacts"][:MAX_CONTACTS]:
        cs.append({"emails": [e["email"] for e in c["emails"]], "tels": c["phones"],
                   "texte": (c["card_text"] or "")[:MAX_TXT]})
    s = json.dumps(r.get("structured") or [], ensure_ascii=False)[:800]
    return {"agence": r["agence"], "site": r["site"], "donnees_structurees": s if s != "[]" else None, "contacts": cs}


def blob_of(r):
    return json.dumps(lean(r), ensure_ascii=False) + " " + " ".join(" ".join(c["addresses"]) for c in r["contacts"])


def strip_acc(t):
    t = unicodedata.normalize("NFKD", (t or "").lower())
    return "".join(c for c in t if not unicodedata.combining(c))


def digits(t):
    return re.sub(r"\D", "", t or "")


def phone_ok(p, blob_d):
    d = digits(p)
    if d.startswith("33") and len(d) == 11:
        d = "0" + d[2:]
    return len(d) >= 9 and (d in blob_d or ("33" + d[1:]) in blob_d)


def verify(res, r):
    """Revérifie chaque valeur dans les données source ; null + note si absente."""
    blob = blob_of(r)
    bl, ba, bd = blob.lower(), strip_acc(blob), digits(blob)
    notes = []
    g = res.get("gerant") or {}
    a = res.get("agence") or {}
    out_g = {k: g.get(k) or None for k in ("nom", "poste", "email", "tel")}
    out_a = {k: a.get(k) or None for k in ("email", "tel", "adresse")}
    for d_, k in ((out_g, "email"), (out_a, "email")):
        if d_[k] and d_[k].lower().strip() not in bl:
            notes.append(f"email {d_[k]} absent des données"); d_[k] = None
    for d_, k in ((out_g, "tel"), (out_a, "tel")):
        if d_[k] and not phone_ok(d_[k], bd):
            notes.append(f"tel {d_[k]} absent des données"); d_[k] = None
    if out_g["nom"]:
        parts = [p for p in re.split(r"[\s\-]+", strip_acc(out_g["nom"])) if len(p) >= 3]
        if not parts or not all(p in ba for p in parts):
            notes.append(f"nom {out_g['nom']} absent des données"); out_g["nom"] = out_g["poste"] = None
    if out_a["adresse"]:
        cp = re.findall(r"\b\d{5}\b", out_a["adresse"])
        if cp and cp[0] not in blob:
            notes.append("adresse : code postal absent des données"); out_a["adresse"] = None
    return {"gerant": out_g, "agence_contact": out_a, "confiance": res.get("confiance") or "faible",
            "remarques": res.get("remarques") or "", "verifs": notes}


def parse_json(txt):
    txt = re.sub(r"^```(?:json)?|```$", "", txt.strip(), flags=re.M).strip()
    try:
        return json.loads(txt)
    except ValueError:
        i, j = txt.find("{"), txt.rfind("}")
        return json.loads(txt[i:j + 1])


def analyze_batch(keys, batch):
    """batch = liste de lignes agence. Retourne {index: résultat}. Se coupe en deux si échec."""
    items = [dict(id=i, **lean(r)) for i, r in enumerate(batch)]
    msg = "Agences à analyser :\n" + json.dumps(items, ensure_ascii=False)
    for _ in range(2):
        try:
            data = parse_json(call_api(keys, msg))
            res = {x["id"]: x for x in data["resultats"] if isinstance(x, dict) and "id" in x}
            if all(i in res for i in range(len(batch))):
                return res
        except NoKeyLeft:
            raise
        except Exception as e:
            err = str(e)
    if len(batch) > 1:
        h = len(batch) // 2
        left = analyze_batch(keys, batch[:h])
        right = analyze_batch(keys, batch[h:])
        return {**left, **{i + h: v for i, v in right.items()}}
    return {0: {"erreur": "échec IA"}}


def make_batches(rows):
    batches, cur, size = [], [], 0
    for r in rows:
        n = len(json.dumps(lean(r), ensure_ascii=False))
        if cur and (size + n > MAX_CHARS_BATCH or len(cur) >= a_batch):
            batches.append(cur); cur, size = [], 0
        cur.append(r); size += n
    if cur:
        batches.append(cur)
    return batches


def main():
    global a_batch
    ap = argparse.ArgumentParser()
    ap.add_argument("inp")
    ap.add_argument("-o", "--out", default="enriched.jsonl")
    ap.add_argument("--limit", type=int, default=0, help="nombre d'agences à traiter (0 = toutes)")
    ap.add_argument("--batch", type=int, default=12, help="agences max par requête")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--dry-run", action="store_true", help="n'appelle pas l'API, affiche la taille des lots")
    a = ap.parse_args()
    a_batch = a.batch

    rows = [json.loads(l) for l in open(a.inp, encoding="utf-8") if l.strip()]
    done = set()
    if os.path.exists(a.out):
        for l in open(a.out, encoding="utf-8"):
            d = json.loads(l); done.add(d["siren"] + "|" + d["site"])
    todo = [r for r in rows if r["siren"] + "|" + r["site"] not in done]
    if a.limit:
        todo = todo[:a.limit]

    # agences sans contact : pas d'appel IA
    vides = [r for r in todo if not r["contacts"]]
    todo = [r for r in todo if r["contacts"]]
    batches = make_batches(todo)
    print(f"{len(todo)} agences à analyser en {len(batches)} requêtes (+ {len(vides)} sans contact), {len(done)} déjà faites", file=sys.stderr)
    if a.dry_run:
        return

    keys = Keys(os.environ.get("CEREBRAS_API_KEYS", ""))
    if not keys.keys:
        sys.exit("Aucune clé : définis la variable CEREBRAS_API_KEYS")

    def rec(r, extra):
        return {"siren": r["siren"], "agence": r["agence"], "site": r["site"], "status": r["status"], **extra}

    n = 0
    with open(a.out, "a", encoding="utf-8") as f:
        for r in vides:
            f.write(json.dumps(rec(r, {"gerant": None, "agence_contact": None, "confiance": "aucune",
                                       "remarques": "aucun contact trouvé", "verifs": []}), ensure_ascii=False) + "\n")
        stop = False
        with ThreadPoolExecutor(a.workers) as ex:
            futs = {ex.submit(analyze_batch, keys, b): b for b in batches}
            for fu in as_completed(futs):
                b = futs[fu]
                try:
                    res = fu.result()
                except NoKeyLeft:
                    stop = True
                    continue
                except Exception as e:
                    print("lot ignoré :", e, file=sys.stderr); continue
                with lock:
                    for i, r in enumerate(b):
                        x = res.get(i, {"erreur": "manquant"})
                        if "erreur" in x:
                            out = rec(r, {"gerant": None, "agence_contact": None, "confiance": "erreur",
                                          "remarques": x["erreur"], "verifs": []})
                        else:
                            out = rec(r, verify(x, r))
                        f.write(json.dumps(out, ensure_ascii=False) + "\n")
                    f.flush()
                    n += len(b)
                    if (n // len(b)) % 10 == 0:
                        print(f"{n}/{len(todo)} agences", file=sys.stderr)
    if stop:
        print("!! Plus aucune clé valide : relance après avoir rechargé du crédit (la reprise est automatique).", file=sys.stderr)
    print(f"Terminé : {n} agences | {USAGE['calls']} requêtes | {USAGE['in']} tokens entrée, {USAGE['out']} sortie", file=sys.stderr)


if __name__ == "__main__":
    main()
