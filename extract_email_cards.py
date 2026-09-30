#!/usr/bin/env python3
"""
Pour chaque agence : crawl (accueil + pages contact/équipe/mentions...) et sauvegarde
les CARTES : le bloc HTML qui entoure un email / un téléphone / une adresse, de façon
à garder ensemble nom, poste, email, téléphone et adresse d'une même personne ou agence.
Récupère aussi les données structurées (JSON-LD, emails dans les scripts).

Usage:
  pip install requests beautifulsoup4 lxml
  python extract_email_cards.py agences_immobilieres_clean.csv -o cards.jsonl -w 20

Sortie : cards.jsonl (1 ligne par agence) + cards.csv (1 ligne par carte)
Reprise auto : relancer la même commande, les agences déjà faites sont sautées.
"""
import argparse, csv, json, re, sys, threading, unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse, unquote
import requests
import urllib3
from bs4 import BeautifulSoup, Tag, NavigableString

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------------------------------------------------------- regex
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9\-]+(?:\.[a-zA-Z0-9\-]+)*\.[a-zA-Z]{2,}")
# téléphone FR : 04 79 84 41 73 / 04.79.84.41.73 / 0479844173 / +33 4 79... / +33 (0)4 79...
PHONE_RE = re.compile(
    r"(?<![\d+])(?:(?:\+|00)33[\s.\-]?(?:\(0\)[\s.\-]?)?|0)[1-9](?:[\s.\-]?\d{2}){4}(?!\d)")
LET = r"A-Za-zÀ-ÖØ-öø-ÿ"
# code postal + ville : 73800 Montmélian
POSTAL_RE = re.compile(
    r"(?<!\d)(?:0[1-9]|[1-8]\d|9[0-5]|97[1-6]|98[4-8])\d{3}(?!\d)[\s,]+[A-ZÀ-ÖØ-Þ][" + LET + r"'’\-]+"
    r"(?:[\s\-][" + LET + r"'’\-]+){0,3}")
STREET_RE = re.compile(
    r"\b\d{1,4}\s?(?:bis|ter|b)?[,\s]+(?:rue|avenue|av\.?|bd|boulevard|place|chemin|impasse|all[ée]e|cours|quai|"
    r"route|square|passage|rond-point|r[ée]sidence|zac|faubourg|esplanade|promenade|mail|parvis|voie|lotissement|"
    r"za|zi)\b", re.I)
# mots qui suivent souvent la ville et que POSTAL_RE avale (majuscule initiale)
TRAIL_RE = re.compile(r"\s+(?:T[ée]l\w*|Fax|Mob\w*|E-?mail|Mail|France|Horaires|Ouvert\w*|Contact\w*|Siret|Siren)\b.*$")
AT_RE = re.compile(r"\s*[\[\(\{<]\s*(?:at|arobase|@)\s*[\]\)\}>]\s*", re.I)
DOT_RE = re.compile(r"\s*[\[\(\{<]\s*(?:dot|point)\s*[\]\)\}>]\s*", re.I)

BAD_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".css", ".js", ".woff", ".woff2")
# Mots-clés de pages à visiter, par niveau de priorité (0 = visité en premier).
# Comparaison sans accents, en minuscules, séparateurs (espace _ / .) remplacés par "-".
KEY_TIERS = [
    # 0 : contact
    ("contact", "coordonnee", "joindre", "nous-ecrire", "ecrivez", "appelez", "rappel",
     "telephone", "email", "e-mail", "mail", "adresse", "acces", "plan-d-acces", "localisation",
     "venir", "trouver", "horaire", "ouverture"),
    # 1 : équipe / personnes
    ("equipe", "team", "staff", "collaborat", "conseill", "negociat", "agent", "commercia",
     "expert", "manager", "directeur", "directrice", "gerant", "responsable", "dirigeant",
     "fondateur", "associe", "mandataire", "courtier", "gestionnaire", "syndic", "notaire",
     "assistant", "secretaire", "comptab", "personnel", "membres", "trombinoscope", "portrait",
     "profil", "talent", "rencontr", "visage", "hommes", "femmes", "interlocuteur", "referent",
     "pro-", "professionnel", "specialiste", "consultant", "chargé", "charge-de", "vendeur",
     "immobilier-team", "notre-equipe", "l-equipe", "nos-equipes", "nos-agents", "nos-conseillers",
     "nos-collaborateurs", "nos-experts"),
    # 2 : agence / présentation
    ("agence", "agences", "bureau", "cabinet", "reseau", "implantation", "point-de-vente",
     "points-de-vente", "magasin", "boutique", "showroom", "franchise", "succursale", "filiale",
     "qui-sommes", "qui-est", "a-propos", "apropos", "about", "presentation", "presente",
     "histoire", "metier", "valeurs", "philosophie", "engagement", "savoir-faire", "notre-",
     "nos-", "societe", "entreprise", "groupe", "nous", "decouvr"),
    # 3 : pages légales (souvent un email de contact)
    ("mention", "legal", "legale", "informations-legales", "cgu", "cgv", "confidentialite",
     "rgpd", "donnees-personnelles", "privacy", "politique", "honoraires", "bareme",
     "reclamation", "mediation", "plan-du-site", "sitemap", "plan-site"),
]
PAGE_KEYS = tuple(k for tier in KEY_TIERS for k in tier)  # compat.
# chemins à ignorer (annonces, blog, panier, fichiers...)
EXCLUDE_PATH = ("/blog", "/actualit", "/news", "/annonce", "/biens/", "/bien/", "/vente/",
                "/location/", "/achat/", "/produit", "/panier", "/cart", "/wp-content", "/wp-json",
                "/tag/", "/category/", "/categorie/", "/author/", "/feed", "/login", "/wp-admin",
                "/recherche", "/search", "/mon-compte", "/account")
MAX_PAGES = 12           # pages max par site (accueil inclus)
CARD_MAX_TEXT = 700      # taille max (texte) d'une carte
CARD_MAX_HTML = 6000     # taille max (html) d'une carte
CARD_MAX_PHONES = 3      # au-delà, on considère que le bloc mélange plusieurs contacts
MAX_BYTES = 3_000_000    # taille max d'une page téléchargée
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot-like research bot)"}
lock = threading.Lock()


# ----------------------------------------------------------------------------- réseau
def _decode(data, ctype):
    m = re.search(r"charset=([\w\-]+)", ctype or "", re.I)
    if not m:
        m = re.search(rb"<meta[^>]+charset=[\"']?([\w\-]+)", data[:4096], re.I)
        enc = m.group(1).decode("ascii", "ignore") if m else None
    else:
        enc = m.group(1)
    for e in (enc, "utf-8"):
        if not e:
            continue
        try:
            return data.decode(e)
        except (UnicodeDecodeError, LookupError):
            continue
    return data.decode("cp1252", errors="replace")


def fetch(url, timeout=12):
    """Retourne (html, url_finale) ou (None, None).
    Fallbacks : https avec certificat cassé -> verify=False, puis http://."""
    attempts = [(url, True)]
    if url.lower().startswith("https://"):
        attempts += [(url, False), ("http://" + url[8:], True)]
    for u, verify in attempts:
        try:
            r = requests.get(u, headers=HEADERS, timeout=timeout, allow_redirects=True,
                             verify=verify, stream=True)
        except requests.exceptions.SSLError:
            continue
        except requests.exceptions.Timeout:
            return None, None
        except Exception:
            continue
        try:
            ct = r.headers.get("content-type", "")
            if r.status_code != 200 or "html" not in ct.lower():
                return None, None
            chunks, size = [], 0
            for ch in r.iter_content(65536):
                chunks.append(ch)
                size += len(ch)
                if size > MAX_BYTES:
                    break
            return _decode(b"".join(chunks), ct), r.url
        except Exception:
            return None, None
        finally:
            r.close()
    return None, None


# ----------------------------------------------------------------------------- normalisation
def decode_cf(enc):  # emails obfusqués Cloudflare
    try:
        k = int(enc[:2], 16)
        return "".join(chr(int(enc[i:i + 2], 16) ^ k) for i in range(2, len(enc), 2))
    except Exception:
        return None


def clean_email(e):
    e = e.strip().strip(".,;:()<>[]\"'").lower()
    if e.endswith(BAD_EXT) or "@2x" in e or "sentry" in e or "example." in e or "wixpress" in e:
        return None
    return e


def norm_phone(p):
    d = re.sub(r"\D", "", p)
    if d.startswith("0033"):
        d = "0" + d[4:]
    elif d.startswith("33") and len(d) == 11:
        d = "0" + d[2:]
    elif d.startswith("330") and len(d) == 12:
        d = d[2:]
    if len(d) != 10:
        return None
    return " ".join(d[i:i + 2] for i in range(0, 10, 2))


def extract_addresses(text):
    """Adresses FR trouvées dans un texte : rue (si présente) + code postal + ville."""
    out = []
    for m in POSTAL_RE.finditer(text):
        window = text[max(0, m.start() - 90):m.end()]
        streets = list(STREET_RE.finditer(window))
        a = window[streets[-1].start():] if streets else m.group(0)
        a = re.sub(r"\s+", " ", a).strip(" ,;-")
        a = TRAIL_RE.sub("", a).strip(" ,;-")
        if a not in out:
            out.append(a)
    return out


# ----------------------------------------------------------------------------- signaux d'un nœud
def sig(el, cache):
    """(texte, emails, phones, addrs, html_len) d'un nœud, calculé UNE fois par nœud."""
    k = id(el)
    if k in cache:
        return cache[k]
    text = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
    emails, phones = set(), set()
    if isinstance(el, Tag):
        for a in el.find_all("a", href=True):
            h = unquote(a["href"])
            hl = h.lower()
            if hl.startswith("mailto:"):
                for m in EMAIL_RE.findall(h[7:].split("?")[0]):
                    emails.add(clean_email(m))
            elif hl.startswith("tel:"):
                p = norm_phone(h[4:])
                if p:
                    phones.add(p)
    for m in EMAIL_RE.findall(text):
        emails.add(clean_email(m))
    for m in PHONE_RE.finditer(text):
        p = norm_phone(m.group(0))
        if p:
            phones.add(p)
    emails.discard(None)
    res = {"text": text, "emails": emails, "phones": phones,
           "addrs": extract_addresses(text), "html_len": len(str(el))}
    cache[k] = res
    return res


def prep(soup):
    for t in soup(["style", "noscript", "svg", "iframe", "link", "meta", "script"]):
        t.decompose()
    for c in soup.find_all(attrs={"data-cfemail": True}):  # décodage CF
        e = decode_cf(c["data-cfemail"])
        if e:
            c.replace_with(e)
    for a in soup.find_all("a", href=re.compile(r"/cdn-cgi/l/email-protection#")):
        e = decode_cf(a["href"].split("#")[-1])
        if e:
            a.string = e
            a["href"] = "mailto:" + e
    # emails écrits "contact [at] site [point] fr", "contact (arobase) site.fr"
    for s in [s for s in soup.find_all(string=AT_RE) if isinstance(s, NavigableString)]:
        t = DOT_RE.sub(".", AT_RE.sub("@", str(s)))
        if EMAIL_RE.search(t):
            s.replace_with(t)
    return soup


# ----------------------------------------------------------------------------- cartes
def has_signal(sg):
    return bool(sg["emails"] or sg["phones"])


def sibling_items(p, cur, cache):
    """Nombre de frères de `cur` (même balise/classes) qui contiennent eux aussi un contact
    et sont assez gros pour être des cartes : si >= 2, `p` est une liste de cartes."""
    key = (cur.name, tuple(cur.get("class") or ()))
    n = 0
    for c in p.children:
        if isinstance(c, Tag) and (c.name, tuple(c.get("class") or ())) == key:
            sg = sig(c, cache)
            if has_signal(sg) and len(sg["text"]) >= 40:
                n += 1
    return n


def find_card(node, cache):
    """Remonte depuis un nœud contenant un email/téléphone/adresse jusqu'au plus grand bloc
    qui reste UN seul contact : un seul email (ou ceux du nœud), <= 3 téléphones,
    <= 1 adresse, taille raisonnable, et qui n'est pas une liste de cartes."""
    s0 = sig(node, cache)
    target = s0["emails"]
    best = cur = node
    while isinstance(cur.parent, Tag):
        p = cur.parent
        if p.name in ("body", "html", "main", "nav"):
            break
        sp = sig(p, cache)
        if target:
            if sp["emails"] - target:          # un autre email apparait -> autre contact
                break
        elif len(sp["emails"]) > 1:
            break
        if len(sp["phones"]) > CARD_MAX_PHONES or len(sp["addrs"]) > 1:
            break
        if len(sp["text"]) > CARD_MAX_TEXT or sp["html_len"] > CARD_MAX_HTML:
            break
        if sibling_items(p, cur, cache) >= 2:  # p = liste d'équipiers, cur = un équipier
            break
        best = cur = p
        if p.name in ("footer", "header", "address"):  # bloc contact complet, on s'arrête là
            break
    return best


def shrink_html(card):
    """HTML allégé : on garde balises/classes/href utiles, on vire le reste."""
    c = BeautifulSoup(str(card), "html.parser").find()
    for t in c.find_all(True):
        keep = {k: v for k, v in t.attrs.items() if k in ("href", "class", "alt", "title")}
        if t.name == "a" and "href" in keep and not keep["href"].lower().startswith(("mailto:", "tel:")):
            keep.pop("href")
        t.attrs = keep
    return re.sub(r"\s+", " ", str(c)).strip()


def is_ancestor(a, b):
    """a est-il un ancêtre de b ?"""
    return any(p is a for p in b.parents)


def build_cards(soup):
    cache = {}
    anchors = []  # (nœud, type d'ancre)
    for a in soup.find_all("a", href=re.compile(r"^(mailto|tel):", re.I)):
        anchors.append((a, "email" if a["href"].lower().startswith("mailto") else "phone"))
    for kind, rx in (("email", EMAIL_RE), ("phone", PHONE_RE), ("address", POSTAL_RE)):
        for s in soup.find_all(string=rx):
            if s.parent is not None:
                anchors.append((s.parent, kind))
    for ad in soup.find_all("address"):
        anchors.append((ad, "address"))

    cards = {}  # id(bloc) -> {"el": bloc, "kinds": set()}
    for n, kind in anchors:
        sg = sig(n, cache)
        if kind == "email" and not sg["emails"]:
            continue
        if kind == "phone" and not sg["phones"]:
            continue
        if kind == "address" and not sg["addrs"]:
            continue
        el = find_card(n, cache)
        cards.setdefault(id(el), {"el": el, "kinds": set()})["kinds"].add(kind)

    items = []
    for c in cards.values():
        sg = sig(c["el"], cache)
        # adresse seule (sans email ni téléphone) : on ne garde que les petits blocs
        if not has_signal(sg) and len(sg["text"]) > 400:
            continue
        items.append((c, sg))
    # supprime une carte incluse dans une autre carte "mono-contact" qui contient les mêmes infos
    keep = []
    for c, sg in items:
        redundant = False
        for c2, sg2 in items:
            if c2 is c or not is_ancestor(c2["el"], c["el"]):
                continue
            if (sg["emails"] <= sg2["emails"] and sg["phones"] <= sg2["phones"]
                    and set(sg["addrs"]) <= set(sg2["addrs"]) and len(sg2["emails"]) <= 1):
                redundant = True
                break
        if not redundant:
            keep.append((c, sg))
    return keep


def structured_from_soup(soup):
    """JSON-LD + emails dans les scripts (à appeler AVANT prep(), qui supprime les <script>)."""
    ld, seen = [], set()

    def walk(o):
        if isinstance(o, dict):
            yield o
            for v in o.values():
                yield from walk(v)
        elif isinstance(o, list):
            for v in o:
                yield from walk(v)

    def flat_addr(a):
        if isinstance(a, dict):
            return " ".join(str(a.get(k, "")) for k in ("streetAddress", "postalCode", "addressLocality")).strip()
        if isinstance(a, list):
            return " | ".join(filter(None, (flat_addr(x) for x in a)))
        return str(a or "").strip()

    script_emails = set()
    for s in soup.find_all("script"):
        txt = s.string or s.get_text() or ""
        if "ld+json" in (s.get("type") or "").lower():
            try:
                data = json.loads(txt.strip())
            except Exception:
                continue
            for d in walk(data):
                if not any(k in d for k in ("email", "telephone", "address", "openingHours",
                                            "openingHoursSpecification")):
                    continue
                item = {"type": d.get("@type"), "name": d.get("name"), "email": d.get("email"),
                        "telephone": d.get("telephone"), "jobTitle": d.get("jobTitle"),
                        "address": flat_addr(d.get("address")) or None,
                        "hours": d.get("openingHours") or d.get("openingHoursSpecification")}
                item = {k: v for k, v in item.items() if v}
                if isinstance(item.get("email"), str):
                    item["email"] = item["email"].replace("mailto:", "").strip().lower()
                key = json.dumps(item, sort_keys=True, ensure_ascii=False)
                if len(item) > 1 and key not in seen:
                    seen.add(key)
                    ld.append(item)
        elif len(txt) < 2_000_000:  # __NEXT_DATA__, window.__INITIAL_STATE__, ...
            for m in EMAIL_RE.findall(txt):
                e = clean_email(m)
                if e:
                    script_emails.add(e)
    out = {}
    if ld:
        out["jsonld"] = ld
    if script_emails:
        out["script_emails"] = sorted(script_emails)
    return out


def parse_page(html, url):
    """-> (cards, structured). Parse une seule fois."""
    soup = BeautifulSoup(html, "lxml")
    structured = structured_from_soup(soup)
    soup = prep(soup)
    cards = []
    for c, sg in build_cards(soup):
        cards.append({
            "emails": sorted(sg["emails"]),
            "phones": sorted(sg["phones"]),
            "addresses": sg["addrs"],
            "anchors": sorted(c["kinds"]),
            "card_text": sg["text"],
            "card_html": shrink_html(c["el"]),
        })
    return cards, structured


def cards_from_page(html, url):  # compat. avec extract_cards_links.py
    return parse_page(html, url)[0]


# ----------------------------------------------------------------------------- crawl
def norm_key(t):
    """minuscules, sans accents, tout ce qui n'est pas alphanumérique -> '-'."""
    t = unicodedata.normalize("NFKD", unquote(t).lower())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", t)


def link_tier(text):
    """Plus petit niveau de priorité dont un mot-clé apparait dans `text` (None si aucun)."""
    for n, tier in enumerate(KEY_TIERS):
        if any(norm_key(k).strip("-") in text for k in tier):
            return n
    return None


def candidate_links(html, base, domain):
    soup = BeautifulSoup(html, "lxml")
    found = {}  # url -> (tier, ordre d'apparition)
    for a in soup.find_all("a", href=True):
        u = urljoin(base, a["href"]).split("#")[0]
        pu = urlparse(u)
        if pu.scheme not in ("http", "https") or pu.netloc.replace("www.", "") != domain:
            continue
        path = pu.path.lower()
        if path.endswith(BAD_EXT + (".pdf", ".zip", ".doc", ".docx")):
            continue
        if any(x in path + "/" for x in EXCLUDE_PATH):
            continue
        tier = link_tier(norm_key(pu.path + " " + a.get_text(" ")))
        if tier is None:
            continue
        if u not in found or tier < found[u][0]:
            found[u] = (tier, found.get(u, (0, len(found)))[1])
    return [u for u, _ in sorted(found.items(), key=lambda kv: kv[1])]


def process(row):
    site = row["Site web"].strip()
    if not site.startswith("http"):
        site = "http://" + site
    html, final = fetch(site)
    out = {"siren": row["Siren"], "agence": row["Nom de l'agence"], "site": site,
           "cards": [], "structured": [], "status": "ok"}
    if not html:
        out["status"] = "site_inaccessible"
        return out
    domain = urlparse(final).netloc.replace("www.", "")
    pages, queue, done = {final: html}, candidate_links(html, final, domain), {final}
    for u in queue:
        if len(pages) >= MAX_PAGES:
            break
        if u in done:
            continue
        done.add(u)
        h, fu = fetch(u)
        if h:
            pages[fu] = h
    seen = set()
    for url, h in pages.items():
        cards, structured = parse_page(h, url)
        if structured:
            out["structured"].append({"page_url": url, **structured})
        for c in cards:
            k = (tuple(c["emails"]), tuple(c["phones"]), c["card_text"][:120])
            if k in seen:
                continue
            seen.add(k)
            c["page_url"] = url
            out["cards"].append(c)
    if not out["cards"] and not out["structured"]:
        out["status"] = "aucun_email"
    return out


def export_flat(out):
    flat = out.rsplit(".", 1)[0] + ".csv"
    with open(flat, "w", newline="", encoding="utf-8") as g:
        w = csv.writer(g)
        w.writerow(["siren", "agence", "site", "page_url", "emails", "phones", "addresses", "anchors",
                    "card_text", "card_html"])
        for line in open(out, encoding="utf-8"):
            d = json.loads(line)
            for c in d["cards"]:
                w.writerow([d["siren"], d["agence"], d["site"], c["page_url"], " | ".join(c["emails"]),
                            " | ".join(c["phones"]), " | ".join(c["addresses"]), " ".join(c["anchors"]),
                            c["card_text"], c["card_html"]])
    return flat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?")
    ap.add_argument("-o", "--out", default="cards.jsonl")
    ap.add_argument("-w", "--workers", type=int, default=20)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--shard", default="0/1", help="i/n : ne traite que les lignes k %% n == i")
    ap.add_argument("--export-only", action="store_true", help="régénère le .csv à plat depuis le .jsonl")
    a = ap.parse_args()

    if a.export_only:
        print("Terminé ->", export_flat(a.out), file=sys.stderr)
        return
    if not a.csv:
        ap.error("csv requis")

    si, sn = map(int, a.shard.split("/"))
    rows = [r for k, r in enumerate(csv.DictReader(open(a.csv, encoding="utf-8"))) if k % sn == si]
    done = set()
    try:
        for line in open(a.out, encoding="utf-8"):
            d = json.loads(line)
            done.add(d["siren"] + "|" + d["site"])
    except FileNotFoundError:
        pass
    todo = [r for r in rows if r["Site web"] and (r["Siren"] + "|" + (r["Site web"].strip() if r["Site web"].startswith("http") else "http://" + r["Site web"].strip())) not in done]
    if a.limit:
        todo = todo[:a.limit]
    print(f"{len(rows)} agences, {len(done)} déjà faites, {len(todo)} à traiter", file=sys.stderr)

    n = 0
    with open(a.out, "a", encoding="utf-8") as f, ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(process, r) for r in todo]
        for fu in as_completed(futs):
            try:
                res = fu.result()
            except Exception:
                continue
            with lock:
                f.write(json.dumps(res, ensure_ascii=False) + "\n")
                f.flush()
            n += 1
            if n % 50 == 0:
                print(f"{n}/{len(todo)}", file=sys.stderr)

    print("Terminé ->", a.out, export_flat(a.out), file=sys.stderr)


if __name__ == "__main__":
    main()
