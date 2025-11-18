import csv
import os
import time
import re
import json
from io import BytesIO
from urllib.parse import urljoin, urlparse, quote_plus
from ftplib import FTP

import requests
from bs4 import BeautifulSoup

# ========================
# CONFIGURAZIONE GENERALE
# ========================
LOCAL_WORK_DIR = "/tmp"
LOCAL_CSV_PATH = os.path.join(LOCAL_WORK_DIR, "prodotti.csv")

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 3  # secondi di pausa tra prodotti

# ========================
# CONFIGURAZIONE FTP (da ENV)
# ========================
FTP_HOST = os.getenv("FTP_HOST", "ftp.tuoserver.com")
FTP_USER = os.getenv("FTP_USER", "username")
FTP_PASS = os.getenv("FTP_PASS", "password")

# Percorsi RELATIVI alla root FTP del tuo utente.
# Per il tuo caso:
#   FTP_CSV_DIR      = "citymoda.cloud/public_html/input"
#   FTP_IMG_BASE_DIR = "citymoda.cloud/public_html/images"
FTP_CSV_DIR = os.getenv("FTP_CSV_DIR", "citymoda.cloud/public_html/input")
FTP_CSV_FILENAME = os.getenv("FTP_CSV_FILENAME", "prodotti.csv")
FTP_IMG_BASE_DIR = os.getenv("FTP_IMG_BASE_DIR", "citymoda.cloud/public_html/images")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# ===============================
# MAPPATURA BRAND → DOMINIO UFFICIALE (fallback HTML)
# ===============================
BRAND_DOMAIN_MAP = {
    "4GIVENESS": "www.4giveness.it",
    "ADIDAS": "www.adidas.it",
    "AERONAUTICA MILITARE": "www.aeronauticamilitareofficialstore.it",
    "ANIYE BY": "www.aniyeby.com",
    "ARMANI EXCHANGE": "www.armaniexchange.com",
    "BIRKENSTOCK": "www.birkenstock.com",
    "BLAUER": "www.blauerusa.com",
    "BLUNDSTONE": "www.blundstone.it",
    "BOSS": "www.hugoboss.com",
    "CALVIN KLEIN": "www.calvinklein.it",
    "CALVIN KLEIN JEANS": "www.calvinklein.it",
    "CALVIN KLEIN MENSWEAR": "www.calvinklein.it",
    "COLORS OF CALIFORNIA": "www.colorsofcalifornia.it",
    "COMPANIA FANTASTICA": "www.companiafantastica.com",
    "CRIME LONDON": "www.crimelondon.com",
    "CROCS": "www.crocs.eu",
    "CULT": "www.cultofficial.com",
    "DESIGUAL": "www.desigual.com",
    "DICKIES": "www.dickieslife.com",
    "DIESEL KID": "it.diesel.com",
    "DISCLAIMER": None,
    "DSQUARED": "www.dsquared2.com",
    "EA7": "www.armani.com",
    "FRACOMINA": "www.fracomina.it",
    "FRANCESCO MILANO": "www.francescomilano.com",
    "G-STAR": "www.g-star.com",
    "GEOX": "www.geox.com",
    "GOLD&GOLD": "www.goldandgold.it",
    "GUESS": "www.guess.eu",
    "GUESS by MARCIANO": "www.guess.eu",
    "GUESS JEANS": "www.guess.eu",
    "HARMONT & BLAINE": "www.harmontblaine.com",
    "HAVAIANAS": "www.havaianas-store.com",
    "HAVEONE": "www.haveone.it",
    "HEY DUDE": "eu.heydude.com",
    "HUGO MEN": "www.hugoboss.com",
    "ICON": None,
    "IMPERIAL": "www.imperialfashion.com",
    "KOCCA": "kocca.it",
    "LACOSTE": "www.lacoste.com",
    "LEVI'S": "www.levi.com",
    "LIU JO": "www.liujo.com",
    "LOVE MOSCHINO": "www.moschino.com",
    "MANILA GRACE": "www.manilagrace.com",
    "MARC ELLIS": "marcellis.com",
    "MARKUP": "www.markupfashion.com",
    "MAYORAL": "www.mayoral.com",
    "MOLLY BRACKEN": "www.mollybracken.com",
    "NAPAPIJRI": "www.napapijri.com",
    "NEW BALANCE": "www.newbalance.it",
    "PEPE JEANS": "www.pepejeans.com",
    "PEUTEREY": "www.peuterey.com",
    "RALPH LAUREN": "www.ralphlauren.it",
    "REFRESH": "www.refreshshoes.com",
    "RICHMOND": "www.johnrichmond.com",
    "RINASCIMENTO": "www.rinascimento.com",
    "SKECHERS": "www.skechers.it",
    "SPRAY GROUND": "www.sprayground.com",
    "SQUAD": None,
    "SUN 68": "www.sun68.com",
    "SUNDEK": "www.sundek.com",
    "SUNS": "www.sunsboards.com",
    "SUPERCULTURE": None,
    "THE FARM by GOORIN BROS.": "www.goorin.com",
    "THE NORTH FACE": "www.thenorthface.it",
    "TIMBERLAND": "www.timberland.it",
    "TOMMY HILFIGER": "it.tommy.com",
    "TOMMY JEANS": "it.tommy.com",
    "TRUSSARDI JEANS": "www.trussardi.com",
    "VALENTINO": "www.valentino.com",
    "VANS": "www.vans.com",
    "VICOLO": "www.vicolofashion.com",
    "V°73": "www.v73.it",
    "YES ZEE": "www.yeszee.com",
}

# ========================
# VARIABILI FTP GLOBALI
# ========================
_ftp = None
ROOT_DIR = None  # root FTP dell'utente dopo il login


# ========================
# UTILITY DI BASE
# ========================

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def brand_to_folder(brand):
    return (
        brand.strip()
        .lower()
        .replace(" ", "_")
        .replace("&", "e")
        .replace("°", "")
        .replace(".", "")
        .replace("'", "")
    )


def http_get(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            print(f"   ✖ Richiesta fallita ({resp.status_code}) → {url}")
            return None
        return resp
    except Exception as e:
        print(f"   ✖ Errore richiesta {url}: {e}")
        return None


def get_file_extension_from_url(url):
    path = urlparse(url).path
    _, ext = os.path.splitext(path)
    if ext:
        return ext.split("?")[0]
    return ".jpg"


# ========================
# FTP
# ========================

def get_ftp():
    global _ftp, ROOT_DIR
    if _ftp is None:
        print(f"[*] Connessione FTP a {FTP_HOST}...")
        _ftp = FTP(FTP_HOST)
        _ftp.login(FTP_USER, FTP_PASS)
        ROOT_DIR = _ftp.pwd()
        print("[*] Connesso a FTP. ROOT_DIR:", ROOT_DIR)
    return _ftp


def ftp_download_csv(local_path):
    ftp = get_ftp()

    # vai sempre alla root
    ftp.cwd(ROOT_DIR)

    # entra nella cartella del CSV
    csv_dir = FTP_CSV_DIR.lstrip("/")
    if csv_dir:
        ftp.cwd(csv_dir)

    print(f"[*] Scarico CSV da FTP: {FTP_CSV_DIR}/{FTP_CSV_FILENAME} → {local_path}")
    with open(local_path, "wb") as f:
        ftp.retrbinary("RETR " + FTP_CSV_FILENAME, f.write)
    print("[*] CSV scaricato.")

    # torna alla root
    ftp.cwd(ROOT_DIR)


def ftp_ensure_dir(path):
    """
    Crea ricorsivamente la directory su FTP se non esiste.
    Parte SEMPRE dalla ROOT_DIR, così non annidiamo mai /input/... all'infinito.
    """
    ftp = get_ftp()

    # normalizza: niente slash iniziale
    rel_path = path.lstrip("/")

    # parti SEMPRE dalla root FTP
    ftp.cwd(ROOT_DIR)

    parts = [p for p in rel_path.split("/") if p]
    for p in parts:
        try:
            ftp.cwd(p)
        except Exception:
            ftp.mkd(p)
            ftp.cwd(p)
    # alla fine restiamo nella dir di destinazione


def ftp_upload_image_stream(binary_content, remote_dir, filename):
    """
    Upload diretto: crea la dir partendo dalla ROOT_DIR, ci entra
    e fa STOR filename (senza path assoluti).
    """
    ftp = get_ftp()

    ftp_ensure_dir(remote_dir)

    print(f"   ⬆ Upload diretto FTP in dir '{remote_dir}': {filename}")
    bio = BytesIO(binary_content)
    ftp.storbinary("STOR " + filename, bio)
    bio.close()


# ========================
# LOGICA DI RICERCA (KOCCA / MARC ELLIS / GENERICA)
# ========================

def build_kocca_query_from_sku(sku):
    """
    KOCCA: CLEMENTINAM9001 → 'clementina'
    Prende la parte prima della prima cifra.
    """
    s = sku.strip()
    parts = re.split(r"\d", s, maxsplit=1)
    base = parts[0] if parts else s
    return base.lower()


def build_marc_ellis_query_from_sku(sku):
    """
    MARC ELLIS: AROUNDM26BLACKLGOLD → 'around m 26 black l gold'
    Rende lo SKU più "umano" per la search Shopify.
    """
    s = sku.strip().lower()
    s = re.sub(r"([a-z])(\d)", r"\1 \2", s)
    s = re.sub(r"(\d)([a-z])", r"\1 \2", s)
    s = s.replace("_", " ")
    return s


def normalize_code_for_match(s):
    """
    Normalizza una stringa per il confronto codici:
    - minuscolo
    - solo [a-z0-9]
    Esempio:
      'AROUNDM26BLACKLGOLD' → 'aroundm26blacklgold'
      'around_m_26-black-l.gold' → 'aroundm26blacklgold'
    """
    return re.sub(r"[^a-z0-9]", "", s.lower())


def find_kocca_product_url(sku):
    """
    Trova l'URL prodotto KOCCA usando l'endpoint di search JSON.
    Esempio: CLEMENTINAM9001 → query 'clementina'.
    """
    query = build_kocca_query_from_sku(sku)  # es. 'clementina'
    q = quote_plus(query)

    suggest_url = (
        "https://kocca.it/search/suggest.json"
        f"?q={q}&resources[type]=product&resources[limit]=10"
    )
    print(f"   🔍 (KOCCA JSON) {suggest_url}")
    resp = http_get(suggest_url)
    if not resp:
        return None

    try:
        data = resp.json()
    except Exception as e:
        print("   ✖ Errore parsing JSON Kocca:", e)
        return None

    resources = data.get("resources") or {}
    results = resources.get("results") or {}
    products = results.get("products") or []

    if not products:
        print("   ✖ Nessun prodotto KOCCA da suggest.json")
        return None

    query_l = query.lower()
    best = None
    best_score = -1

    for p in products:
        title = (p.get("title") or "").lower()
        handle = (p.get("handle") or "").lower()
        p_url = p.get("url") or ""

        score = 0
        if query_l in title:
            score += 3
        if query_l in handle:
            score += 2
        if "clementina" in query_l and "clementina" in title:
            score += 1

        if score > best_score:
            best_score = score
            best = p_url or ("/products/" + handle if handle else None)

    if not best:
        return None

    return urljoin("https://kocca.it", best)


def find_marc_ellis_product_url(sku):
    """
    Trova l'URL prodotto MARC ELLIS usando la search JSON Shopify.
    Migliorata per preferire handle/titoli che matchano fortemente lo SKU.
    """
    query = build_marc_ellis_query_from_sku(sku)
    q = quote_plus(query)

    suggest_url = (
        "https://marcellis.com/search/suggest.json"
        f"?q={q}&resources[type]=product&resources[limit]=10"
    )
    print(f"   🔍 (MARC ELLIS JSON) {suggest_url}")
    resp = http_get(suggest_url)
    if not resp:
        return None

    try:
        data = resp.json()
    except Exception as e:
        print("   ✖ Errore parsing JSON Marc Ellis:", e)
        return None

    resources = data.get("resources") or {}
    results = resources.get("results") or {}
    products = results.get("products") or []

    if not products:
        print("   ✖ Nessun prodotto Marc Ellis da suggest.json")
        return None

    sku_norm = normalize_code_for_match(sku)
    tokens = [t for t in re.split(r"\s+", query.lower()) if len(t) > 2]

    best = None
    best_score = -1

    for p in products:
        title = (p.get("title") or "").lower()
        handle = (p.get("handle") or "").lower()
        p_url = p.get("url") or ""

        handle_norm = normalize_code_for_match(handle)
        title_norm = normalize_code_for_match(title)

        score = 0

        # match fortissimo: SKU normalizzato contenuto nell'handle
        if sku_norm and sku_norm in handle_norm:
            score += 100
        elif sku_norm and handle_norm in sku_norm:
            score += 80

        # match sul titolo (meno forte)
        if sku_norm and sku_norm in title_norm:
            score += 40

        # bonus per token della query presenti in titolo/handle
        for t in tokens:
            if t in title:
                score += 2
            if t in handle:
                score += 1

        if score > best_score:
            best_score = score
            best = p_url or ("/products/" + handle if handle else None)

    if not best:
        return None

    return urljoin("https://marcellis.com", best)


def build_search_url(brand, sku):
    """
    Costruisce l'URL di ricerca generico (fallback HTML) se non abbiamo JSON
    o se la ricerca brand-specific non trova nulla.
    """
    domain = BRAND_DOMAIN_MAP.get((brand or "").upper())
    if not domain:
        return None

    q = quote_plus(sku.strip())
    return f"https://{domain}/search?q={q}"


def pick_first_product_link_from_search(html, base_url):
    soup = BeautifulSoup(html, "html.parser")

    # 1) link con <img> (tipico risultato prodotto)
    for a in soup.find_all("a", href=True):
        if a.find("img"):
            return urljoin(base_url, a["href"])

    # 2) fallback: link che assomiglia a pagina prodotto
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(base_url, href)
        path = urlparse(full).path.lower()
        if any(x in path for x in ["/product", "/prod", "/p/", "/item", "/art"]):
            return full

    return None


def extract_all_images_from_product_page(html, page_url):
    """
    Estrae TUTTE le immagini prodotto rilevanti dalla pagina:
    - og:image
    - JSON-LD con lista di immagini
    - <img> dentro sezioni prodotto (heuristica)
    Esclude SVG.
    """
    soup = BeautifulSoup(html, "html.parser")
    urls = []

    def add_url(u):
        if not u:
            return
        full = urljoin(page_url, u)
        # ❌ escludi SVG
        lower = full.lower()
        if lower.endswith(".svg") or ".svg" in lower:
            return
        if full not in urls:
            urls.append(full)

    # 1) og:image
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        add_url(og.get("content").strip())

    # 2) JSON-LD con image
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "{}")
        except Exception:
            continue

        if isinstance(data, dict):
            imgs = data.get("image")
            if isinstance(imgs, str):
                add_url(imgs)
            elif isinstance(imgs, list):
                for u in imgs:
                    add_url(u)
        elif isinstance(data, list):
            for obj in data:
                if not isinstance(obj, dict):
                    continue
                imgs = obj.get("image")
                if isinstance(imgs, str):
                    add_url(imgs)
                elif isinstance(imgs, list):
                    for u in imgs:
                        add_url(u)

    # 3) fallback: tutte le <img> "grandi" collegate al prodotto
    product_wrappers = soup.select(
        "[class*='product'] img, [class*='gallery'] img, [class*='media'] img"
    )
    candidates = product_wrappers or soup.find_all("img")

    scored = []

    for img in candidates:
        src = img.get("src") or img.get("data-src") or img.get("data-srcset")
        if not src:
            continue

        if "," in src:
            src = src.split(",")[0].split(" ")[0]

        full = urljoin(page_url, src)

        lower = full.lower()
        if lower.endswith(".svg") or ".svg" in lower:
            continue

        try:
            w = int(img.get("width") or 0)
            h = int(img.get("height") or 0)
            area = w * h
        except Exception:
            area = 0

        path = urlparse(full).path.lower()
        if "/products/" in path or "/files/" in path:
            area += 100000  # boost

        scored.append((area, full))

    for _, u in sorted(scored, key=lambda x: x[0], reverse=True):
        add_url(u)

    return urls


# ========================
# DOWNLOAD & UPLOAD IMMAGINI
# ========================

def download_and_upload_images(img_urls, sku, brand):
    """
    Scarica e carica su FTP tutte le immagini nella lista.
    La prima immagine mantiene il nome SKU.ext,
    le successive diventano SKU_2.ext, SKU_3.ext, ecc.
    Esclude SVG prima del download.
    """
    if not img_urls:
        print("   ✖ Nessuna immagine da scaricare.")
        return

    brand_folder = brand_to_folder(brand)
    remote_dir = os.path.join(FTP_IMG_BASE_DIR, brand_folder).replace("\\", "/")

    for idx, img_url in enumerate(img_urls, start=1):
        lower = img_url.lower()
        if lower.endswith(".svg") or ".svg" in lower:
            print("   ⚠️ Ignorata immagine SVG:", img_url)
            continue

        print(f"   ⬇ Download immagine #{idx}: {img_url}")
        resp = http_get(img_url)
        if not resp:
            continue

        ext = get_file_extension_from_url(img_url)

        # sicurezza: se per caso l'estensione è .svg, salta
        if ext.lower() == ".svg":
            print("   ⚠️ Ignorata immagine SVG (da estensione):", img_url)
            continue

        if idx == 1:
            filename = f"{sku}{ext}"
        else:
            filename = f"{sku}_{idx}{ext}"

        ftp_upload_image_stream(resp.content, remote_dir, filename)


# ========================
# PROCESS PRODUCT
# ========================

def process_product(sku, brand):
    print(f"\n➡️ SKU: {sku} | Brand: {brand}")
    b = (brand or "").strip().lower()

    product_url = None

    # 1) BRAND-SPECIFIC JSON SEARCH
    if b == "kocca":
        product_url = find_kocca_product_url(sku)
        if product_url:
            print(f"   🔗 Pagina prodotto KOCCA (JSON): {product_url}")
    elif b == "marc ellis":
        product_url = find_marc_ellis_product_url(sku)
        if product_url:
            print(f"   🔗 Pagina prodotto MARC ELLIS (JSON): {product_url}")

    # 2) FALLBACK HTML SE JSON NON TROVA NIENTE
    if not product_url:
        search_url = build_search_url(brand, sku)
        if not search_url:
            print("   ✖ Nessuna URL di ricerca disponibile per questo brand.")
            return

        print(f"   🔍 Cerco prodotto (fallback HTML) su: {search_url}")
        search_resp = http_get(search_url)
        if not search_resp:
            return

        product_url = pick_first_product_link_from_search(search_resp.text, search_url)
        if not product_url:
            print("   ✖ Nessuna pagina prodotto trovata nemmeno via HTML.")
            return

        print(f"   🔗 Pagina prodotto (fallback HTML): {product_url}")

    # 3) SCARICA PAGINA PRODOTTO E TROVA IMMAGINI
    time.sleep(SLEEP_BETWEEN_REQUESTS)
    product_resp = http_get(product_url)
    if not product_resp:
        return

    img_urls = extract_all_images_from_product_page(product_resp.text, product_url)
    if not img_urls:
        print("   ✖ Nessuna immagine trovata nella pagina prodotto.")
        return

    print(f"   ✅ Trovate {len(img_urls)} immagini prodotto (SVG escluse).")
    download_and_upload_images(img_urls, sku, brand)


# ========================
# MAIN
# ========================

def main():
    ensure_dir(LOCAL_WORK_DIR)

    # 1) Scarica il CSV da FTP
    ftp_download_csv(LOCAL_CSV_PATH)

    # 2) Leggi il CSV con auto-detect delimitatore e header
    with open(LOCAL_CSV_PATH, newline="", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
            f.seek(0)
            reader = csv.DictReader(f, dialect=dialect)
        except Exception:
            f.seek(0)
            reader = csv.DictReader(f)

        print("[*] Colonne trovate nel CSV:", reader.fieldnames)

        def normalize(name):
            return (name or "").strip().lower().replace(" ", "")

        field_map = {}
        for name in reader.fieldnames or []:
            n = normalize(name)
            if "sku" in n:
                field_map["sku"] = name
            if "brand" in n or "marca" in n:
                field_map["brand"] = name

        if "sku" not in field_map or "brand" not in field_map:
            print("✖ Non riesco a trovare colonne SKU/Brand nel CSV. Controlla intestazioni.")
            return

        for row in reader:
            sku = (row.get(field_map["sku"]) or "").strip()
            brand = (row.get(field_map["brand"]) or "").strip()
            if not sku or not brand:
                continue

            process_product(sku, brand)
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    global _ftp
    if _ftp is not None:
        _ftp.quit()
        _ftp = None
        print("[*] Connessione FTP chiusa.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("✖ ERRORE FATALE:", repr(e))
        raise
