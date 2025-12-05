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

# parole chiave per ESCLUDERE immagini di layout / default
BAD_IMAGE_KEYWORDS = [
    "logo",
    "placeholder",
    "default",
    "noimage",
    "no-image",
    "spinner",
    "loader",
    "banner",
    "hero",
    "header",
    "footer",
    "icon",
    "sprite",
    "background",
    "bg_",
    "dummy",
]

# soglia minima di area per considerare una img come foto prodotto (~200x200)
MIN_IMAGE_AREA = 40000

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


def is_bad_image_url(url):
    """
    True se l'immagine è SVG o sembra una immagine di layout / default.
    """
    lower = url.lower()
    if lower.endswith(".svg") or ".svg" in lower:
        return True
    for kw in BAD_IMAGE_KEYWORDS:
        if kw in lower:
            return True
    return False


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

    ftp.cwd(ROOT_DIR)

    csv_dir = FTP_CSV_DIR.lstrip("/")
    if csv_dir:
        ftp.cwd(csv_dir)

    print(f"[*] Scarico CSV da FTP: {FTP_CSV_DIR}/{FTP_CSV_FILENAME} → {local_path}")
    with open(local_path, "wb") as f:
        ftp.retrbinary("RETR " + FTP_CSV_FILENAME, f.write)
    print("[*] CSV scaricato.")

    ftp.cwd(ROOT_DIR)


def ftp_ensure_dir(path):
    """
    Crea ricorsivamente la directory su FTP se non esiste.
    Parte SEMPRE dalla ROOT_DIR.
    """
    ftp = get_ftp()

    rel_path = path.lstrip("/")
    ftp.cwd(ROOT_DIR)

    parts = [p for p in rel_path.split("/") if p]
    for p in parts:
        try:
            ftp.cwd(p)
        except Exception:
            ftp.mkd(p)
            ftp.cwd(p)


def ftp_upload_image_stream(binary_content, remote_dir, filename):
    ftp = get_ftp()

    ftp_ensure_dir(remote_dir)

    print(f"   ⬆ Upload diretto FTP in dir '{remote_dir}': {filename}")
    bio = BytesIO(binary_content)
    ftp.storbinary("STOR " + filename, bio)
    bio.close()


# ========================
# LOGICA DI RICERCA (KOCCA / MARC ELLIS / PEUTEREY / BLAUER / GENERICA)
# ========================

def build_kocca_query_from_sku(sku):
    """
    KOCCA: CLEMENTINAM9001 → 'clementina'
    """
    s = sku.strip()
    parts = re.split(r"\d", s, maxsplit=1)
    base = parts[0] if parts else s
    return base.lower()


def build_marc_ellis_query_from_sku(sku):
    """
    MARC ELLIS: AROUNDM26BLACKLGOLD → 'around m 26 black l gold'
    """
    s = sku.strip().lower()
    s = re.sub(r"([a-z])(\d)", r"\1 \2", s)
    s = re.sub(r"(\d)([a-z])", r"\1 \2", s)
    s = s.replace("_", " ")
    return s


def build_peuterey_query_from_sku(sku):
    """
    PEUTEREY:
      I1PEUTTUCANOMQN02NER → 'tucano'
    """
    s = sku.strip()

    prefix = "I1PEUT"
    if s.upper().startswith(prefix):
        s = s[len(prefix):]

    if len(s) > 3:
        s = s[:-3]

    parts = re.split(r"\d", s, maxsplit=1)
    base = parts[0] if parts else s

    return base.lower()


def build_blauer_codes_from_sku(sku):
    """
    BLAUER:
      I1BLAUBLUC02077006943999
      → rimuove I1BLAU → BLUC02077006943999
      → rimuove ultime 3 cifre (999) → BLUC02077006943
      → split in:
         - product_code = BLUC02077
         - color_code   = 006943
      Ritorna (product_code, color_code) in UPPER.
    """
    s = sku.strip()

    prefix = "I1BLAU"
    if s.upper().startswith(prefix):
        s = s[len(prefix):]

    # rimuovi ultime 3 cifre (codice colore finale breve)
    if len(s) > 3:
        s = s[:-3]

    # separa ultimi 6 numeri (colore lungo)
    match = re.match(r"^(.*?)(\d{6})$", s)
    if match:
        product_part = match.group(1)
        color_part = match.group(2)
    else:
        product_part = s
        color_part = ""

    return product_part.upper(), color_part.upper()


def normalize_code_for_match(s):
    return re.sub(r"[^a-z0-9]", "", s.lower())


def find_kocca_product_url(sku):
    query = build_kocca_query_from_sku(sku)
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

        if sku_norm and sku_norm in handle_norm:
            score += 100
        elif sku_norm and handle_norm in sku_norm:
            score += 80

        if sku_norm and sku_norm in title_norm:
            score += 40

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
    - KOCCA / MARC ELLIS: usano JSON dedicato (gestiti altrove)
    - PEUTEREY: nome prodotto semplificato (TUCANO)
    - BLAUER: nome prodotto + colore (es. BLUC02077 006943)
    - altri brand: /search?q=SKU
    """
    b = (brand or "").strip().lower()

    if b == "peuterey":
        query = build_peuterey_query_from_sku(sku)
        # nell'esempio: q=TUCANO (ma il sito non è case-sensitive)
        q = quote_plus(query.upper())
        return f"https://www.peuterey.com/it/search/?q={q}"

    if b == "blauer":
        product_code, color_code = build_blauer_codes_from_sku(sku)
        if color_code:
            query = f"{product_code} {color_code}"
        else:
            query = product_code
        q = quote_plus(query)
        # nell'esempio:
        # https://www.blauerusa.com/eshop/search/?...&product_name=BLUC02077+006943&key=BLUC02077+006943
        return (
            "https://www.blauerusa.com/eshop/search/"
            f"?search_type=&search_id=&product_id=&product_name={q}&key={q}"
        )

    domain = BRAND_DOMAIN_MAP.get((brand or "").upper())
    if not domain:
        return None

    q = quote_plus(sku.strip())
    return f"https://{domain}/search?q={q}"


def pick_first_product_link_from_search(html, base_url):
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=True):
        if a.find("img"):
            return urljoin(base_url, a["href"])

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(base_url, href)
        path = urlparse(full).path.lower()
        if any(x in path for x in ["/product", "/prod", "/p/", "/item", "/art"]):
            return full

    return None


def extract_all_images_from_product_page(html, page_url):
    """
    Estrae TUTTE le immagini prodotto:
    - og:image
    - JSON-LD image
    - <img> in container gallery/prodotto
    Esclude:
    - SVG
    - immagini con keyword di layout (logo, banner, hero, ecc.)
    - immagini troppo piccole se non sembrano prodotto
    """
    soup = BeautifulSoup(html, "html.parser")
    urls = []

    def add_url(u):
        if not u:
            return
        full = urljoin(page_url, u)
        if is_bad_image_url(full):
            return
        if full not in urls:
            urls.append(full)

    # 1) og:image (solo se non "cattiva")
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        candidate = urljoin(page_url, og.get("content").strip())
        if not is_bad_image_url(candidate):
            add_url(candidate)

    # 2) JSON-LD con image (spesso è la gallery prodotto)
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

        if is_bad_image_url(full):
            continue

        try:
            w = int(img.get("width") or 0)
            h = int(img.get("height") or 0)
            area = w * h
        except Exception:
            area = 0

        path = urlparse(full).path.lower()

        # se area è 0 ma il path sembra prodotto, alza l'area a minimo
        if area == 0 and any(
            x in path for x in ["/product", "/prod", "/p/", "/catalog", "/item", "/files"]
        ):
            area = MIN_IMAGE_AREA

        # se area troppo piccola e non sembra prodotto → scarta
        if area < MIN_IMAGE_AREA and not any(
            x in path for x in ["/product", "/prod", "/p/", "/catalog", "/item", "/files"]
        ):
            continue

        if "/products/" in path or "/files/" in path:
            area += 100000

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
    Prima immagine: SKU.ext
    Successive: SKU_2.ext, SKU_3.ext, ...
    """
    if not img_urls:
        print("   ✖ Nessuna immagine da scaricare.")
        return

    brand_folder = brand_to_folder(brand)
    remote_dir = os.path.join(FTP_IMG_BASE_DIR, brand_folder).replace("\\", "/")

    img_index = 0
    for img_url in img_urls:
        if is_bad_image_url(img_url):
            print("   ⚠️ Ignorata immagine non valida / layout:", img_url)
            continue

        img_index += 1
        print(f"   ⬇ Download immagine #{img_index}: {img_url}")
        resp = http_get(img_url)
        if not resp:
            continue

        ext = get_file_extension_from_url(img_url)
        if ext.lower() == ".svg":
            print("   ⚠️ Ignorata immagine SVG (da estensione):", img_url)
            continue

        if img_index == 1:
            filename = f"{sku}{ext}"
        else:
            filename = f"{sku}_{img_index}{ext}"

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

    # 2) FALLBACK HTML (incl. PEUTEREY, BLAUER)
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

    print(f"   ✅ Trovate {len(img_urls)} immagini prodotto (dopo filtri).")
    download_and_upload_images(img_urls, sku, brand)


# ========================
# MAIN
# ========================

def main():
    ensure_dir(LOCAL_WORK_DIR)

    ftp_download_csv(LOCAL_CSV_PATH)

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
