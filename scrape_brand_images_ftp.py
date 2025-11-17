import csv
import os
import time
from urllib.parse import urljoin, urlparse
from ftplib import FTP

import requests
from bs4 import BeautifulSoup

# ========================
# CONFIGURAZIONE GENERALE
# ========================
# Paths locali (sul server Render o dove lanci lo script)
LOCAL_WORK_DIR = "/tmp"  # puoi cambiarlo se vuoi
LOCAL_CSV_PATH = os.path.join(LOCAL_WORK_DIR, "prodotti.csv")
LOCAL_IMG_DIR = os.path.join(LOCAL_WORK_DIR, "immagini_brand")

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 3

# ========================
# CONFIGURAZIONE FTP (da ENV)
# ========================
FTP_HOST = os.getenv("FTP_HOST", "ftp.tuoserver.com")
FTP_USER = os.getenv("FTP_USER", "username")
FTP_PASS = os.getenv("FTP_PASS", "password")

FTP_CSV_DIR = os.getenv("FTP_CSV_DIR", "/input")
FTP_CSV_FILENAME = os.getenv("FTP_CSV_FILENAME", "prodotti.csv")
FTP_IMG_BASE_DIR = os.getenv("FTP_IMG_BASE_DIR", "/images")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# ===============================
# MAPPATURA BRAND → DOMINIO UFFICIALE
# (DA ESTENDERE/AGGIUSTARE)
# ===============================
BRAND_DOMAIN_MAP = {
    "ADIDAS": "www.adidas.it",
    "TOMMY HILFIGER": "it.tommy.com",
    "TOMMY JEANS": "it.tommy.com",
    "GUESS": "www.guess.eu",
    "GUESS JEANS": "www.guess.eu",
    "GUESS by MARCIANO": "www.guess.eu",
    "VANS": "www.vans.com",
    "THE NORTH FACE": "www.thenorthface.it",
    "CALVIN KLEIN": "www.calvinklein.it",
    "CALVIN KLEIN JEANS": "www.calvinklein.it",
    "LIU JO": "www.liujo.com",
    "NAPAPIJRI": "www.napapijri.com",
    "RALPH LAUREN": "www.ralphlauren.it",
    "GEOX": "www.geox.com",
    "NEW BALANCE": "www.newbalance.it",
    "TIMBERLAND": "www.timberland.it",
    "SKECHERS": "www.skechers.it",
    "PEUTEREY": "www.peuterey.com",
    "VICOLO": "www.vicolofashion.com",
    # ... aggiungi tutti gli altri brand che ti servono ...
}


# ========================
# UTILITY DI BASE
# ========================
def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def brand_to_folder(brand: str) -> str:
    slug = (
        brand.strip()
        .lower()
        .replace(" ", "_")
        .replace("&", "e")
        .replace("°", "")
        .replace(".", "")
    )
    return slug


def http_get(url: str) -> requests.Response | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            print(f"   ✖ Richiesta fallita ({resp.status_code}) → {url}")
            return None
        return resp
    except Exception as e:
        print(f"   ✖ Errore richiesta {url}: {e}")
        return None


def get_file_extension_from_url(url: str) -> str:
    path = urlparse(url).path
    _, ext = os.path.splitext(path)
    if ext:
        return ext.split("?")[0]
    return ".jpg"


# ========================
# FUNZIONI FTP
# ========================
_ftp = None


def get_ftp() -> FTP:
    global _ftp
    if _ftp is None:
        print(f"[*] Connessione FTP a {FTP_HOST}...")
        _ftp = FTP(FTP_HOST)
        _ftp.login(FTP_USER, FTP_PASS)
        print("[*] Connesso a FTP.")
    return _ftp


def ftp_download_csv(local_path: str):
    ftp = get_ftp()
    ftp.cwd(FTP_CSV_DIR)
    print(f"[*] Scarico CSV da FTP: {FTP_CSV_DIR}/{FTP_CSV_FILENAME} → {local_path}")
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {FTP_CSV_FILENAME}", f.write)
    print("[*] CSV scaricato.")


def ftp_ensure_dir(path: str):
    """
    Crea ricorsivamente la directory su FTP se non esiste.
    """
    ftp = get_ftp()
    original_cwd = ftp.pwd()
    parts = [p for p in path.split("/") if p]
    for p in parts:
        try:
            ftp.cwd(p)
        except Exception:
            ftp.mkd(p)
            ftp.cwd(p)
    # torna alla dir iniziale
    ftp.cwd(original_cwd)


def ftp_upload_image(local_path: str, remote_dir: str, filename: str):
    ftp = get_ftp()
    # assicurati che la directory esista
    ftp_ensure_dir(remote_dir)
    # vai nella directory destinazione
    ftp.cwd(remote_dir)
    print(f"   ⬆ Carico su FTP: {remote_dir}/{filename}")
    with open(local_path, "rb") as f:
        ftp.storbinary(f"STOR {filename}", f)


# ========================
# LOGICA SCRAPING BRAND
# ========================
def build_search_url(brand: str, sku: str) -> str | None:
    domain = BRAND_DOMAIN_MAP.get(brand.upper())
    if not domain:
        return None
    # pattern generico
    return f"https://{domain}/search?q={sku}"


def pick_first_product_link_from_search(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    # 1) link con <img> (tipico prodotto)
    for a in soup.find_all("a", href=True):
        if a.find("img"):
            return urljoin(base_url, a["href"])

    # 2) fallback su link con pattern "product"
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = urljoin(base_url, href)
        path = urlparse(full).path.lower()
        if any(x in path for x in ["/product", "/prod", "/p/", "/item", "/art"]):
            return full

    return None


def extract_main_image_from_product_page(html: str, page_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    # 1) og:image
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return urljoin(page_url, og["content"].strip())

    # 2) JSON-LD con image
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json
            data = json.loads(script.string or "{}")
            if isinstance(data, dict) and "image" in data:
                img = data["image"]
                if isinstance(img, list) and img:
                    return urljoin(page_url, img[0])
                if isinstance(img, str):
                    return urljoin(page_url, img)
        except Exception:
            pass

    # 3) fallback prima immagine grande
    best = None
    best_area = 0
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-srcset")
        if not src:
            continue

        if "," in src:
            src = src.split(",")[0].split(" ")[0]

        full = urljoin(page_url, src)

        try:
            w = int(img.get("width") or 0)
            h = int(img.get("height") or 0)
            area = w * h
        except Exception:
            area = 0

        if area > best_area:
            best_area = area
            best = full

    return best


def download_and_upload_image(img_url: str, sku: str, brand: str):
    print(f"   ⬇ Download immagine: {img_url}")
    resp = http_get(img_url)
    if not resp:
        return

    ext = get_file_extension_from_url(img_url)
    filename = f"{sku}{ext}"

    brand_folder = brand_to_folder(brand)

    # Percorso locale
    local_brand_dir = os.path.join(LOCAL_IMG_DIR, brand_folder)
    ensure_dir(local_brand_dir)
    local_path = os.path.join(local_brand_dir, filename)

    # Salva in locale
    try:
        with open(local_path, "wb") as f:
            f.write(resp.content)
        print(f"   ✅ Salvata localmente come {local_path}")
    except Exception as e:
        print(f"   ✖ Errore nel salvataggio locale {filename}: {e}")
        return

    # Percorso remoto FTP
    remote_dir = os.path.join(FTP_IMG_BASE_DIR, brand_folder).replace("\\", "/")
    ftp_upload_image(local_path, remote_dir, filename)


def process_product(sku: str, brand: str):
    print(f"\n➡️ SKU: {sku} | Brand: {brand}")

    search_url = build_search_url(brand, sku)
    if not search_url:
        print("   ✖ Brand non mappato in BRAND_DOMAIN_MAP. Aggiorna il dizionario.")
        return

    print(f"   🔍 Cerco prodotto su: {search_url}")
    search_resp = http_get(search_url)
    if not search_resp:
        return

    product_url = pick_first_product_link_from_search(search_resp.text, search_url)
    if not product_url:
        print("   ✖ Nessuna pagina prodotto trovata.")
        return

    print(f"   🔗 Pagina prodotto: {product_url}")
    time.sleep(SLEEP_BETWEEN_REQUESTS)

    product_resp = http_get(product_url)
    if not product_resp:
        return

    img_url = extract_main_image_from_product_page(product_resp.text, product_url)
    if not img_url:
        print("   ✖ Nessuna immagine trovata nella pagina prodotto.")
        return

    download_and_upload_image(img_url, sku, brand)


def main():
    ensure_dir(LOCAL_WORK_DIR)
    ensure_dir(LOCAL_IMG_DIR)

    # 1) scarica il CSV da FTP
    ftp_download_csv(LOCAL_CSV_PATH)

    # 2) leggi il CSV locale
    with open(LOCAL_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = (row.get("sku") or "").strip()
            brand = (row.get("brand") or "").strip()
            if not sku or not brand:
                continue

            process_product(sku, brand)
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    # chiudi connessione FTP
    global _ftp
    if _ftp is not None:
        _ftp.quit()
        _ftp = None
        print("[*] Connessione FTP chiusa.")


if __name__ == "__main__":
    main()
