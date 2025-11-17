import csv
import os
import time
from urllib.parse import urljoin, urlparse
from ftplib import FTP
from io import BytesIO

import requests
from bs4 import BeautifulSoup

# ========================
# CONFIGURAZIONE GENERALE
# ========================
# Directory di lavoro locale solo per il CSV
LOCAL_WORK_DIR = "/tmp"
LOCAL_CSV_PATH = os.path.join(LOCAL_WORK_DIR, "prodotti.csv")

REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 3  # secondi di pausa tra prodotti

# ========================
# CONFIGURAZIONE FTP (da ENV)
# ========================
FTP_HOST = os.getenv("FTP_HOST", "it3.siteground.eu")
FTP_USER = os.getenv("FTP_USER", "foto@citymoda.cloud")
FTP_PASS = os.getenv("FTP_PASS", "!53v2cccH48!")

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
# MAPPATURA BRAND → DOMINIO UFFICIALE
# (puoi estenderla / correggerla nel tempo)
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
    "DISCLAIMER": None,  # TODO: aggiorna con dominio ufficiale
    "DSQUARED": "www.dsquared2.com",
    "EA7": "www.armani.com",  # linea EA7 su Armani
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
    "ICON": None,  # troppo generico, da gestire a parte
    "IMPERIAL": "www.imperialfashion.com",
    "KOCCA": "www.kocca.it",
    "LACOSTE": "www.lacoste.com",
    "LEVI'S": "www.levi.com",
    "LIU JO": "www.liujo.com",
    "LOVE MOSCHINO": "www.moschino.com",
    "MANILA GRACE": "www.manilagrace.com",
    "MARC ELLIS": "www.marcellis.com",
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
    "SQUAD": None,  # TODO
    "SUN 68": "www.sun68.com",
    "SUNDEK": "www.sundek.com",
    "SUNS": "www.sunsboards.com",
    "SUPERCULTURE": None,  # TODO
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
        .replace("'", "")
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
    Usa sempre path RELATIVO (senza / iniziale) e
    NON lascia l'FTP "incastrato" in quella directory.
    """
    ftp = get_ftp()
    original_cwd = ftp.pwd()

    # normalizza: niente slash iniziale
    rel_path = path.lstrip("/")

    parts = [p for p in rel_path.split("/") if p]
    for p in parts:
        try:
            ftp.cwd(p)
        except Exception:
            ftp.mkd(p)
            ftp.cwd(p)

    # torna alla dir iniziale dopo aver creato la struttura
    ftp.cwd(original_cwd)


def ftp_upload_image_stream(binary_content: bytes, remote_dir: str, filename: str):
    """
    Upload diretto: non facciamo cwd finale, ma usiamo STOR con il path completo.
    In questo modo evitiamo annidamenti progressivi.
    """
    ftp = get_ftp()

    # normalizza base dir
    rel_dir = remote_dir.lstrip("/")
    ftp_ensure_dir(rel_dir)

    # path completo del file rispetto alla root FTP
    if rel_dir:
        remote_path = f"{rel_dir.rstrip('/')}/{filename}"
    else:
        remote_path = filename

    print(f"   ⬆ Upload diretto FTP: {remote_path}")

    bio = BytesIO(binary_content)
    ftp.storbinary(f"STOR {remote_path}", bio)
    bio.close()



# ========================
# LOGICA SCRAPING BRAND
# ========================

def build_search_url(brand: str, sku: str) -> str | None:
    domain = BRAND_DOMAIN_MAP.get(brand.upper())
    if not domain:
        return None
    # Pattern generico. Se per qualche brand non funziona,
    # puoi personalizzare qui dentro con un if brand == "...":
    return f"https://{domain}/search?q={sku}"


def pick_first_product_link_from_search(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    # 1) Link con <img> (tipico risultato prodotto in griglia)
    for a in soup.find_all("a", href=True):
        if a.find("img"):
            return urljoin(base_url, a["href"])

    # 2) Fallback: link con path che assomiglia a una pagina prodotto
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

    # 2) JSON-LD con campo image
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

    # 3) Fallback: immagine "più grande" per area (width * height)
    best = None
    best_area = 0
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-srcset")
        if not src:
            continue

        # Se è srcset, prendi il primo URL
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
    remote_dir = os.path.join(FTP_IMG_BASE_DIR, brand_folder).replace("\\", "/")

    ftp_upload_image_stream(resp.content, remote_dir, filename)


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

    # 1) Scarica il CSV da FTP
    ftp_download_csv(LOCAL_CSV_PATH)

    # 2) Leggi il CSV locale (solo per i dati, non per le immagini)
    with open(LOCAL_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = (row.get("sku") or "").strip()
            brand = (row.get("brand") or "").strip()
            if not sku or not brand:
                continue

            process_product(sku, brand)
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    # Chiudi connessione FTP
    global _ftp
    if _ftp is not None:
        _ftp.quit()
        _ftp = None
        print("[*] Connessione FTP chiusa.")


if __name__ == "__main__":
    main()
