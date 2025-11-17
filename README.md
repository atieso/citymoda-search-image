# citymoda-search-image

Worker per cercare immagini prodotto sui siti ufficiali dei brand a partire da un CSV `sku,brand` su FTP, e caricare le immagini sempre su FTP, pronte per essere usate nelle schede prodotto (es. Shopify).

## Flusso

1. Su FTP carichi:
   - `/input/prodotti.csv` con colonne: `sku,brand`

2. Il worker su Render:
   - scarica `/input/prodotti.csv`
   - per ogni riga:
     - cerca il prodotto sul sito ufficiale del brand
     - prende l'immagine principale (og:image, JSON-LD o la più grande)
     - salva in locale
     - carica su FTP in `/images/<brand_slug>/<sku>.jpg`

Esempio su FTP:

```text
/input/prodotti.csv
/images/adidas/GX1234.jpg
/images/tommy_hilfiger/TJM12345.jpg
