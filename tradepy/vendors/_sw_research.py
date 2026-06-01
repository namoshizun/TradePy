import requests as rq


def fetch_stock_industry_classification_history():
    url = "https://www.swsresearch.com/swindex/pdf/SwClass2021/StockClassifyUse_stock.xls"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = rq.get(url, headers=headers, verify=False)
    response.raise_for_status()

    filename = url.split("/")[-1]
    with open(filename, "wb") as f:
        f.write(response.content)
