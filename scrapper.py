import aiohttp
import asyncio
import aiofiles
from bs4 import BeautifulSoup
import os
import ssl
import certifi
import traceback
import random
import re
from urllib.parse import urljoin

# Base URL & Storage Path
BASE_URL = "https://papers.nips.cc"
SAVE_DIR  = "C:/Users/YourUsername/Documents/NeurIPS_Papers/"
os.makedirs(SAVE_DIR, exist_ok=True)

# Headers to avoid blocking
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# SSL Fix
ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(certifi.where())  
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Sanitize filenames
def clean_filename(title):
    return re.sub(r'[^a-zA-Z0-9_.-]', '_', title).strip()[:200] + ".pdf"

# CSV Setup
async def initialize_csv():
    async with aiofiles.open(os.path.join(SAVE_DIR, "output.csv"), mode='w', encoding="utf-8") as f:
        await f.write("Year,Title,Authors,Paper Link,PDF Link\n")

# File Download with 2 attempts
async def fetch_pdf(session, pdf_url, file_path):
    for attempt in range(1, 3):
        try:
            async with session.get(pdf_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=90)) as response:
                if response.status == 200:
                    async with aiofiles.open(file_path, 'wb') as file:
                        await file.write(await response.read())
                    print(f"✅ Saved: {file_path}")
                    return
        except Exception as e:
            print(f"⚠️ Retry {attempt} for PDF {pdf_url}: {e}")
            await asyncio.sleep(random.randint(3, 10))
    print(f"❌ Skipped PDF: {pdf_url}")

# Fetch paper details
async def fetch_paper_info(session, year, title, paper_url, folder_path):
    for attempt in range(1, 3):
        try:
            async with session.get(paper_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=90)) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    authors = ', '.join([a.text.strip() for a in soup.select('i')])
                    pdf_tag = soup.select_one('a[href$=".pdf"]')
                    pdf_link = urljoin(BASE_URL, pdf_tag['href']) if pdf_tag else "N/A"
                    if pdf_link != "N/A":
                        await fetch_pdf(session, pdf_link, os.path.join(folder_path, clean_filename(title)))
                    async with aiofiles.open(os.path.join(SAVE_DIR, "papers.csv"), mode='a', encoding="utf-8") as f:
                        await f.write(f'"{year}","{title}","{authors}","{paper_url}","{pdf_link}"\n')
                    return
        except Exception as e:
            print(f"⚠️ Retry {attempt} for {title}: {e}")
            await asyncio.sleep(random.randint(3, 10))
    print(f"❌ Skipped paper: {title}")

# Process year
async def scrape_year(session, year):
    year_url = f"{BASE_URL}/paper_files/paper/{year}"
    year_folder = os.path.join(SAVE_DIR, str(year))
    os.makedirs(year_folder, exist_ok=True)
    for attempt in range(1, 3):
        try:
            async with session.get(year_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=90)) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    paper_links = soup.select("ul.paper-list li a")
                    if not paper_links:
                        print(f"⚠️ No papers found for {year}")
                        return
                    tasks = [fetch_paper_info(session, year, link.text.strip(), urljoin(BASE_URL, link['href']), year_folder) for link in paper_links]
                    await asyncio.gather(*tasks)
                    return
        except Exception as e:
            print(f"⚠️ Retry {attempt} for {year}: {e}")
            await asyncio.sleep(random.randint(5, 15))
    print(f"❌ Skipped year: {year}")

# Main function
async def main():
    await initialize_csv()
    years = list(range(2019, 2024))
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        await asyncio.gather(*(scrape_year(session, y) for y in years))

# Run script
if __name__ == '__main__':
    asyncio.run(main())
