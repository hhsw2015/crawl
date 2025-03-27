import requests
from bs4 import BeautifulSoup
import csv
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
import subprocess
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

csv_file = "torrent_data.csv"
MAX_RETRIES = 3
RETRY_DELAY = 5
COMMIT_INTERVAL = 10

def init_csv():
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["page_id", "id", "name", "magnet", "size", "uploader"])
    logging.info("Initialized CSV file")

def git_commit(message):
    """提交 CSV 文件到 Git 仓库"""
    try:
        # 配置 Git 身份
        subprocess.run(["git", "config", "--global", "user.email", "hhsw2015@gmail.com"], check=True)
        subprocess.run(["git", "config", "--global", "user.name", "hhsw2015"], check=True)

        subprocess.run(["git", "add", csv_file], check=True)
        result = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            logging.info(f"Git commit successful: {message}")
        else:
            logging.warning(f"No changes to commit: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git error: {e.stderr}")
        raise

def crawl_sub_page(sub_url, page_id, index, retries=0):
    torrent_id = sub_url.split("/t/")[-1]
    try:
        response = requests.get(sub_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        info_div = soup.find("div", class_="torrent_info_div")
        if info_div:
            hash_info = None
            for div in info_div.find_all("div"):
                if div.text.startswith("[hash_info]:"):
                    hash_info = div
                    break

            name = info_div.find("span", class_="tname_span")
            size = info_div.find("span", class_="tsize_span")
            uploader = info_div.find("a", class_="torrent_uploader")

            magnet = f"magnet:?xt=urn:btih:{hash_info.text.replace('[hash_info]:', '').strip()}" if hash_info else "N/A"
            logging.info(f"Sub-page data - page_id: {page_id}, id: {torrent_id}, name: {name.text if name else 'N/A'}, magnet: {magnet}")

            data = {
                "page_id": page_id,
                "id": torrent_id,
                "name": name.text.strip() if name else "N/A",
                "magnet": magnet,
                "size": size.text.strip() if size else "N/A",
                "uploader": uploader.find("span", class_="uploader_nick").text.strip() if uploader else "N/A",
                "index": index
            }
            return data
        else:
            logging.warning(f"No torrent_info_div found on {sub_url}")
            return {"page_id": page_id, "id": torrent_id, "name": "N/A", "magnet": "N/A", "size": "N/A", "uploader": "N/A", "index": index}

    except requests.RequestException as e:
        logging.error(f"Error fetching sub-page {sub_url}: {e}")
        if retries < MAX_RETRIES:
            logging.info(f"Retrying {sub_url} ({retries + 1}/{MAX_RETRIES}) after {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
            return crawl_sub_page(sub_url, page_id, index, retries + 1)
        else:
            logging.error(f"Max retries reached for {sub_url}. Stopping program.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error on {sub_url}: {e}")
        return {"page_id": page_id, "id": torrent_id, "name": "N/A", "magnet": "N/A", "size": "N/A", "uploader": "N/A", "index": index}

def crawl_torrent_pages(start_page, end_page):
    init_csv()
    pbar = tqdm(range(start_page, end_page - 1, -1), desc="Crawling pages")
    page_count = 0

    for page_id in pbar:
        url = f"https://myporn.club/ts/{page_id}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            torrent_elements = soup.find_all("div", class_="torrent_element_text_div")
            logging.info(f"Page {page_id}: Found {len(torrent_elements)} torrent elements")

            sub_tasks = []
            for index, element in enumerate(torrent_elements):
                sub_link_tag = element.find("a", class_="tdn", href=lambda x: x and "/t/" in x)
                if sub_link_tag:
                    sub_url = "https://myporn.club" + sub_link_tag["href"]
                    sub_tasks.append((sub_url, page_id, index))

            results = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(crawl_sub_page, url, pid, idx) for url, pid, idx in sub_tasks]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        results.append(result)

            results.sort(key=lambda x: x["index"])
            with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                for data in results:
                    writer.writerow([data["page_id"], data["id"], data["name"], data["magnet"], data["size"], data["uploader"]])

            page_count += 1
            if page_count % COMMIT_INTERVAL == 0:
                git_commit(f"Update data for pages {page_id + COMMIT_INTERVAL - 1} to {page_id}")

            pbar.update(1)

        except requests.RequestException as e:
            logging.error(f"Error fetching page {url}: {e}")
            time.sleep(5)

    if page_count % COMMIT_INTERVAL != 0:
        git_commit(f"Final update for pages {start_page} to {end_page}")

if __name__ == "__main__":
    logging.info("Starting crawl...")
    start_page = int(os.getenv("START_PAGE", 5572))
    end_page = int(os.getenv("END_PAGE", 5570))
    crawl_torrent_pages(start_page, end_page)
    logging.info(f"Data saved to {csv_file}")
