from __future__ import annotations

import argparse
import csv
import json
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import psutil
import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.nst.com.my"
SITEMAP_INDEX = "https://www.nst.com.my/sitemap.xml"
ROBOTS_URL = "https://www.nst.com.my/robots.txt"
DEFAULT_USER_AGENT = "UTM-Academic-NSTCrawler/1.0 (+coursework; metadata collection)"

RAW_DIR = Path("data/raw")
LOG_DIR = Path("data/logs")
RAW_JSONL = RAW_DIR / "nst_articles.jsonl"
RAW_CSV = RAW_DIR / "nst_articles.csv"
VISITED_PATH = RAW_DIR / "nst_visited_urls.txt"
CRAWL_LOG = LOG_DIR / "nst_crawling_log.csv"
PERF_CSV = RAW_DIR / "performance_crawl.csv"

Record = dict[str, Any]


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def build_session(user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def load_robot_parser() -> RobotFileParser:
    parser = RobotFileParser()
    parser.set_url(ROBOTS_URL)
    parser.read()
    return parser


def can_fetch(robots: RobotFileParser, user_agent: str, url: str) -> bool:
    try:
        return robots.can_fetch(user_agent, url)
    except Exception:
        return False


def polite_get(session: requests.Session, url: str, delay: float) -> requests.Response:
    if delay > 0:
        time.sleep(delay)
    response = session.get(url, timeout=45)
    response.raise_for_status()
    return response


def xml_locs(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text.encode("utf-8"))
    locs: list[str] = []
    for element in root.iter():
        if element.tag.endswith("loc") and element.text:
            locs.append(element.text.strip())
    return locs


def looks_like_article(url: str) -> bool:
    path = urlparse(url).path.lower()
    return bool(re.search(r"/\d{4}/\d{2}/\d+/", path))


def append_crawl_log(target: str, status: str, records: int, error: str = "") -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    exists = CRAWL_LOG.exists()
    with CRAWL_LOG.open("a", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=["target", "status", "records", "time", "error"])
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "target": target,
                "status": status,
                "records": records,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error": error,
            }
        )


def discover_article_urls(
    session: requests.Session,
    robots: RobotFileParser,
    user_agent: str,
    target_records: int,
    max_sitemaps: int,
    skip_sitemaps: int,
    delay: float,
) -> list[str]:
    if not can_fetch(robots, user_agent, SITEMAP_INDEX):
        raise RuntimeError(f"robots.txt does not allow sitemap index: {SITEMAP_INDEX}")

    sitemap_index = polite_get(session, SITEMAP_INDEX, delay).text
    all_sitemap_urls = [url for url in xml_locs(sitemap_index) if "sitemap-news-" in url]
    sitemap_urls = all_sitemap_urls[skip_sitemaps : skip_sitemaps + max_sitemaps]
    log(
        f"Using {len(sitemap_urls):,} monthly sitemaps "
        f"after skipping {skip_sitemaps:,} of {len(all_sitemap_urls):,} available."
    )
    article_urls: list[str] = []
    seen: set[str] = set()

    for sitemap_url in sitemap_urls:
        if len(article_urls) >= target_records:
            break
        if not can_fetch(robots, user_agent, sitemap_url):
            append_crawl_log(sitemap_url, "robots_skipped", 0)
            continue
        try:
            sitemap_xml = polite_get(session, sitemap_url, delay).text
            urls = [url for url in xml_locs(sitemap_xml) if looks_like_article(url)]
        except Exception as exc:
            append_crawl_log(sitemap_url, "sitemap_failed", 0, str(exc))
            continue

        added = 0
        for url in urls:
            if url not in seen:
                seen.add(url)
                article_urls.append(url)
                added += 1
                if len(article_urls) >= target_records:
                    break
        append_crawl_log(sitemap_url, "sitemap_success", added)
        log(f"Sitemap {Path(urlparse(sitemap_url).path).name}: +{added:,} URLs; total {len(article_urls):,}")

    return article_urls[:target_records]


def meta_content(soup: BeautifulSoup, *names: str) -> str:
    for name in names:
        tag = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag["content"].strip()
    return ""


def section_from_url(url: str) -> str:
    parts = [part for part in urlparse(url).path.split("/") if part]
    if len(parts) >= 2:
        return "/".join(parts[:2])
    return parts[0] if parts else ""


def extract_article(url: str, html: str) -> Record | None:
    soup = BeautifulSoup(html, "html.parser")
    title = meta_content(soup, "og:title", "twitter:title")
    if not title and soup.find("h1"):
        title = soup.find("h1").get_text(" ", strip=True)
    title = title.replace(" | New Straits Times", "").strip()

    summary = meta_content(soup, "description", "og:description", "twitter:description")
    published_at = meta_content(soup, "article:published_time", "pubdate", "date")
    modified_at = meta_content(soup, "article:modified_time")
    author = meta_content(soup, "author", "article:author")
    section = meta_content(soup, "article:section") or section_from_url(url)
    keywords = meta_content(soup, "keywords", "news_keywords")

    paragraphs = []
    article_node = soup.find("article") or soup.select_one("[class*=article], [class*=story], main")
    source = article_node if article_node else soup
    for paragraph in source.find_all("p"):
        text = paragraph.get_text(" ", strip=True)
        if len(text) >= 40:
            paragraphs.append(text)
    body_preview = "\n".join(dict.fromkeys(paragraphs[:8]))

    if not title:
        return None

    return {
        "record_id": url,
        "url": url,
        "headline": title,
        "publication_date": published_at,
        "modified_date": modified_at,
        "section": section,
        "author": author,
        "summary": summary,
        "keywords": keywords,
        "body_preview": body_preview,
        "word_count": len(re.findall(r"\w+", body_preview)),
        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def load_visited() -> set[str]:
    if not VISITED_PATH.exists():
        return set()
    return {line.strip() for line in VISITED_PATH.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_visited(urls: list[str]) -> None:
    if not urls:
        return
    with VISITED_PATH.open("a", encoding="utf-8") as file:
        for url in urls:
            file.write(url + "\n")


def append_jsonl(records: list[Record]) -> None:
    if not records:
        return
    with RAW_JSONL.open("a", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def crawl_one(url: str, user_agent: str, robots: RobotFileParser, delay: float) -> Record | None:
    if not can_fetch(robots, user_agent, url):
        append_crawl_log(url, "robots_skipped", 0)
        return None
    session = build_session(user_agent)
    try:
        html = polite_get(session, url, delay).text
        record = extract_article(url, html)
        append_crawl_log(url, "article_success" if record else "article_incomplete", 1 if record else 0)
        return record
    except Exception as exc:
        append_crawl_log(url, "article_failed", 0, str(exc))
        return None


def crawl_basic(urls: list[str], args: argparse.Namespace, robots: RobotFileParser, visited: set[str]) -> int:
    total = 0
    for url in urls:
        if total >= args.target_records:
            break
        if url in visited:
            continue
        record = crawl_one(url, args.user_agent, robots, args.delay)
        if record:
            append_jsonl([record])
            append_visited([url])
            total += 1
            log(f"[basic] {total:,}/{args.target_records:,} {record['headline'][:80]}")
    return total


def crawl_threaded(urls: list[str], args: argparse.Namespace, robots: RobotFileParser, visited: set[str]) -> int:
    total = 0
    pending = [url for url in urls if url not in visited]
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(crawl_one, url, args.user_agent, robots, args.delay): url for url in pending}
        for future in as_completed(futures):
            if total >= args.target_records:
                break
            url = futures[future]
            record = future.result()
            if record:
                append_jsonl([record])
                append_visited([url])
                total += 1
                log(f"[threaded] {total:,}/{args.target_records:,} {record['headline'][:80]}")
    return total


def export_csv() -> None:
    if not RAW_JSONL.exists():
        return
    rows = [json.loads(line) for line in RAW_JSONL.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows:
        return
    fieldnames = sorted({field for row in rows for field in row.keys()})
    with RAW_CSV.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def append_performance(mode: str, records: int, seconds: float) -> None:
    exists = PERF_CSV.exists()
    process = psutil.Process()
    memory_mb = process.memory_info().rss / (1024 * 1024)
    cpu_percent = psutil.cpu_percent(interval=0.2)
    throughput = records / seconds if seconds else 0
    with PERF_CSV.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["dataset", "mode", "records", "seconds", "records_per_second", "cpu_percent", "memory_mb"],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "dataset": "nst",
                "mode": mode,
                "records": records,
                "seconds": round(seconds, 3),
                "records_per_second": round(throughput, 3),
                "cpu_percent": cpu_percent,
                "memory_mb": round(memory_mb, 2),
            }
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NST metadata crawler")
    parser.add_argument("--mode", choices=["basic", "threaded"], default="basic")
    parser.add_argument("--target-records", type=int, default=1000)
    parser.add_argument("--max-sitemaps", type=int, default=12)
    parser.add_argument("--skip-sitemaps", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    session = build_session(args.user_agent)
    robots = load_robot_parser()
    visited = load_visited()

    log("Discovering NST article URLs from monthly news sitemaps...")
    urls = discover_article_urls(
        session,
        robots,
        args.user_agent,
        args.target_records * 2,
        args.max_sitemaps,
        args.skip_sitemaps,
        args.delay,
    )
    log(f"Discovered {len(urls):,} article URLs; visited already: {len(visited):,}")

    started = time.perf_counter()
    if args.mode == "basic":
        records = crawl_basic(urls, args, robots, visited)
    else:
        records = crawl_threaded(urls, args, robots, visited)
    seconds = time.perf_counter() - started

    export_csv()
    if records > 0:
        append_performance(args.mode, records, seconds)
    log(f"DONE -- saved {records:,} records in {seconds / 60:.1f} min.")


if __name__ == "__main__":
    main()
