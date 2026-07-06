from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from pathlib import Path

import psutil
import requests
from bs4 import BeautifulSoup


BENCHMARK_DIR = Path("data/benchmark")
BENCHMARK_ARTICLES = BENCHMARK_DIR / "benchmark_articles.jsonl"
BENCHMARK_VISITED = BENCHMARK_DIR / "benchmark_visited_urls.txt"

# This file is read by make_nst_charts.py
PERF_CSV = Path("data/raw/performance_crawl.csv")

SITEMAP_INDEX = "https://www.nst.com.my/sitemap.xml"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}


def log(message: str) -> None:
    print(message, flush=True)


def normalize_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def make_record_id(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def start_cpu_monitor() -> tuple[list[float], threading.Event, threading.Thread]:
    """
    Measure CPU continuously during the benchmark.
    This avoids getting 0.0% CPU after the crawl has already finished.
    """
    samples: list[float] = []
    stop_event = threading.Event()

    def monitor() -> None:
        psutil.cpu_percent(interval=None)
        while not stop_event.is_set():
            samples.append(psutil.cpu_percent(interval=1.0))

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    return samples, stop_event, thread


def fetch_url(url: str, timeout: int = 20) -> str:
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def get_soup_xml(xml_text: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(xml_text, "xml")
    except Exception:
        return BeautifulSoup(xml_text, "html.parser")


def discover_sitemaps(max_sitemaps: int) -> list[str]:
    try:
        xml = fetch_url(SITEMAP_INDEX)
        soup = get_soup_xml(xml)

        sitemaps = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
        filtered = [url for url in sitemaps if "sitemap" in url.lower()]

        if filtered:
            return filtered[:max_sitemaps]

    except Exception as error:
        log(f"[sitemap] Failed to read sitemap index: {error}")

    return [SITEMAP_INDEX][:max_sitemaps]


def extract_urls_from_sitemap(sitemap_url: str) -> list[str]:
    try:
        xml = fetch_url(sitemap_url)
        soup = get_soup_xml(xml)

        urls = []
        for loc in soup.find_all("loc"):
            url = loc.get_text(strip=True)
            if "nst.com.my" in url and "/news/" in url:
                urls.append(url)

        return urls

    except Exception as error:
        log(f"[sitemap] Failed {sitemap_url}: {error}")
        return []


def collect_candidate_urls(max_sitemaps: int, max_urls: int) -> list[str]:
    sitemaps = discover_sitemaps(max_sitemaps=max_sitemaps)

    all_urls = []
    seen = set()

    for sitemap_url in sitemaps:
        urls = extract_urls_from_sitemap(sitemap_url)
        log(f"[sitemap] {sitemap_url} -> {len(urls)} article URLs")

        for url in urls:
            if url not in seen:
                seen.add(url)
                all_urls.append(url)

            if len(all_urls) >= max_urls:
                return all_urls

    return all_urls


def parse_article(url: str) -> dict | None:
    try:
        html = fetch_url(url)
        soup = BeautifulSoup(html, "html.parser")

        headline = ""
        h1 = soup.find("h1")
        if h1:
            headline = normalize_text(h1.get_text(" ", strip=True))
        elif soup.title:
            headline = normalize_text(soup.title.get_text(" ", strip=True))

        if not headline:
            return None

        summary = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            summary = normalize_text(meta_desc.get("content"))

        author = ""
        author_meta = soup.find("meta", attrs={"name": "author"})
        if author_meta and author_meta.get("content"):
            author = normalize_text(author_meta.get("content"))

        publication_date = ""
        pub_meta = soup.find("meta", attrs={"property": "article:published_time"})
        if pub_meta and pub_meta.get("content"):
            publication_date = normalize_text(pub_meta.get("content"))

        modified_date = ""
        mod_meta = soup.find("meta", attrs={"property": "article:modified_time"})
        if mod_meta and mod_meta.get("content"):
            modified_date = normalize_text(mod_meta.get("content"))

        keywords = ""
        keywords_meta = soup.find("meta", attrs={"name": "keywords"})
        if keywords_meta and keywords_meta.get("content"):
            keywords = normalize_text(keywords_meta.get("content"))

        section = ""
        match = re.search(r"nst\.com\.my/([^/]+/[^/]+)/", url)
        if match:
            section = match.group(1)

        paragraphs = []
        article = soup.find("article") or soup
        for p in article.find_all("p"):
            text = normalize_text(p.get_text(" ", strip=True))
            if len(text) > 30:
                paragraphs.append(text)

        body_preview = " ".join(paragraphs[:3])
        combined_text = f"{headline} {summary} {body_preview}".strip()
        word_count = len(re.findall(r"\w+", combined_text))

        return {
            "record_id": make_record_id(url),
            "url": url,
            "headline": headline,
            "publication_date": publication_date,
            "modified_date": modified_date,
            "section": section,
            "author": author,
            "summary": summary,
            "keywords": keywords,
            "body_preview": body_preview,
            "word_count": word_count,
            "collected_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception:
        return None


def append_jsonl(record: dict) -> None:
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    with BENCHMARK_ARTICLES.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


def save_visited(url: str) -> None:
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    with BENCHMARK_VISITED.open("a", encoding="utf-8") as file:
        file.write(url + "\n")


def load_benchmark_visited() -> set[str]:
    if not BENCHMARK_VISITED.exists():
        return set()

    with BENCHMARK_VISITED.open("r", encoding="utf-8") as file:
        return {line.strip() for line in file if line.strip()}


def clear_benchmark_files() -> None:
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    if BENCHMARK_ARTICLES.exists():
        BENCHMARK_ARTICLES.unlink()
    if BENCHMARK_VISITED.exists():
        BENCHMARK_VISITED.unlink()


def append_performance(mode: str, records: int, seconds: float, cpu_percent: float) -> None:
    PERF_CSV.parent.mkdir(parents=True, exist_ok=True)

    exists = PERF_CSV.exists()
    process = psutil.Process()
    memory_mb = process.memory_info().rss / (1024 * 1024)
    throughput = records / seconds if seconds else 0

    with PERF_CSV.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "dataset",
                "mode",
                "records",
                "seconds",
                "records_per_second",
                "cpu_percent",
                "memory_mb",
            ],
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
                "cpu_percent": round(cpu_percent, 2),
                "memory_mb": round(memory_mb, 2),
            }
        )


def crawl_basic(urls: list[str], target_records: int, delay: float) -> int:
    visited = load_benchmark_visited()
    collected = 0

    for url in urls:
        if collected >= target_records:
            break

        if url in visited:
            continue

        record = parse_article(url)

        if record:
            append_jsonl(record)
            save_visited(url)
            visited.add(url)
            collected += 1
            log(f"[basic] {collected:,}/{target_records:,} {record['headline'][:80]}")

        if delay > 0:
            time.sleep(delay)

    return collected


def crawl_threaded(urls: list[str], target_records: int, workers: int, delay: float) -> int:
    visited = load_benchmark_visited()
    candidate_urls = [url for url in urls if url not in visited]

    collected = 0
    index = 0
    pending = set()

    def task(url: str) -> tuple[str, dict | None]:
        if delay > 0:
            time.sleep(delay)
        return url, parse_article(url)

    executor = ThreadPoolExecutor(max_workers=workers)

    try:
        while index < len(candidate_urls) and len(pending) < workers and collected < target_records:
            future = executor.submit(task, candidate_urls[index])
            pending.add(future)
            index += 1

        while pending and collected < target_records:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)

            for future in done:
                if collected >= target_records:
                    break

                try:
                    url, record = future.result()
                except Exception:
                    url, record = "", None

                if record and url not in visited:
                    append_jsonl(record)
                    save_visited(url)
                    visited.add(url)
                    collected += 1
                    log(f"[threaded] {collected:,}/{target_records:,} {record['headline'][:80]}")

                if index < len(candidate_urls) and collected < target_records:
                    new_future = executor.submit(task, candidate_urls[index])
                    pending.add(new_future)
                    index += 1

        for future in pending:
            future.cancel()

    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return collected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fair benchmark for NST crawler modes")
    parser.add_argument("--mode", choices=["basic", "threaded"], required=True)
    parser.add_argument("--target-records", type=int, default=500)
    parser.add_argument("--max-sitemaps", type=int, default=10)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear benchmark JSONL and benchmark visited file before this run.",
    )
    parser.add_argument(
        "--reset-performance",
        action="store_true",
        help="Delete data/raw/performance_crawl.csv before this run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.reset_performance and PERF_CSV.exists():
        PERF_CSV.unlink()
        log(f"Deleted old crawl performance file: {PERF_CSV}")

    if args.clear:
        clear_benchmark_files()
        log(f"Cleared benchmark files in: {BENCHMARK_DIR}")

    candidate_limit = max(args.target_records * 5, args.target_records + 100)
    urls = collect_candidate_urls(
        max_sitemaps=args.max_sitemaps,
        max_urls=candidate_limit,
    )

    if not urls:
        raise RuntimeError("No candidate URLs found from NST sitemaps.")

    log(f"Candidate URLs discovered: {len(urls):,}")
    log(f"Benchmark mode: {args.mode}")
    log(f"Target records: {args.target_records:,}")

    cpu_samples, cpu_stop, cpu_thread = start_cpu_monitor()

    start = time.perf_counter()

    if args.mode == "basic":
        records = crawl_basic(
            urls=urls,
            target_records=args.target_records,
            delay=args.delay,
        )
    else:
        records = crawl_threaded(
            urls=urls,
            target_records=args.target_records,
            workers=args.workers,
            delay=args.delay,
        )

    seconds = time.perf_counter() - start

    cpu_stop.set()
    cpu_thread.join(timeout=2)

    avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0

    append_performance(args.mode, records, seconds, avg_cpu)

    log("=" * 60)
    log(f"Benchmark complete: {args.mode}")
    log(f"Records: {records:,}")
    log(f"Seconds: {seconds:.3f}")
    log(f"Average CPU: {avg_cpu:.2f}%")
    log(f"Throughput: {records / seconds if seconds else 0:.3f} records/sec")
    log(f"Benchmark raw output: {BENCHMARK_ARTICLES}")
    log(f"Performance output: {PERF_CSV}")
    log("=" * 60)


if __name__ == "__main__":
    main()
