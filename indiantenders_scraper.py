from playwright.sync_api import sync_playwright
import time
from tender_results.save_csv import save_tenders_to_csv


class IndianTendersScraper:

    def __init__(self, config):
        self.config = config
        self.base_url = config["base_url"]
        self.start_url = config["start_url"]
        self.s = config["selectors"]

        # 🔑 keyword handling (unchanged)
        if "search_keywords" in config:
            self.keywords = config["search_keywords"]
        elif "search_keyword" in config:
            self.keywords = [config["search_keyword"]]
        else:
            raise ValueError("No search_keyword(s) found in portal config")

        # 🔥 NEW (SAFE)
        self.max_tenders = config.get("max_tenders")  # None = unlimited

        self.tenders = []
        self.seen_ids = set()

    # =====================================================================
    def run(self):
        print("\n=== Running IndianTenders Scraper ===\n")

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()

            self._open_site(page)

            for keyword in self.keywords:
                if self._limit_reached():
                    break

                print(f"\n🔎 Searching keyword: {keyword}")
                self._open_site(page)
                self._search(page, keyword)
                self._scrape_all_pages(page, context, keyword)

            browser.close()

        csv_path = save_tenders_to_csv(self.tenders, portal="indiantenders")
        print(f"\nSaved {len(self.tenders)} tenders → {csv_path}\n")

    # =====================================================================
    def _limit_reached(self):
        return self.max_tenders and len(self.tenders) >= self.max_tenders

    # =====================================================================
    def _open_site(self, page):
        page.goto(self.start_url, timeout=60000)
        page.wait_for_load_state("networkidle")

    # =====================================================================
    def _search(self, page, keyword):
        page.fill(self.s["keyword_input"], "")
        page.fill(self.s["keyword_input"], keyword)
        page.click(self.s["search_button"])
        page.wait_for_timeout(3000)  # Angular render safety

    # =====================================================================
    def _scrape_all_pages(self, page, context, keyword):

        page_no = 1

        while True:

            if self._limit_reached():
                print(f"🛑 Limit reached ({self.max_tenders}), stopping pagination")
                break

            print(f"\n📄 Scraping page {page_no} for '{keyword}'")

            count = self._scrape_one_page(page, context, keyword)

            if count == 0:
                break

            next_btn = page.locator(self.s["next_page"])
            if not next_btn.is_visible():
                break

            next_btn.click()
            page.wait_for_timeout(2000)
            page_no += 1

    # =====================================================================
    def _scrape_one_page(self, page, context, keyword):

        cards = page.locator(self.s["tender_card"])
        count = cards.count()

        if count == 0:
            return 0

        for i in range(count):

            if self._limit_reached():
                return 0

            try:
                card = cards.nth(i)
                data = self._parse_card(card, page, context, keyword)

                dedupe_key = data["tdr_no"] or data["detail_url"]
                if dedupe_key in self.seen_ids:
                    continue

                self.seen_ids.add(dedupe_key)
                self.tenders.append(data)

            except Exception as e:
                print(f"Error parsing card {i}: {e}")

        return count

    # =====================================================================
    def _parse_card(self, card, page, context, keyword):
        s = self.s

        state = card.locator(s["state"]).inner_text().replace("State:", "").strip()
        title = card.locator(s["title"]).inner_text().strip()
        ref_id = card.locator(s["ref_id"]).inner_text().replace("Ref ID:", "").strip()
        deadline = card.locator(s["deadline"]).inner_text().replace("Deadline:", "").strip()
        value = card.locator(s["value"]).inner_text().replace("₹", "").strip()
        detail_url = card.locator(s["detail_url"]).get_attribute("href")

        detail_text = self._get_detail_page(detail_url, context)

        return {
            "portal": "indiantenders",
            "tdr_no": ref_id,
            "title": title,
            "description": detail_text,
            "tender_value": value,
            "state": state,
            "city": "",
            "closing_date": deadline,
            "detail_url": detail_url,
            "pdf_urls": "",
            "searched_keyword": keyword,
            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    # =====================================================================
    def _get_detail_page(self, url, context):
        try:
            page = context.new_page()
            page.goto(url, timeout=60000)
            page.wait_for_load_state("networkidle")
            text = page.locator("body").inner_text().strip()
            page.close()
            return text
        except Exception:
            return ""
