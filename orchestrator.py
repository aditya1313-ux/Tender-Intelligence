import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

from paths import CONFIGS_DIR, TENDER_RESULTS_DIR

from scrapers.gov.eprocure_scraper import EprocureScraper
from scrapers.aggregators.tenderdetail_scraper import TenderDetailScraper
from scrapers.aggregators.indiantenders_scraper import IndianTendersScraper

from semantic_engine.runners.run_on_csv import run_semantic_pipeline
from scrapers.aggregators.tendertiger_scraper import TenderTigerScraper



class Orchestrator:
    def __init__(self):
        with open(CONFIGS_DIR / "portals.yaml", "r", encoding="utf-8") as f:
            self.portal_configs = yaml.safe_load(f)

        self.tender_dir = TENDER_RESULTS_DIR

    # ---------------------------------------------------------
    def _run_single_scrape(self, site, keyword, max_tenders) -> Path:
        cfg = dict(self.portal_configs[site])
        cfg["search_keyword"] = keyword
        cfg["search_keywords"] = [keyword]
        cfg["max_tenders"] = max_tenders

        scraper = self._get_scraper(cfg)

        before = set(self.tender_dir.glob(f"{site}_*.csv"))
        scraper.run()
        after = set(self.tender_dir.glob(f"{site}_*.csv"))

        new_csv = max(after - before, key=lambda p: p.stat().st_mtime)
        return new_csv

    # ---------------------------------------------------------
    def run_batch_pipeline(self, sites, keywords, max_tenders):
        """
        Runs ALL (site, keyword) combinations
        → merges raw CSVs
        → runs semantic pipeline ONCE
        """

        collected_csvs = []

        for site in sites:
            for keyword in keywords:
                print(f"▶ Scraping {site} | {keyword}")
                csv_path = self._run_single_scrape(site, keyword, max_tenders)
                collected_csvs.append(csv_path)

        if not collected_csvs:
            raise RuntimeError("No tenders scraped")

        # -------- MERGE RAW CSVs --------
        dfs = [pd.read_csv(p) for p in collected_csvs]
        merged_df = pd.concat(dfs, ignore_index=True)

        merged_csv = self.tender_dir / f"_merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        merged_df.to_csv(merged_csv, index=False)

        print(f"🧩 Merged CSV created: {merged_csv}")

        # -------- SEMANTIC PIPELINE (ONCE) --------
        final_output = run_semantic_pipeline(str(merged_csv))
        return final_output

    # ---------------------------------------------------------
    def run_single_pipeline(self, site, keyword, max_tenders):
        csv_path = self._run_single_scrape(site, keyword, max_tenders)
        return run_semantic_pipeline(str(csv_path))

    # ---------------------------------------------------------
    def _get_scraper(self, cfg):
        if cfg["scraper"] == "eprocure":
            return EprocureScraper(cfg)
        if cfg["scraper"] == "tenderdetail":
            return TenderDetailScraper(cfg)
        if cfg["scraper"] == "indiantenders":
            return IndianTendersScraper(cfg)
        if cfg["scraper"] == "tendertiger":
            return TenderTigerScraper(cfg)
        raise ValueError("Unknown scraper")
