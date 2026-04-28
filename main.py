from src.scraping.emart import run_emart
from src.scraping.naivas import run_naivas
from src.scraping.quickmart import run_quickmart

from src.processing.cleaning import run_cleaning
from src.analysis.analysis import run_analysis


def main():
    print("\n🚀 Starting full data pipeline...\n")

    try:
        # -------------------------
        # 1. SCRAPING
        # -------------------------
        print("🔹 Step 1: Scraping E-Mart...")
        run_emart()

        print("🔹 Step 1: Scraping Naivas...")
        run_naivas()

        print("🔹 Step 1: Scraping Quickmart...")
        run_quickmart()

        print("✅ Scraping completed.\n")

        # -------------------------
        # 2. CLEANING
        # -------------------------
        print("🔹 Step 2: Cleaning data...")
        run_cleaning()
        print("✅ Cleaning completed.\n")

        # -------------------------
        # 3. ANALYSIS
        # -------------------------
        print("🔹 Step 3: Running analysis...")
        run_analysis()
        print("✅ Analysis completed.\n")

    except Exception as e:
        print("❌ Pipeline failed:", e)
        raise

    print("🎉 Pipeline finished successfully.\n")


if __name__ == "__main__":
    main()
