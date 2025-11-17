import io
import re
import time
import zipfile
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    # AutoNation's eBrochure endpoint expects a same-site referer.
    "Referer": "https://www.autonation.com/",
}

st.set_page_config(page_title="AutoNation Carfax Fetcher", layout="wide")
st.title("🚗 AutoNation eBrochure → Carfax Fetcher")

st.write(
    "Upload a CSV with your inventory (including eBrochure links), "
    "and I’ll extract Carfax URLs and optionally download the reports."
)

uploaded = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded is not None:
    # Read CSV
    df = pd.read_csv(uploaded)

    st.subheader("Preview of uploaded data")
    st.dataframe(df.head())

    if df.empty:
        st.error("The CSV appears to be empty.")
        st.stop()

    # Guess columns
    def guess_col(candidates, default=None):
        cols_lower = {c.lower(): c for c in df.columns}
        for pattern in candidates:
            for lower_name, actual_name in cols_lower.items():
                if pattern in lower_name:
                    return actual_name
        return default or df.columns[0]

    guessed_ebrochure_col = guess_col(["ebrochure", "e-brochure", "brochure", "vlp"])
    guessed_vin_col = guess_col(["vin"])

    st.subheader("Column selection")

    ebrochure_col = st.selectbox(
        "Select the column that contains the eBrochure URL",
        options=list(df.columns),
        index=list(df.columns).index(guessed_ebrochure_col)
        if guessed_ebrochure_col in df.columns
        else 0,
    )

    vin_col = st.selectbox(
        "Select the column that contains the VIN (used for filenames)",
        options=list(df.columns),
        index=list(df.columns).index(guessed_vin_col)
        if guessed_vin_col in df.columns
        else 0,
    )

    download_carfax_files = st.checkbox(
        "Download Carfax pages/PDFs as a ZIP (not just URLs)", value=True
    )

    start = st.button("Start scraping Carfax links")

    if start:
        results = []
        carfax_files = {}  # vin -> (filename, bytes)

        progress = st.progress(0)
        status_text = st.empty()

        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)

        total = len(df)

        hyperlink_pattern = re.compile(
            r"=HYPERLINK\(\"([^\"]+)\"(?:,\"[^\"]*\")?\)", re.IGNORECASE
        )

        def fetch_ebrochure(url: str, retries: int = 3, backoff: float = 1.5):
            """Fetch an AutoNation eBrochure page with a few retries."""

            last_exc = None
            for attempt in range(1, retries + 1):
                try:
                    resp = session.get(url, timeout=30, allow_redirects=True)
                    resp.raise_for_status()
                    return resp
                except Exception as exc:
                    last_exc = exc
                    if attempt == retries:
                        break
                    # Gentle backoff between attempts
                    time.sleep(backoff * attempt)

            raise RuntimeError(
                f"Failed to fetch eBrochure after {retries} attempts: {last_exc}"
            )

        def normalize_url(raw_value: str) -> str:
            """Handle Excel-style HYPERLINK formulas and bare domains."""

            if not isinstance(raw_value, str):
                raw_value = "" if pd.isna(raw_value) else str(raw_value)

            cleaned = raw_value.strip().strip("'\"")

            match = hyperlink_pattern.fullmatch(cleaned)
            if match:
                cleaned = match.group(1).strip()

            if cleaned.startswith("www."):
                cleaned = f"https://{cleaned}"

            return cleaned

        def is_valid_url(url: str) -> bool:
            try:
                parsed = urlparse(url)
            except Exception:
                return False
            return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

        for idx, row in df.iterrows():
            progress.progress((idx + 1) / total)
            vin = str(row[vin_col]) if not pd.isna(row[vin_col]) else f"row_{idx}"
            ebrochure_url = normalize_url(row[ebrochure_col])

            status_text.text(f"[{idx+1}/{total}] Processing VIN {vin}…")

            carfax_url = None
            error = None
            file_info = None

            # Basic sanity check
            if not is_valid_url(ebrochure_url):
                error = "Invalid eBrochure URL"
                results.append(
                    {
                        "VIN": vin,
                        "EBROCHURE_URL": ebrochure_url,
                        "CARFAX_URL": None,
                        "STATUS": "ERROR",
                        "ERROR_MESSAGE": error,
                    }
                )
                continue

            try:
                # 1) Fetch eBrochure HTML
                resp = fetch_ebrochure(ebrochure_url)

                soup = BeautifulSoup(resp.text, "html.parser")

                # 2) Find the Carfax link by class
                tag = soup.find("a", class_="j-carfax-link")
                if not tag or not tag.get("href"):
                    error = "No j-carfax-link anchor found"
                    results.append(
                        {
                            "VIN": vin,
                            "EBROCHURE_URL": ebrochure_url,
                            "CARFAX_URL": None,
                            "STATUS": "NO_CARFAX_LINK",
                            "ERROR_MESSAGE": error,
                        }
                    )
                    continue

                carfax_url = tag["href"].strip()

                # 3) Optionally download the Carfax page / PDF
                if download_carfax_files:
                    try:
                        r2 = session.get(carfax_url, timeout=30)
                        r2.raise_for_status()

                        content_type = r2.headers.get("Content-Type", "").lower()
                        if "pdf" in content_type:
                            ext = ".pdf"
                        else:
                            # Might be an HTML viewer or landing page
                            ext = ".html"

                        safe_vin = "".join(
                            ch if ch.isalnum() or ch in ("-", "_") else "_"
                            for ch in vin
                        )
                        filename = f"{safe_vin}{ext}"

                        carfax_files[vin] = (filename, r2.content)
                        file_info = filename
                        status_label = "OK_DOWNLOADED"
                    except Exception as e2:
                        error = f"Carfax download error: {e2}"
                        status_label = "URL_ONLY"

                else:
                    status_label = "OK_URL_ONLY"

                results.append(
                    {
                        "VIN": vin,
                        "EBROCHURE_URL": ebrochure_url,
                        "CARFAX_URL": carfax_url,
                        "STATUS": status_label,
                        "ERROR_MESSAGE": error,
                        "FILE_NAME": file_info,
                    }
                )

                # Be a tiny bit polite to the servers
                time.sleep(0.3)

            except Exception as e:
                error = str(e)
                results.append(
                    {
                        "VIN": vin,
                        "EBROCHURE_URL": ebrochure_url,
                        "CARFAX_URL": None,
                        "STATUS": "ERROR",
                        "ERROR_MESSAGE": error,
                    }
                )

        progress.progress(1.0)
        status_text.text("Done!")

        results_df = pd.DataFrame(results)

        st.subheader("Scrape results")
        st.dataframe(results_df)

        # Merge CARFAX_URL back into original df (by VIN)
        merged = df.copy()
        # Build lookup by VIN
        carfax_lookup = (
            results_df[["VIN", "CARFAX_URL"]]
            .dropna(subset=["VIN"])
            .drop_duplicates(subset=["VIN"])
            .set_index("VIN")["CARFAX_URL"]
            .to_dict()
        )

        merged["CARFAX_URL"] = merged[vin_col].astype(str).map(carfax_lookup)

        # Download enriched CSV
        csv_buffer = io.StringIO()
        merged.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        st.download_button(
            label="⬇️ Download CSV with CARFAX_URL column",
            data=csv_bytes,
            file_name="listings_with_carfax.csv",
            mime="text/csv",
        )

        # ZIP of Carfax files, if we downloaded them
        if download_carfax_files and carfax_files:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for vin, (filename, content) in carfax_files.items():
                    zf.writestr(filename, content)
            zip_buffer.seek(0)

            st.download_button(
                label=f"⬇️ Download ZIP of {len(carfax_files)} Carfax files",
                data=zip_buffer,
                file_name="carfax_reports.zip",
                mime="application/zip",
            )
        elif download_carfax_files:
            st.warning("No Carfax files were downloaded; check the results table above.")
