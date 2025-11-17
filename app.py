import io
import time
import zipfile
import json
import pandas as pd
import requests
import streamlit as st
from urllib.parse import urlparse, parse_qs

API_ENDPOINT = "https://www.autonation.com/api/ebrochure?vid="

st.set_page_config(page_title="AutoNation Carfax Fetcher", layout="wide")
st.title("🚗 AutoNation Carfax Fetcher (API-powered, No 403 Errors)")

st.write("Upload a CSV containing AutoNation eBrochure links. This tool extracts the VID, "
         "calls the hidden AutoNation API, retrieves the Carfax URL, and optionally downloads the PDF.")

uploaded = st.file_uploader("Upload CSV", type=["csv"])

if uploaded:
    df = pd.read_csv(uploaded)

    st.subheader("CSV Preview")
    st.dataframe(df.head())

    # Guess likely eBrochure column
    def guess_col(patterns):
        for col in df.columns:
            lc = col.lower()
            if any(p in lc for p in patterns):
                return col
        return df.columns[0]

    ebrochure_col = st.selectbox(
        "Select column containing eBrochure URLs",
        df.columns,
        index=list(df.columns).index(guess_col(["ebrochure", "vlp", "brochure"]))
    )

    vin_col = st.selectbox(
        "Select VIN column (used for filenames)",
        df.columns,
        index=list(df.columns).index(guess_col(["vin"]))
    )

    download_files = st.checkbox("Download Carfax PDF/HTML files", value=True)

    go = st.button("Start")

    if go:
        results = []
        carfax_files = {}
        session = requests.Session()
        total = len(df)
        progress = st.progress(0)
        status = st.empty()

        for i, row in df.iterrows():
            progress.progress((i + 1) / total)

            e_url = str(row[ebrochure_col]).strip()
            vin = str(row[vin_col]).strip()

            status.text(f"[{i+1}/{total}] Processing VIN {vin}…")

            # Extract VID from URL
            try:
                parsed = urlparse(e_url)
                vid = parse_qs(parsed.query).get("VID", [None])[0]
            except:
                vid = None

            if not vid:
                results.append({
                    "VIN": vin,
                    "EBROCHURE_URL": e_url,
                    "VID": None,
                    "CARFAX_URL": None,
                    "STATUS": "NO_VID"
                })
                continue

            # Call hidden API endpoint
            api_url = API_ENDPOINT + vid
            carfax_url = None
            file_name = None
            status_label = ""

            try:
                resp = session.get(api_url, timeout=20)
                data = resp.json()
                carfax_url = data.get("carfaxUrl")

                if not carfax_url:
                    status_label = "NO_CARFAX_FOUND"
                else:
                    status_label = "FOUND_URL"

                    if download_files:
                        try:
                            r2 = session.get(carfax_url, timeout=30)

                            content_type = r2.headers.get("Content-Type", "").lower()
                            ext = ".pdf" if "pdf" in content_type else ".html"

                            file_name = f"{vin}{ext}"
                            carfax_files[file_name] = r2.content
                            status_label = "DOWNLOADED"

                        except Exception as e:
                            status_label = f"URL_ONLY ({e})"

            except Exception as e:
                status_label = f"API_ERROR: {e}"

            results.append({
                "VIN": vin,
                "EBROCHURE_URL": e_url,
                "VID": vid,
                "CARFAX_URL": carfax_url,
                "STATUS": status_label,
                "FILE_NAME": file_name
            })

            time.sleep(0.1)

        results_df = pd.DataFrame(results)

        st.subheader("Results")
        st.dataframe(results_df)

        # Attach CARFAX_URL to original CSV
        merged = df.copy()
        lut = results_df.set_index("VIN")["CARFAX_URL"].to_dict()
        merged["CARFAX_URL"] = merged[vin_col].astype(str).map(lut)

        csv_buf = io.StringIO()
        merged.to_csv(csv_buf, index=False)

        st.download_button(
            "⬇️ Download CSV with CARFAX_URL",
            csv_buf.getvalue().encode(),
            "listings_with_carfax.csv",
            "text/csv"
        )

        # ZIP of Carfax files
        if download_files and carfax_files:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zipf:
                for fname, content in carfax_files.items():
                    zipf.writestr(fname, content)

            zip_buf.seek(0)
            st.download_button(
                f"⬇️ Download ZIP of {len(carfax_files)} Carfax files",
                zip_buf,
                "carfax_reports.zip",
                "application/zip"
            )
