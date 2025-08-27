import streamlit as st
import pandas as pd
import numpy as np
import io
import requests

st.set_page_config(page_title="CSV Upload & Validation Tool", layout="wide")

# Expected columns (cleaned: lowercase, no spaces)
EXPECTED_COLUMNS = {
    "date": "Date",
    "name": "Name",
    "client id": "Client ID",
    "deposit": "Deposit",
}

def clean_columns(cols):
    return [c.strip().lower() for c in cols]

def map_columns(df, mapping_dict):
    df = df.rename(columns=mapping_dict)
    return df

def highlight_duplicates(df, duplicate_index_set):
    return df.style.apply(
        lambda x: ['background-color: #FFCCCC' if x.name in duplicate_index_set else '' for _ in x], axis=1
    )

st.title("CSV/Excel Upload")

uploaded_file = st.file_uploader("Upload your CSV or Excel file", type=["csv", "xlsx"])
if uploaded_file:
    try:
        if uploaded_file.name.endswith('.csv'):
            df_uploaded = pd.read_csv(uploaded_file)
        else:
            df_uploaded = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error reading uploaded file: {e}")
        st.stop()

    original_cols = list(df_uploaded.columns)
    cleaned_cols = clean_columns(original_cols)
    st.write("### Uploaded Columns (cleaned)")
    st.write(cleaned_cols)

    missing_required = [col for col in EXPECTED_COLUMNS.keys() if col not in cleaned_cols]
    if missing_required:
        st.warning("Some required columns are not detected or mismatched. Please map them below.")
        user_mappings = {}
        for req_col in missing_required:
            st.markdown(f"**Map required column '{EXPECTED_COLUMNS[req_col]}'**")
            selected_col = st.selectbox(
                f"Select column to map to '{EXPECTED_COLUMNS[req_col]}'",
                options=cleaned_cols,
                key=req_col
            )
            user_mappings[selected_col] = EXPECTED_COLUMNS[req_col]
        if st.button("Confirm column mapping"):
            rename_map = {orig: new for orig, new in zip(original_cols, cleaned_cols)}
            rename_map.update(user_mappings)
            df_uploaded = map_columns(df_uploaded, rename_map)
            st.success("Columns mapped successfully.")
            st.experimental_rerun()
        st.stop()
    else:
        rename_map = {}
        for orig_col, cleaned_col in zip(original_cols, cleaned_cols):
            if cleaned_col in EXPECTED_COLUMNS:
                rename_map[orig_col] = EXPECTED_COLUMNS[cleaned_col]
        df_uploaded.rename(columns=rename_map, inplace=True)

    st.write("### Uploaded Data Preview")
    st.dataframe(df_uploaded.head())

    progress_bar = st.progress(0)
    status_text = st.empty()

    progress_bar.progress(10)
    status_text.text("Cleaning duplicate entries...")
    df_uploaded['Date'] = pd.to_datetime(df_uploaded['Date']).dt.date
    subset_cols = ['Client ID', 'Date']
    dup_mask = df_uploaded.duplicated(subset=subset_cols, keep=False)
    duplicates = df_uploaded[dup_mask].copy()
    duplicates['DuplicateType'] = np.where(
        duplicates.duplicated(subset=subset_cols, keep='first'),
        'Dropped Duplicate',
        'Original'
    )
    uploaded_shape = df_uploaded.shape
    num_duplicates = duplicates.shape[0] // 2  # duplicates are pairs

    progress_bar.progress(30)
    status_text.text("Loading internal Excel data for validation...")

    # Load internal Excel from Google Sheets export link
    try:
        FILE_ID = "1zJULAZyrMx87ZVJ0ErtqFnc07Rvm-WSd"
        download_url = f"https://docs.google.com/spreadsheets/d/{FILE_ID}/export?format=xlsx"
        response = requests.get(download_url)
        response.raise_for_status()
        file_bytes = io.BytesIO(response.content)
        cdb = pd.read_excel(file_bytes)
        cdb['Date'] = pd.to_datetime(cdb['Date']).dt.date
    except Exception as e:
        st.error(f"Failed to load internal Excel for validation: {e}")
        st.stop()

    progress_bar.progress(50)
    status_text.text("Running validation calculations...")

    merged = pd.merge(
        df_uploaded,
        cdb,
        how='left',
        left_on=['Date', 'Client ID'],
        right_on=['Date', 'accountId'],
        suffixes=('', '_cdb')
    )

    def find_previous_info(row, cdb):
        acc = row['Client ID']
        current_date = pd.to_datetime(row['Date'])
        prev_records = cdb[(cdb['accountId'] == acc) & (pd.to_datetime(cdb['Date']) < current_date)]
        if prev_records.empty:
            return pd.Series({
                'prev_last_activity': '',
                'prev_activity_set': '',
                'remark': 'No previous date found'
            })
        else:
            closest = prev_records.loc[prev_records['Date'].idxmax()]
            return pd.Series({
                'prev_last_activity': closest['last_activity'],
                'prev_activity_set': closest['activity_set'],
                'remark': ''
            })

    prev_info = merged.apply(find_previous_info, axis=1, cdb=cdb)
    merged = pd.concat([merged, prev_info], axis=1)

    def check_validity(row):
        if pd.isna(row['last_activity']) or row['remark'] == 'No previous date found':
            return row['remark']
        prov_date = pd.to_datetime(row['Date']).date()
        last_act_date = pd.to_datetime(row['prev_last_activity']).date()
        delta = (prov_date - last_act_date).days
        if delta >= 7:
            return 'valid'
        elif delta >= 0:
            return 'invalid'
        else:
            return 'invalid'

    merged['status'] = merged.apply(check_validity, axis=1)
    merged.loc[merged['deposit_amount'].isna(), ['status', 'remark']] = ['No entry found for this trx', '']

    final_df = merged[
        ['Name', 'Date', 'Client ID', 'Deposit', 'deposit_amount', 'deposit_distribution', 'prev_last_activity', 'prev_activity_set', 'remark', 'status']
    ].rename(columns={
        'Name': 'CRE',
        'Deposit': '1st_deposit_provided',
        'deposit_amount': 'overall_deposit_amount',
        'prev_last_activity': 'previous_activity'
    })

    final_df['remaining_deposit'] = final_df['overall_deposit_amount'] - final_df['1st_deposit_provided']

    output = final_df[
        ['CRE', 'Date', 'Client ID', '1st_deposit_provided', 'remaining_deposit', 'overall_deposit_amount',
         'previous_activity', 'remark', 'status']
    ].copy()

    progress_bar.progress(90)
    status_text.text("Analysis complete.")
    st.write(f"### Uploaded Data Shape: {uploaded_shape}")
    st.write(f"### Number of duplicate pairs found (original + duplicate): {num_duplicates}")

    if num_duplicates > 0:
        show_dups = st.button("Show Duplicate Entries")
        if show_dups:
            styled_dups = highlight_duplicates(duplicates, duplicate_index_set=set(duplicates.index))
            st.write("Original and duplicate rows highlighted (duplicates in red):")
            st.dataframe(styled_dups)

    st.write("### Output Preview")
    st.dataframe(output.head())

    st.write("### Distribution by Status")
    st.table(output['status'].value_counts())

    progress_bar.progress(100)
    status_text.text("")

    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')

    csv_data = convert_df_to_csv(output)

    st.download_button(
        label="Download Processed Data as CSV",
        data=csv_data,
        file_name="processed_data.csv",
        mime="text/csv"
    )

