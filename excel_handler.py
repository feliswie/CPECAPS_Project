import pandas as pd

def process_excel_file(file):
    """
    Process Excel file with fixed column structure:
    - Column 1: Disregard
    - Column 2: Device_ID
    - Column 3: Last_Sighted_Date
    - Column 4: Last_Sighted_Location
    - Column 5: Location_Code
    """
    # Read Excel file without headers
    raw = pd.read_excel(file, header=None)
    
    # Validate minimum columns
    if raw.shape[1] < 5:
        raise ValueError("File must have at least 5 columns")

    # Check if first row contains exact column names
    first_row = raw.iloc[0].astype(str).str.strip()
    expected_headers = ['Device_ID', 'Last_Sighted_Date', 'Last_Sighted_Location', 'Location_Code']
    
    # If first row matches our expected headers, skip it
    if all(header in first_row.values for header in expected_headers):
        raw = raw.iloc[1:].reset_index(drop=True)

    # Create DataFrame with specific columns (skip first column)
    # Convert dates with error handling
    dates = pd.to_datetime(raw.iloc[:, 2], errors='coerce')
    if dates.isna().any():
        raise ValueError("Some dates in column 3 are not in a valid format. Please ensure all dates are valid.")
        
    df = pd.DataFrame({
        'Device_ID': raw.iloc[:, 1].astype(str).str.strip(),
        'Last_Sighted_Date': dates.dt.strftime('%Y-%m-%d %H:%M:%S'),
        'Last_Sighted_Location': raw.iloc[:, 3].astype(str).str.strip(),
        'Location_Code': raw.iloc[:, 4].astype(str).str.strip()
    })
    
    return df