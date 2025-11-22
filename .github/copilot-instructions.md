You are helping me repurpose an existing web app in this repo.

Goal
Build a simple web UI that lets me upload three Excel workbooks (DMS Dump, repJourney dump, MAIN Inventory), then run a four phase processing pipeline that updates the MAIN workbook and returns a consolidated XLSX for download. The app must work by column names, not fixed column letters, and it must show per phase progress on the front end.

Use the existing tech stack and project structure in this repo. Do not set up a new framework. Refactor current code where needed.

High level UX
- Single page at index.html.
- Three upload cards arranged like a flow: DMS Dump, repJourney Dump, then Main Inventory.
- A “Process & Generate File” button that starts the backend pipeline once all three files are provided.
- A final card that says “Your consolidated file is ready” with a Download button that fetches the processed XLSX.
- Each phase (1 to 4) has its own small progress bar and status text.

Front end requirements
1. Update index.html:
   - Add three upload boxes, visually similar:
     - “Upload DMS Dump” with an underlying `<input type="file" accept=".xls,.xlsx">`.
     - “Upload repJourney Dump” with similar input.
     - “Upload Main Inventory” with similar input.
   - Show filename and a small status label under each box once a file is chosen, for example “File loaded” or “Waiting for file”.
   - Add a main button under the flow, label it “Process & Generate File”.
   - Add a results card on the right side of the flow:
     - Text “Your consolidated file is ready” plus a tick icon placeholder.
     - A “Download” button that is disabled until backend processing finishes.

2. Progress display:
   - Somewhere below or beside the flow, render a section “Processing status”.
   - Inside it render four entries:
     - “Phase 1: DMS normalization”
     - “Phase 2: repJourney merge”
     - “Phase 3: MAIN enrichment”
     - “Phase 4: Completed, ready for human review”
   - Each entry has:
     - A text label like “Phase 3: Updating MAIN last disarmed fields – 60% (3,548 / 5,913 rows)”.
     - A progress bar `<div>` whose width reflects percentage.
   - The front end polls a `/progress` endpoint periodically during processing, for example every 500 to 1000 ms, and updates each phase’s text and bar.

3. Front end behaviour:
   - “Process & Generate File” is disabled until all three file inputs have files.
   - When the user clicks the button:
     - Send all three files to a single backend endpoint using `FormData` and `fetch` (or the framework’s equivalent).
     - Disable the button and all file inputs while processing.
     - Start polling `/progress` until the backend reports that processing is finished.
   - When processing is complete:
     - Enable the Download button.
     - Optionally show a short success message.
   - Download button calls a `/download` endpoint that returns the processed XLSX as a file download.

Backend requirements
Use the existing backend stack (for example Node, Python, or similar) already present in this repo.

Files and sheets:
- Uploaded files:
  - DMS Dump workbook, typically with a sheet that includes a column header `Device_ID`.
  - repJourney workbook, which contains journey rows and a column header `Begin Journey Date`.
  - MAIN workbook, with at least:
    - A sheet named `DMS Dump` where normalized DMS data should live.
    - A month sheet whose name follows `MMMYYYY` (for example `Jul2025`, `jun2025`, `SEP2025`), containing the repJourney data plus extra formula columns.
    - A sheet (often `MAIN`) that has `Device Nos`, `Last Disarmed Date`, and `Last Disarmed Area` columns.

Work by header names
Never hard code Excel columns by letter like “AP” or “BK”. Always:
- Read the header row.
- Locate the index of the column whose header text matches what we need, for example `Device_ID`, `Begin Journey Date`, `Destination`, `Disarm Date`, `Last Disarmed Date`, `Last Disarmed Area`, `Device Nos`.
- Use those indices throughout.

Phase 1 – DMS normalization
Input: uploaded DMS Dump workbook, and the MAIN workbook.

Logic:
1. Parse the DMS workbook.
   - Use the first sheet in that file or a sheet named “DMS Dump” if present.
   - Identify the column whose header text equals `Device_ID`.
2. For each row with a value in `Device_ID`:
   - Convert Device_ID to a string.
   - Strip leading zeros, leaving at least one digit, for example `0000011592` becomes `11592`.
3. Open the MAIN workbook in memory.
4. Find or create the sheet named `DMS Dump` in MAIN.
   - Keep the header row.
   - Delete all existing data rows beneath the header.
   - Write all normalized DMS rows from the uploaded DMS file into this sheet.
5. While iterating rows, update a shared progress structure for Phase 1:
   - total_rows = number of DMS rows.
   - processed_rows = current row index.
   - percent = processed_rows / total_rows * 100.

Phase 2 – repJourney merge into the month sheet
Input: uploaded repJourney workbook, and MAIN workbook (already modified in Phase 1).

Logic:
1. Parse the repJourney workbook.
   - Use the first sheet unless there is a strong reason to use another.
   - Find the column with header `Begin Journey Date`.
2. Sort all data rows by `Begin Journey Date` in descending order.
3. In the MAIN workbook:
   - Detect the month sheet name dynamically.
     - Find a sheet whose name matches an `MMMYYYY` pattern, for example `Jul2025`, `jun2025`, `SEP2025`.
     - If there are multiple, either pick the one that matches the month present in repJourney, or for now pick the first.
   - In that month sheet:
     - Identify all headers from the repJourney file that also exist in this sheet.
     - Identify the first formula column by header name `Destination`. All columns from `Destination` onward are considered formula driven and must be preserved or copied down.
4. Replace and append behaviour:
   - We assume the existing rows in the month sheet correspond to the older repJourney data.
   - We can clear and rewrite the “raw data” portion:
     - For columns that exist in repJourney, overwrite the values in the month sheet from row 2 downward with the sorted repJourney data.
   - For rows that exceed the original row count in MAIN (true new entries):
     - Append new rows at the bottom of the month sheet.
5. Formula propagation:
   - For all rows that exist in the original month sheet, formula columns starting at `Destination` are already present and should remain aligned.
   - For any appended new rows:
     - For each formula column, copy the formula from the last existing data row above and paste it into the new rows.
     - Do not paste values, keep them as formulas so they compute correctly once the workbook is opened.
6. Update Phase 2 progress:
   - As you iterate through repJourney rows while writing them into the month sheet, update total and processed counts.

Phase 3 – MAIN enrichment without Excel VLOOKUP
This is the phase that used to be slow in manual Excel because of many VLOOKUP formulas.

Goal
Populate `Last Disarmed Date` and `Last Disarmed Area` in MAIN directly in code, using the latest journey row per device, then write values only. We do not recalc via Excel formulas.

Logic:
1. From the month sheet in MAIN (the one updated in Phase 2):
   - Identify the header `Disarm Date`.
   - Identify the header `Destination`.
   - Identify the device identifier column used for linking, for example `IVM/iScout Device ID` or whatever device id column is consistent with the existing formulas.
   - Optionally identify `Begin Journey Date` if we need to select the latest row per device. We expect the sheet to be sorted by this field in descending order from Phase 2.
2. Build a lookup map in memory keyed by device id:
   - Iterate rows from top to bottom.
   - Because the sheet is sorted by `Begin Journey Date` descending, the first time we see a device id we treat that as the latest journey row.
   - For each device id, store:
     - lastDisarmedDate = value from `Disarm Date`.
     - lastDisarmedArea = value from `Destination`.
3. In the `MAIN` sheet:
   - Identify the columns `Device Nos`, `Last Disarmed Date`, and `Last Disarmed Area` by header names.
   - Iterate through each data row that has a Device Nos value.
   - Look up that device id in the map built from the month sheet.
     - If found, set the `Last Disarmed Date` cell to the mapped date.
     - Set the `Last Disarmed Area` cell to the mapped destination.
     - Write these as plain values, not formulas.
     - If not found, leave the cells as they are or blank, based on current design.
4. While processing MAIN rows:
   - Update Phase 3 progress frequently.
   - Example: `Phase 3: Updating MAIN last disarmed fields – 87% (5,153 / 5,913 rows)`.
5. At the end of Phase 3:
   - The `MAIN` sheet should have `Last Disarmed Date` and `Last Disarmed Area` fully populated as values.
   - No VLOOKUPs are required.

Phase 4 – completion only
There is no extra computation in this phase.

Backend behaviour
1. Processing endpoint:
   - Define an endpoint like `POST /process` that accepts three file uploads in a multipart form.
   - Store the uploaded files in memory or a temp directory.
   - Run Phases 1, 2, and 3 in sequence on a background task or synchronous handler.
   - Maintain a shared progress object in memory keyed by a session id or similar:
     - For each phase, store: status (pending, running, done, error), totalRows, processedRows, percentage, and a human readable message.
   - Update this progress object inside the loops for DMS rows, repJourney rows, and MAIN rows.
   - At the end, store the final processed MAIN workbook for download, for example as a single consolidated XLSX file in a temp location or in memory.

2. Progress endpoint:
   - Define `GET /progress` that returns the current progress object in JSON.
   - The front end will poll this endpoint while `POST /process` is running.
   - Shape the JSON so each phase has something like:
     - `{ phase: 3, label: "Phase 3: Updating MAIN last disarmed fields", processed: 5913, total: 5913, percent: 100 }`.

3. Download endpoint:
   - Define `GET /download` that streams the final processed workbook with the correct `Content-Type` and `Content-Disposition` headers so the browser downloads a file.
   - Disable this endpoint or return a clear error if processing is not yet complete.

Error handling and validation
- If any required column is missing in an uploaded file:
  - Abort processing.
  - Set the progress object to an error state for the relevant phase, including a useful message like: “Phase 2 error: column ‘Begin Journey Date’ not found in repJourney file”.
  - The front end should show that message in the Phase section and keep the Download button disabled.
- If the month sheet cannot be detected in MAIN:
  - Fail early with a clear message that no MMMYYYY sheet was found.

Implementation notes
- Use a reliable Excel library that can read and write .xlsx with styles and formulas preserved.
- Be careful to preserve styles and formulas in non touched fields when rewriting sheets.
- For performance, load each workbook only once per processing call and avoid unnecessary recalculation.
- Write code in small, testable helper functions for:
  - Loading headers and mapping them to indices.
  - Normalizing the DMS Device_ID column.
  - Sorting repJourney rows by Begin Journey Date.
  - Merging repJourney into the month sheet and copying formulas down.
  - Building the device to last disarmed map.
  - Updating the MAIN sheet’s last disarmed columns.

Please inspect the existing code in this repo, reuse its patterns for routing and static file serving, then implement this specification end to end.
