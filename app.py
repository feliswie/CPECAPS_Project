import copy
from datetime import datetime
from io import BytesIO
from threading import Lock, Thread

from flask import Flask, render_template, request, jsonify, send_file
from excel_handler import process_excel_file
from database import init_db, update_or_insert_data, get_all_data
import sqlite3
from workbook_consolidator import PipelineError, run_workbook_pipeline

app = Flask(__name__)

# Initialize database on startup
init_db()


PHASE_LABELS = {
    "1": "Phase 1: DMS normalization",
    "2": "Phase 2: repJourney merge",
    "3": "Phase 3: MAIN enrichment",
    "4": "Phase 4: Completed, ready for human review",
}

progress_lock = Lock()
pipeline_state = {}
processed_workbook_bytes: bytes | None = None


def _phase_template(label: str) -> dict:
    return {
        "label": label,
        "status": "pending",
        "percent": 0,
        "processed_rows": 0,
        "total_rows": 0,
        "message": "Waiting to begin.",
    }


def _default_phases() -> dict:
    return {key: _phase_template(label) for key, label in PHASE_LABELS.items()}


def _reset_pipeline_state():
    global processed_workbook_bytes
    pipeline_state.clear()
    pipeline_state.update(
        {
            "overall_status": "idle",
            "download_ready": False,
            "error": None,
            "started_at": None,
            "finished_at": None,
            "filename": None,
            "phases": _default_phases(),
        }
    )
    processed_workbook_bytes = None


def _update_progress(phase: int, **payload):
    phase_key = str(phase)
    with progress_lock:
        phase_state = pipeline_state.get("phases", {}).get(phase_key)
        if not phase_state:
            return
        status = payload.get("status")
        if status:
            phase_state["status"] = status
            if status == "running" and pipeline_state.get("overall_status") not in {"error", "completed"}:
                pipeline_state["overall_status"] = "running"
            if status == "error":
                pipeline_state["overall_status"] = "error"
        total = payload.get("total_rows") or payload.get("total")
        if total is not None:
            phase_state["total_rows"] = int(total)
        processed = payload.get("processed_rows") or payload.get("processed")
        if processed is not None:
            phase_state["processed_rows"] = int(processed)
        percent = payload.get("percent")
        total_for_percent = phase_state.get("total_rows") or total or 0
        if total_for_percent and processed is not None:
            fraction = min(processed, total_for_percent) / total_for_percent
            phase_state["percent"] = round(fraction * 100, 2)
        elif percent is not None:
            phase_state["percent"] = percent
        message = payload.get("message")
        if message:
            phase_state["message"] = message


def _handle_pipeline_failure(exc: Exception):
    message = str(exc)
    global processed_workbook_bytes
    with progress_lock:
        pipeline_state["overall_status"] = "error"
        pipeline_state["error"] = message
        pipeline_state["download_ready"] = False
        pipeline_state["finished_at"] = datetime.utcnow().isoformat() + "Z"
        processed_workbook_bytes = None
        if isinstance(exc, PipelineError) and exc.phase:
            phase_key = str(exc.phase)
            if phase_key in pipeline_state["phases"]:
                pipeline_state["phases"][phase_key]["status"] = "error"
                pipeline_state["phases"][phase_key]["message"] = message


_reset_pipeline_state()

@app.route('/')
def index():
    """Homepage"""
    return render_template('pages/index.html')

@app.route('/upload', methods=['POST'])
def upload():
    """Handle Excel file upload."""
    file = request.files.get('file')
    if file is None:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        # Process the Excel file using our handler
        df = process_excel_file(file)
        
        # Update database (update existing or insert new records)
        inserted, updated = update_or_insert_data(df)
        
        return jsonify({"message": "Upload successful", "inserted": inserted, "updated": updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/process', methods=['POST'])
def start_processing():
    """Start the four-phase workbook pipeline in a background thread."""

    required_fields = {
        'dms_file': request.files.get('dms_file'),
        'rep_file': request.files.get('rep_file'),
        'main_file': request.files.get('main_file'),
    }
    missing = [key for key, file in required_fields.items() if file is None]
    if missing:
        return jsonify({"error": f"Missing uploads: {', '.join(missing)}"}), 400

    file_bytes = {}
    for key, storage in required_fields.items():
        content = storage.read()
        if not content:
            return jsonify({"error": f"Upload '{key}' is empty."}), 400
        file_bytes[key] = content

    with progress_lock:
        if pipeline_state.get("overall_status") == "running":
            return jsonify({"error": "A processing job is already running."}), 409
        _reset_pipeline_state()
        pipeline_state["overall_status"] = "running"
        pipeline_state["started_at"] = datetime.utcnow().isoformat() + "Z"

    def worker():
        global processed_workbook_bytes
        try:
            output_buffer, filename = run_workbook_pipeline(
                file_bytes['dms_file'],
                file_bytes['rep_file'],
                file_bytes['main_file'],
                progress_callback=_update_progress,
            )
            with progress_lock:
                processed_workbook_bytes = output_buffer.getvalue()
                pipeline_state["download_ready"] = True
                pipeline_state["overall_status"] = "completed"
                pipeline_state["finished_at"] = datetime.utcnow().isoformat() + "Z"
                pipeline_state["filename"] = filename
                phase_four = pipeline_state["phases"].get("4")
                if phase_four:
                    phase_four.update(
                        {
                            "status": "done",
                            "percent": 100,
                            "processed_rows": 1,
                            "total_rows": 1,
                            "message": PHASE_LABELS["4"],
                        }
                    )
        except Exception as exc:
            _handle_pipeline_failure(exc)

    Thread(target=worker, daemon=True).start()
    return jsonify({"message": "Processing started"})


@app.route('/progress')
def get_progress():
    with progress_lock:
        snapshot = copy.deepcopy(pipeline_state)
    return jsonify(snapshot)


@app.route('/download')
def download_processed_workbook():
    with progress_lock:
        if not pipeline_state.get("download_ready") or not processed_workbook_bytes:
            return jsonify({"error": "No processed workbook is ready yet."}), 400
        filename = pipeline_state.get("filename") or "Consolidated_MAIN.xlsx"
        payload = processed_workbook_bytes
    buffer = BytesIO(payload)
    buffer.seek(0)
    return send_file(
        buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename,
    )

@app.route('/raw_data')
def raw_data():
    """Display raw data page."""
    return render_template('pages/raw_data.html')

@app.route('/dashboard')
def dashboard():
    """Dashboard placeholder page."""
    return render_template('pages/dashboard.html')

@app.route('/alerts')
def alerts():
    """Alerts placeholder page; supply minimal context to render the template."""
    return render_template('pages/alerts.html', alerts={"soft": [], "urgent": []}, devices=[])

@app.route('/data')
def data():
    """Return all data for the raw data table."""
    df = get_all_data()
    return jsonify(df.to_dict(orient='records'))

@app.route('/clear_database', methods=['POST'])
def clear_database():
    """Clear all data from the database."""
    try:
        conn = sqlite3.connect('data.db')
        conn.execute('DELETE FROM telemetry')
        conn.commit()
        conn.close()
        return jsonify({"message": "Database cleared successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/update_database', methods=['POST'])
def update_database():
    """Update database entries with missing data."""
    try:
        df = get_all_data()
        # Here you would implement logic to update missing data
        # For demonstration, we will just re-insert the same data
        inserted, updated = update_or_insert_data(df)
        return jsonify({"message": "Database updated successfully", "inserted": inserted, "updated": updated})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

