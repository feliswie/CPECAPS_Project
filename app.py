from flask import Flask, render_template, request, jsonify, send_file
from excel_handler import process_excel_file
from database import init_db, update_or_insert_data, get_all_data
from workbook_consolidator import run_workbook_pipeline
import sqlite3
import threading
from io import BytesIO

app = Flask(__name__)

# Initialize database on startup
init_db()


# ---------------------------------------------------------------------------
# Progress tracking helpers for consolidation pipeline
# ---------------------------------------------------------------------------

PHASE_LABELS = {
    '1': 'This takes approximately 2 minutes',
    '2': 'Merging repJourney data...',
    '3': 'Importing to main workbook...',
    '4': 'Make sure to double check for accuracy!',
}

progress_lock = threading.Lock()
progress_state = {}
result_payload = {'buffer': None, 'filename': None}


def _make_phase_state(label: str) -> dict:
    return {
        'status': 'pending',
        'message': label,
        'percent': 0,
        'processed_rows': 0,
        'total_rows': 0,
    }


def _reset_progress_state() -> None:
    with progress_lock:
        progress_state['phases'] = {key: _make_phase_state(label) for key, label in PHASE_LABELS.items()}
        progress_state['overall_status'] = 'idle'
        progress_state['download_ready'] = False
        progress_state['error'] = None
        result_payload['buffer'] = None
        result_payload['filename'] = None


def _snapshot_progress() -> dict:
    with progress_lock:
        phases = {key: dict(value) for key, value in progress_state.get('phases', {}).items()}
        return {
            'overall_status': progress_state.get('overall_status', 'idle'),
            'download_ready': progress_state.get('download_ready', False),
            'error': progress_state.get('error'),
            'phases': phases,
        }


def _update_phase_state(phase: int, **payload) -> None:
    phase_key = str(phase)
    with progress_lock:
        phases = progress_state.setdefault('phases', {})
        phase_state = phases.setdefault(phase_key, _make_phase_state(PHASE_LABELS.get(phase_key, f'Phase {phase_key}')))

        # Core fields
        if 'message' in payload and payload['message']:
            phase_state['message'] = payload['message']

        processed = payload.get('processed_rows')
        if processed is None and 'processed' in payload:
            processed = payload['processed']
        if processed is not None:
            phase_state['processed_rows'] = processed

        total = payload.get('total_rows')
        if total is None and 'total' in payload:
            total = payload['total']
        if total is not None:
            phase_state['total_rows'] = total

        percent = payload.get('percent')
        if percent is not None:
            phase_state['percent'] = percent
        elif phase_state['total_rows']:
            phase_state['percent'] = round((phase_state['processed_rows'] / phase_state['total_rows']) * 100)

        status = payload.get('status')
        if status:
            phase_state['status'] = status
            if status == 'running':
                progress_state['overall_status'] = 'running'
            elif status == 'error':
                progress_state['overall_status'] = 'error'
                progress_state['error'] = payload.get('message', 'An error occurred during processing.')
            elif status == 'done' and phase == 4:
                phase_state['percent'] = phase_state.get('percent') or 100


def _pipeline_progress_callback(phase, **payload):
    _update_phase_state(phase, **payload)


def _run_pipeline_async(dms_bytes: bytes, rep_bytes: bytes, main_bytes: bytes) -> None:
    try:
        buffer, filename = run_workbook_pipeline(dms_bytes, rep_bytes, main_bytes, _pipeline_progress_callback)
        with progress_lock:
            result_payload['buffer'] = buffer
            result_payload['filename'] = filename
            progress_state['download_ready'] = True
            if progress_state.get('overall_status') != 'error':
                progress_state['overall_status'] = 'completed'
            # Ensure phase 4 is marked done if callback didn't already
            phase_four = progress_state['phases'].setdefault('4', _make_phase_state(PHASE_LABELS['4']))
            phase_four.update({'status': 'done', 'percent': 100, 'message': PHASE_LABELS['4']})
    except Exception as exc:  # broad catch to capture any pipeline failure
        with progress_lock:
            progress_state['overall_status'] = 'error'
            progress_state['error'] = str(exc)
            phase_four = progress_state['phases'].setdefault('4', _make_phase_state(PHASE_LABELS['4']))
            phase_four.update({'status': 'error', 'message': str(exc)})
            result_payload['buffer'] = None
            result_payload['filename'] = None


# Initialize default progress state on startup
_reset_progress_state()

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
def process_pipeline():
    """Start the consolidation pipeline asynchronously."""
    dms_file = request.files.get('dms_file')
    rep_file = request.files.get('rep_file')
    main_file = request.files.get('main_file')

    if not dms_file or not rep_file or not main_file:
        return jsonify({'error': 'All three files (dms_file, rep_file, main_file) are required.'}), 400

    dms_bytes = dms_file.read()
    rep_bytes = rep_file.read()
    main_bytes = main_file.read()

    _reset_progress_state()
    with progress_lock:
        progress_state['overall_status'] = 'running'

    worker = threading.Thread(target=_run_pipeline_async, args=(dms_bytes, rep_bytes, main_bytes), daemon=True)
    worker.start()

    return jsonify({'message': 'Processing started.'}), 202


@app.route('/progress')
def progress():
    """Return the current progress snapshot."""
    return jsonify(_snapshot_progress())


@app.route('/download')
def download():
    """Stream the processed workbook once ready."""
    with progress_lock:
        ready = progress_state.get('download_ready', False)
        buffer = result_payload.get('buffer')
        filename = result_payload.get('filename') or 'Consolidated_MAIN.xlsx'

    if not ready or buffer is None:
        return jsonify({'error': 'Processed file is not ready yet.'}), 400

    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
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

@app.route('/dashboard_data')
def dashboard_data():
    """Return dashboard data including alerts."""
    from database import get_dashboard_alerts
    
    try:
        alerts = get_dashboard_alerts()
        df = get_all_data()
        
        return jsonify({
            "devices": df.to_dict(orient='records'),
            "alerts": alerts,
            "total": len(df),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)

