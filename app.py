from flask import Flask, render_template, request, jsonify, send_file
from workbook_consolidator import run_workbook_pipeline
from run_storage import (
    persist_run,
    load_latest_alerts,
    load_latest_devices,
    load_latest_stats,
    get_latest_result_path,
    load_latest_meta,
)
import threading
from io import BytesIO
from werkzeug.utils import secure_filename

app = Flask(__name__)


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
        persist_run(buffer, filename)
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

    if ready and buffer is not None:
        buffer.seek(0)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

    stored_path = get_latest_result_path()
    if stored_path:
        return send_file(
            stored_path,
            as_attachment=True,
            download_name=stored_path.name,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )

    return jsonify({'error': 'Processed file is not ready yet.'}), 400

@app.route('/dashboard')
def dashboard():
    """Dashboard placeholder page."""
    return render_template('pages/dashboard.html')

@app.route('/dashboard_data')
def dashboard_data():
    """Return the latest processed device rows, alerts, and stats."""
    devices = load_latest_devices()
    alerts = load_latest_alerts()
    stats = load_latest_stats()
    meta = load_latest_meta() or {}
    return jsonify({
        'devices': devices,
        'alerts': alerts,
        'stats': stats,
        'meta': meta,
    })


@app.route('/dashboard_upload', methods=['POST'])
def dashboard_upload():
    """Allow dashboard users to upload a consolidated MAIN workbook."""
    upload = request.files.get('main_file')
    if not upload or not upload.filename:
        return jsonify({'error': 'Please choose an .xls or .xlsx file to upload.'}), 400

    filename = secure_filename(upload.filename) or 'Manual_Upload.xlsx'
    buffer = BytesIO(upload.read())
    if buffer.getbuffer().nbytes == 0:
        return jsonify({'error': 'Uploaded file appears to be empty.'}), 400
    buffer.seek(0)

    try:
        meta = persist_run(buffer, filename)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:  # pragma: no cover - defensive guard
        return jsonify({'error': f'Failed to process workbook: {exc}'}), 500

    return jsonify({'message': 'Workbook uploaded successfully.', 'meta': meta})


if __name__ == '__main__':
    app.run(debug=True)

