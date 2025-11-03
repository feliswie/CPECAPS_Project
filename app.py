from flask import Flask, render_template, request, jsonify
from excel_handler import process_excel_file
from database import init_db, update_or_insert_data, get_all_data
import sqlite3

app = Flask(__name__)

# Initialize database on startup
init_db()

@app.route('/')
def index():
    """Simple homepage with link to raw data view."""
    return render_template('index.html')

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

@app.route('/raw_data')
def raw_data():
    """Display raw data page."""
    return render_template('raw_data.html')

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

if __name__ == '__main__':
    app.run(debug=True)

