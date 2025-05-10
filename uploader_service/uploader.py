from flask import Flask, request, jsonify
from google.cloud import storage
import json
import tempfile
import os

app = Flask(__name__)

@app.route("/", methods=["POST"])
def upload():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing JSON data"}), 400

        # Save JSON to temporary file
        with tempfile.NamedTemporaryFile("w+", suffix=".json", delete=False) as tmpfile:
            json.dump(data, tmpfile, ensure_ascii=False)
            tmpfile.flush()

            # Upload to GCS
            client = storage.Client()
            bucket = client.bucket("new-projects-datalake")
            blob = bucket.blob("projects/projects.json")
            blob.upload_from_filename(tmpfile.name)

        return jsonify({"status": "✅ Upload successful"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # ✅ Listen on port 8080 and accept external connections
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
