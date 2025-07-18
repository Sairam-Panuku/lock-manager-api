from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)

# In-memory lock store
lock_store = {}

# Default time-to-live in seconds (5 minutes)
DEFAULT_TTL = 300

# Helper to check if a lock is expired
def is_expired(acquired_at, timeout):
    acquired_dt = datetime.strptime(acquired_at, "%Y-%m-%d %H:%M:%S")
    return datetime.now() > acquired_dt + timedelta(seconds=timeout)

# Endpoint: Create a new lock
@app.route('/lock', methods=['POST'])
def create_lock():
    data = request.get_json()
    resource = data.get('resource')
    owner = data.get('owner')
    ttl = data.get('ttl_seconds', DEFAULT_TTL)

    if not resource or not owner:
        return jsonify({"error": "Missing 'resource' or 'owner'"}), 400

    # Check if lock already exists and is not expired
    if resource in lock_store:
        lock_info = lock_store[resource]
        if not is_expired(lock_info["acquired_at"], lock_info["timeout"]):
            return jsonify({"error": "Resource already locked"}), 409
        else:
            # Remove expired lock and continue
            del lock_store[resource]

    lock_store[resource] = {
        "owner": owner,
        "acquired_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeout": ttl
    }

    return jsonify({
        "message": f"Lock acquired on {resource}",
        "ttl_seconds": ttl
    }), 201

# Endpoint: Get all active (non-expired) locks
@app.route('/locks', methods=['GET'])
def get_locks():
    response = []
    expired_keys = []

    for resource, info in lock_store.items():
        if is_expired(info["acquired_at"], info["timeout"]):
            expired_keys.append(resource)
            continue

        response.append({
            "resource": resource,
            "owner": info["owner"],
            "acquired_at": info["acquired_at"],
            "expires_in": info["timeout"]
        })

    # Cleanup expired locks
    for key in expired_keys:
        del lock_store[key]

    return jsonify(response), 200

# Endpoint: Get all active locks by process ID
@app.route('/lock/process/<string:process_id>', methods=['GET'])
def get_locks_by_process(process_id):
    result = []
    expired_keys = []

    for resource, info in lock_store.items():
        if is_expired(info["acquired_at"], info["timeout"]):
            expired_keys.append(resource)
            continue

        if info["owner"] == process_id:
            result.append({
                "resource": resource,
                "acquired_at": info["acquired_at"],
                "expires_in": info["timeout"]
            })

    # Cleanup expired locks
    for key in expired_keys:
        del lock_store[key]

    return jsonify(result), 200

if __name__ == '__main__':
    app.run(debug=True)
