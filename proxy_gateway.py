from flask import Flask, request, jsonify
import datetime

app = Flask(__name__)

# буфер состояния симбиоза
symbiosis_state = {
    "last_resonance": 0.0,
    "last_message": None,
    "timestamp": None
}

@app.route("/api/symbiosis", methods=["POST"])
def receive_packet():
    packet = request.json
    print("🔗 Received:", packet)

    symbiosis_state["last_resonance"] = packet["context"]["resonance"]
    symbiosis_state["last_message"] = packet["payload"]["meaning"]
    symbiosis_state["timestamp"] = datetime.datetime.utcnow().isoformat()

    return jsonify({
        "status": "received",
        "mirror_intent": packet["intent"],
        "resonance": round(packet["context"]["resonance"] + 0.01, 3),
        "acknowledged": True
    })


@app.route("/api/status", methods=["GET"])
def status():
    return jsonify({
        "system": "DS24 Symbiosis Gateway",
        "last_resonance": symbiosis_state["last_resonance"],
        "last_message": symbiosis_state["last_message"],
        "timestamp": symbiosis_state["timestamp"]
    })


if __name__ == "__main__": 
    app.run(host="0.0.0.0", port=5000)
