# app.py
import json
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    cleaning_rules = json.loads(request.form['cleaningRules'])  # Parse JSON
    uploaded_file = request.files['csvFile']

    # Save the uploaded CSV file to the server
    if uploaded_file:
        uploaded_file.save(f"uploads/{uploaded_file.filename}")

    # Process cleaning_rules and uploaded_file as needed

    # Save the generated JSON to a file in the root directory
    with open('generated_rules.json', 'w') as json_file:
        json.dump(cleaning_rules, json_file, indent=2)

    # Sample response
    response_data = {'status': 'success', 'message': 'Processing complete.'}
    return jsonify(response_data)

if __name__ == '__main__':
    app.run(debug=True)
