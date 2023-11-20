# app.py
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    data_path = request.form['dataPath']
    cleaning_rules = request.form['cleaningRules']
    uploaded_file = request.files['csvFile']

    # Save the uploaded CSV file to the server
    if uploaded_file:
        uploaded_file.save(f"uploads/{uploaded_file.filename}")

    # Process data_path, cleaning_rules, and uploaded_file as needed

    # Parse cleaning_rules to check if it's valid JSON
    try:
        cleaning_rules_json = json.loads(cleaning_rules)
        print('Generated Cleaning Rules:', cleaning_rules_json)
    except json.JSONDecodeError as e:
        print('Error decoding JSON:', str(e))
        return jsonify({'status': 'error', 'message': 'Invalid JSON in cleaning rules'})

    # Sample response
    response_data = {'status': 'success', 'message': 'Processing complete.'}
    print(jsonify(response_data))

if __name__ == '__main__':
    app.run(debug=True)
