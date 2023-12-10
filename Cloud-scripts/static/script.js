// static/script.js
let ruleCount = 0;

const sparkOperations = ['filter', 'withColumn', 'drop', 'groupBy', 'agg', 'orderBy', 'and more...'];

function addRule() {
    ruleCount++;

    const rulesContainer = document.getElementById('rulesContainer');

    const ruleDiv = document.createElement('div');
    ruleDiv.innerHTML = `
        <label for="columnName${ruleCount}">Column Name:</label>
        <input type="text" id="columnName${ruleCount}" name="columnName${ruleCount}" required>

        <label for="operation${ruleCount}">Operation:</label>
        <select id="operation${ruleCount}" name="operation${ruleCount}">
            ${sparkOperations.map(op => `<option value="${op}">${op}</option>`).join('')}
        </select>

        <label for="condition${ruleCount}">Condition:</label>
        <input type="text" id="condition${ruleCount}" name="condition${ruleCount}">

        <br>
    `;

    rulesContainer.appendChild(ruleDiv);
}

function deleteRule() {
    if (ruleCount > 0) {
        const rulesContainer = document.getElementById('rulesContainer');
        rulesContainer.removeChild(rulesContainer.lastChild);
        ruleCount--;
    }
}

function browseFile() {
    const fileInput = document.getElementById('csvFile');
    fileInput.click();
    fileInput.addEventListener('change', function () {
        const filePath = fileInput.value;
        document.getElementById('dataPath').value = filePath;
    });
}

function displayJSON(json) {
    const jsonContainer = document.getElementById('jsonContainer');
    jsonContainer.innerHTML = `<pre>${JSON.stringify(json, null, 2)}</pre>`;
}

document.getElementById('cleaningForm').addEventListener('submit', function (event) {
    event.preventDefault();

    const rules = [];

    for (let i = 1; i <= ruleCount; i++) {
        const columnName = document.getElementById(`columnName${i}`).value;
        const operation = document.getElementById(`operation${i}`).value;
        const condition = document.getElementById(`condition${i}`).value;

        const rule = {
            columnName: columnName,
            operation: operation,
            condition: condition
        };

        rules.push(rule);
    }

    const cleaningRulesJSON = JSON.stringify(rules, null, 2);
    console.log('Generated Cleaning Rules:', cleaningRulesJSON);

    // Display the generated JSON on the web page
    displayJSON(rules);

    // Add logic to send csvFile and cleaningRulesJSON to the backend (e.g., using fetch or AJAX)
    fetch('/process', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            csvFile: document.getElementById('csvFile').files[0],
            cleaningRules: rules,
        }),
    })
    .then(response => response.json())
    .then(data => {
        console.log('Server response:', data);

        // Check for errors from the server
        if (data.status === 'error') {
            alert('Error: ' + data.message);
        }
    })
    .catch(error => {
        console.error('Error:', error);
    });
});
