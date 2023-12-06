// static/script.js
let ruleCount = 0;

const sparkOperations = ['filter', 'withColumn', 'drop', 'groupBy', 'agg', 'orderBy'];

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

function displayJSON(json) {
    const jsonContainer = document.getElementById('jsonContainer');
    jsonContainer.innerHTML = `<pre>${JSON.stringify(json, null, 2)}</pre>`;
}

document.getElementById('cleaningForm').addEventListener('submit', async function (event) {
    event.preventDefault();

    const rules = [];

    for (let i = 1; i <= ruleCount; i++) {
        const columnName = document.getElementById(`columnName${i}`).value;
        const operation = document.getElementById(`operation${i}`).value;

        const rule = {
            columnName: columnName,
            operation: operation,
        };

        rules.push(rule);
    }

    const cleaningRulesJSON = JSON.stringify(rules, null, 2);
    console.log('Generated Cleaning Rules:', cleaningRulesJSON);

    // Display the generated JSON on the web page
    displayJSON(rules);

    try {
        const apiUrl = 'http://34.174.88.226:8000/projects/jobsapi/';

        const response = await axios.post(apiUrl, rules);

        console.log('Response:', response.data.download_url);
    } catch (error) {
        console.error('Error:', error);

        if (error.response && error.response.status === 500) {
            console.log('No response');
        } else {
            console.log('Error occurred');
        }
    }
});