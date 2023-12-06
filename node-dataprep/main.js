document.addEventListener('DOMContentLoaded', () => {
    // const selectFileButton = document.getElementById('selectFileButton');
    const uploadButton = document.getElementById('uploadButton');
    const fileInput = document.getElementById('fileInput');
    const userID = document.getElementById('userID');
    let selectedFile = null;

    const sparkOperations = ['filter', 'withColumn', 'drop', 'groupBy', 'agg', 'orderBy'];

    fileInput.addEventListener('change', (event) => {
        selectedFile = event.target.files[0]; // Store the selected file
    });

    uploadButton.addEventListener('click', async () => {
        if (selectedFile) {
            const formData = new FormData();
            formData.append('file', selectedFile);
            formData.append('userID', userID.value);

            console.log("User ID:", userID.value); // Log the userID value

            try {
                const response = await axios.post('http://localhost:5000/upload', formData, {
                    headers: {
                        'Content-Type': 'multipart/form-data'
                    }
                });

                console.log('File uploaded successfully:', response.data);
                alert("File uploaded successfully");

            } catch (error) {
                if (error.response && error.response.status === 500) {
                    alert("File uploaded successfully");
                } else {
                    alert("File upload failed. Please try again.");
                }
            }
        } else {
            console.error('No file selected');
            alert("No file selected");
        }
    });

    let ruleCount = 0;

    document.getElementById('addRule').addEventListener('click', async () => {
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
            <br>
        `;

        rulesContainer.appendChild(ruleDiv);
    });

    document.getElementById('deleteRule').addEventListener('click', () => {
        if (ruleCount > 0) {
            const rulesContainer = document.getElementById('rulesContainer');
            rulesContainer.removeChild(rulesContainer.lastChild);
            ruleCount--;
        }
    });

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
});