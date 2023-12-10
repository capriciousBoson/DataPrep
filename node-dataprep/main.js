document.addEventListener('DOMContentLoaded', () => {

    const uploadButton = document.getElementById('uploadButton');
    const fileInput = document.getElementById('fileInput');
    const userID = document.getElementById('userID');


    let selectedFile = null; // contains our complete file
    let jsonSelectedFile = null; // contains the json file
    let filename = null; // contains the filename without extension
    const header = ["", "auto"]; // contains the header of the csv file

    // prod IP
    // whenever IP changed, update both prod_ip and backend 
    const prod_ip = 'http://34.174.108.150:5000';
    const test_ip = 'http://localhost:5000';
    const backend = 'http://34.174.108.150:8000'


    const sparkOperations = ['', 'auto', 'filter', 'withColumn', 'drop', 'groupBy', 'agg', 'orderBy', 'mean_normalization', 'categorial_encoding', 'fillna', 'cast'];

    fileInput.addEventListener('change', (event) => {

        selectedFile = event.target.files[0]; // Store the selected file
        console.log(fileInput);

        // Read the header when a file is selected
        // if (selectedFile) {

        if (selectedFile.name.endsWith('.csv')) {
            // Valid CSV file, you can proceed with further processing
            // alert('File is a valid CSV file. You can proceed with processing.');
            // Here, you can add additional code to handle the file, such as uploading it to a server or processing its content.
            const reader = new FileReader();

            reader.onload = function (e) {
                const content = e.target.result;

                // Assuming the header is in the first line of the CSV
                const firstLine = content.split('\n')[0];
                header.splice(2, header.length, ...firstLine.split(','));

                // Log or use the header array as needed
                console.log('CSV Header:', header);
            };
            reader.readAsText(selectedFile);
            const lastDot = selectedFile.name.lastIndexOf('.');
            filename = selectedFile.name.substring(0, lastDot);
            // console.log("filename");

        } else {
            // Invalid file type
            alert('Please select a valid CSV file.');
            fileInput.value = '';
            selectedFile = null;
        }
        // }

        console.log("hello this error", selectedFile, filename, selectedFile.name);
    });


    uploadButton.addEventListener('click', async () => {
        showLoader();

        if (selectedFile) {
            const formData = new FormData();
            formData.append('file', selectedFile);
            formData.append('userID', userID.value);

            console.log("User ID:", userID.value); // Log the userID value
            // tried using a localhost but dfacing issues with cors current stop gap fix is updating ip of the server here
            try {
                // hardcoded, need to update when we restart instance?!
                const response = await axios.post(`${prod_ip}/upload`, formData, {
                    headers: {
                        'Content-Type': 'multipart/form-data'
                    }
                });

                console.log('File uploaded successfully:', response.data);
                alert("File uploaded successfully");

            } catch (error) {
                if (error.response && error.response.status === 500) {
                    alert("File uploaded successfully");
                    const lastDot = selectedFile.name.lastIndexOf('.');
                    filename = selectedFile.name.substring(0, lastDot);
                    console.log(filename);
                    hideLoader();
                } else {
                    console.log('error:', error);
                    alert("File upload failed. Please try again.");
                    hideLoader();
                }
            }
        } else {
            console.error('No file selected');
            alert("No file selected");
        }
        hideLoader();
        
    });

    let ruleCount = 0;

    document.getElementById('addRule').addEventListener('click', async () => {
        ruleCount++;

        const rulesContainer = document.getElementById('rulesContainer');

        const ruleDiv = document.createElement('div');
        ruleDiv.classList.add('flex-container');
        ruleDiv.innerHTML = `
    <div class="rule-row">
        <label for="columnName${ruleCount}" class="rule-label">Column Name:</label>
        <select id="columnName${ruleCount}" name="columnName${ruleCount}" required class="rule-select">
            ${header.map(colName => `<option value="${colName}">${colName}</option>`).join('')}
        </select>
    </div>

    <div class="rule-row">
        <label for="operation${ruleCount}" class="rule-label">Operation:</label>
        <select id="operation${ruleCount}" name="operation${ruleCount}" required class="rule-select">
            ${sparkOperations.map(op => `<option value="${op}">${op}</option>`).join('')}
        </select>
    </div>
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
                column: columnName,
                operation: operation,
            };

            rules.push(rule);
        }

        const cleaningRulesJSON = JSON.stringify(rules, null, 2);
        console.log('Generated Cleaning Rules:', cleaningRulesJSON);

        // Display the generated JSON on the web page
        // displayJSON(rules);

        const jsonfilename = `${filename}.json`;
        const jsonfilename_blob = new Blob([cleaningRulesJSON], { type: 'application/json' });

        const jsonNew = new File([jsonfilename_blob], jsonfilename, { type: 'application/json' });
        // json file created here
        console.log('JSONNew', jsonNew.name);
        console.log('JSONNew', jsonNew);

        // upload this created json file to gcs bucket in the same path 

        if (1) {
            const formData = new FormData();
            formData.append('file', jsonNew);
            formData.append('userID', userID.value);

            console.log("User ID:", userID.value); // Log the userID value
            // tried using a localhost but dfacing issues with cors current stop gap fix is updating ip of the server here
            try {
                // hardcoded, need to update when we restart instance?!
                const response = await axios.post(`${prod_ip}/upload`, formData, {
                    headers: {
                        'Content-Type': 'multipart/form-data'
                    }
                });

                console.log('Json File uploaded successfully:', response.data);
                alert("Processing File...");

            } catch (error) {
                if (error.response && error.response.status === 500) {
                    alert("Json File uploaded successfully");
                } else {
                    console.log('error:', error);
                    alert("File upload failed. Please try again.");
                }
            }
        } else {
            console.error('No file selected');
            alert("No file selected");
        }
        showLoader();
        const triggerJson = {
            "project_name": userID.value,
            "project_id": userID.value,
            "dataset_name": filename,
            "username": userID.value
        };

        console.log('Trigger JSON:', triggerJson);
        try {
            const apiUrl = `${backend}/projects/jobsapi/`;

            const response = await axios.post(apiUrl, triggerJson, {
                headers: {
                    'Content-Type': 'application/json'
                },
            });

            console.log(response);



            console.log('Response:', response.data.download_url);

            if (response) {
                hideLoader();
            }


            dataField.value = response.data.download_url;

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

// output download button function
function downloadFromLink() {
    var dataField = document.getElementById("dataField");
    if (dataField.value) {
        window.open(dataField.value, '_blank');
    }
}

// output copy link button function
function copyToClipboard() {
    var dataField = document.getElementById("dataField");
    // dataField = "";
    // Select the text in the textfield
    dataField.select();
    dataField.setSelectionRange(0, 99999); /* For mobile devices */

    // Copy the selected text to clipboard
    if (dataField.value !== "") {
        navigator.clipboard.writeText(dataField.value)
            .then(() => {
                // Alert the user that the link is copied
                alert("Link copied to clipboard: " + dataField.value);
            })
            .catch(error => {
                console.log('Error copying to clipboard:', error);
            });
    }
    else {
        console.log("nothing to copy")
    }
}


// loader functions
function showLoader() {
    document.getElementById('overlay').style.display = 'block';
    document.getElementById('loader').style.display = 'block';
  
    // Disable all interactive elements
    disableInteractiveElements();
}

function hideLoader() {
    document.getElementById('overlay').style.display = 'none';
    document.getElementById('loader').style.display = 'none';
  
    // Enable all interactive elements
    enableInteractiveElements();
}


function disableInteractiveElements() {
    // Disable all interactive elements
    var interactiveElements = document.querySelectorAll('a, button, input, select, textarea');
    interactiveElements.forEach(function (element) {
        element.disabled = true;
    });

    // Optionally, you can add a class to visually indicate that elements are disabled
    document.body.classList.add('disabled');
}

function enableInteractiveElements() {
    // Enable all interactive elements
    var interactiveElements = document.querySelectorAll('a, button, input, select, textarea');
    interactiveElements.forEach(function (element) {
        element.disabled = false;
    });

    // Optionally, remove the class that indicates elements are disabled
    document.body.classList.remove('disabled');
}