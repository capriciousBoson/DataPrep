// Function to fetch data from the backend and populate the textfield using Axios
function fetchDataFromBackend() {
    // Replace 'your_backend_api_url' with the actual API endpoint
    axios.get('http://34.174.86.160:8000//projects/jobsapi/')
        .then(response => {
            // Update the value of the dataField input with the fetched data
            document.getElementById("dataField").value = response.data.download_url;
            console.log(response.data.fact);
        })
        .catch(error => {
            console.error('Error fetching data:', error);
        });
}

function copyToClipboard() {
    var dataField = document.getElementById("dataField");

    // Select the text in the textfield
    dataField.select();
    dataField.setSelectionRange(0, 99999); /* For mobile devices */

    // Copy the selected text to clipboard
    navigator.clipboard.writeText(dataField.value)
        .then(() => {
            // Alert the user that the link is copied
            alert("Link copied to clipboard: " + dataField.value);
        })
        .catch(error => {
            console.error('Error copying to clipboard:', error);
        });
}


// Fetch data from the backend when the page loads
fetchDataFromBackend();
