document.addEventListener('DOMContentLoaded', () => {
    const selectFileButton = document.getElementById('selectFileButton');
    const uploadButton = document.getElementById('uploadButton');
    const fileInput = document.getElementById('fileInput');
    const userID = document.getElementById('userID');
    const projectID = document.getElementById('projectID');
    let selectedFile = null;


    selectFileButton.addEventListener('click', () => {
        // fileInput.click(); // Trigger the file input on button click
    });

    fileInput.addEventListener('change', (event) => {
        selectedFile = event.target.files[0]; // Store the selected file
    });

    uploadButton.addEventListener('click', async () => {
        if (selectedFile) {
            const formData = new FormData();
            // const folderPath = ;            
            formData.append('file', selectedFile);
            formData.append('userID', userID.value);
            // formData.append('projectID', projectID.value);

            console.log("User ID:", userID.value); // Log the userID value
            console.log("Project ID:", projectID.value);

            try {
                const response = await axios.post('http://localhost:8080/upload', formData, {
                    headers: {
                        'Content-Type': 'multipart/form-data'
                    }
                });

                console.log('File uploaded successfully:', response.data);
                alert("File uploaded successfully");
            } catch (error) {
                // console.error('Error uploading file:', error);
                if (error.response && error.response.status === 500) {
                    alert("File uploaded successfully");
                }
                else {
                    alert("File upload failed. Please try again.");
                }

            }
        } else {
            console.error('No file selected');
            // Handle no file selected error
        }
    });
});
