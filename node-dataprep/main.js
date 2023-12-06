document.addEventListener('DOMContentLoaded', () => {
    // const selectFileButton = document.getElementById('selectFileButton');
    const uploadButton = document.getElementById('uploadButton');
    const fileInput = document.getElementById('fileInput');
    const userID = document.getElementById('userID');
    let selectedFile = null;

    const postData = {
        "project_name": "test",
        "project_id": "test001",
        "dataset_name": "subset_dataset",
        "username": "test_user"
    }

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

            try {
                const response = await axios.post('http://localhost:8080/upload', formData, {
                    headers: {
                        'Content-Type': 'multipart/form-data'
                    }
                });

                console.log('File uploaded successfully:', response.data);
                alert("File uploaded successfully");

                // window.location.href = 'download.html';
            } catch (error) {
                // console.error('Error uploading file:', error);
                if (error.response && error.response.status === 500) {
                    alert("File uploaded successfully");
                    // window.location.href = 'download.html';
                }
                else {
                    alert("File upload failed. Please try again.");
                }

            }
            try {
                const apiUrl = 'http://192.168.1.73:8000/projects/jobsapi/';

                const response = await axios.post(apiUrl, postData)

                console.log('Response:', response.data.download_url);

            }
            catch (error) {
                if (error.response && error.response.status === 500) {
                    // alert("No response");
                    console.log('No response');
                    // window.location.href = 'download.html';
                }
                else {
                    // alert("File upload failed. Please try again.");
                    console.log('error');
                }
            }
        } else {
            console.error('No file selected');
            alert("No file selected");
        }
    });
});
