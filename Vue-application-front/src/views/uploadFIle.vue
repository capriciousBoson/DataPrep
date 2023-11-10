<template>
    <div>
        <h1>DataPrep</h1>
        <input type="file" id="fileInput" style="display: none" @change="onFileChange"/>
        <label for="fileInput" class="custom-upload">Select File</label>
        <input type="file"  />
        <!-- <label class="fileupload" @click="uploadFile">Upload</label> -->
        <button class="fileupload" id= "fileupload" @click="uploadFile" :disabled="!selectedFile">Upload</button>
        <p>{{ selectedFileName }}</p>
    </div>
</template>

<script>
export default {
    data() {
        return {
            selectedFile: null,
            selectedFileName: 'Please select a .csv file to upload.'
        };
    },

    methods: {
        onFileChange(event) {
            const fileInput = event.target;
            if (fileInput.files.length > 0) {
                this.selectedFile = fileInput.files[0];
                this.selectedFileName = `Selected file: ${this.selectedFile.name}`;
            } else {
                this.selectedFile = null;
                this.selectedFileName = 'Please select a .csv file to upload.';
            }
        },
        onFileSelected(event) {
            this.selectedFile = event.target.files[0];
        },
        async uploadFile() {
            if (!this.selectedFile) {
                alert("Please select a file to upload.");
                return;
            }
            
            const formData = new FormData();
            formData.append("file", this.selectedFile);

            // Replace 'YOUR_GCS_UPLOAD_URL' with the actual GCS upload URL
            const uploadUrl = 'http://storage.googleapis.com/data-prep-bucket/this.selectedFile';

            try {
                const response = await fetch(uploadUrl, {
                    method: "POST",
                    body: formData,
                });
                if (response.ok) {
                    alert("File uploaded successfully!");
                } else {
                    alert("File upload failed. Please try again.");
                }
            } catch (error) {
                console.error("Error uploading file:", error);
                alert("File upload failed. Please try again.");
            }
        },
    },
};
</script>
  