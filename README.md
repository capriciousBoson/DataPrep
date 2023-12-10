# Developer Setup Instructions

Welcome to the project! To get started, follow these steps to set up your development environment.

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/your-repository.git](https://github.com/capriciousBoson/DataPrep.git)
   cd DataPrep
Set Up keys.json
Create a service account and make sure the service account has access to Dataproc and storage, and download keys for the service account.
Place your keys.json file in the dataprepBackend folder. This file contains sensitive information, so it should be kept secure and not shared publicly.
  Note: Ensure that this file is included in your .gitignore to avoid unintentionally committing it to the repository.
Or Add it as a secret in the GCP account to keep the key file secure.



/your-repository
  /dataprepBackend
    - keys.json
    
Configure config.py
Copy the config_template.py file in the dataprepBackend folder to a new file named config.py.


cp dataprepBackend/config_template.py dataprepBackend/config.py
Edit config.py and set any necessary configuration values for your development environment. This file should contain non-sensitive configuration settings and can be version-controlled.

python
Copy code
# dataprepBackend/config.py

class Config:
    DEBUG = True
    # Add other configuration variables as needed
Update .gitignore
Ensure that your keys.json file and any sensitive configuration files are ignored by Git. Modify or create a .gitignore file in the root of your repository if needed.


# .gitignore

# Ignore keys and sensitive configuration files
/dataprepBackend/keys.json
/dataprepBackend/config.py
Create Docker Container

docker build -t dataprep .
Run the Application

docker run -it -p 8000:8000 dataprep
Open your browser and navigate to http://localhost:8000 to view the application.

Additional Notes

Keep your keys.json file secure and do not share it in public repositories.
Always follow best practices for handling sensitive information.
If you have any questions or issues, feel free to reach out to the project maintainers.
Happy coding! 🚀
