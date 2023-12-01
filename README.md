Developer Setup Instructions
Welcome to the project! To get started, follow these steps to set up your development environment.

1. Clone the Repository
bash
Copy code
git clone https://github.com/your-username/your-repository.git
cd your-repository
2. Set Up keys.json
Place your keys.json file in the dataprepBackend folder. This file contains sensitive information, so it should be kept secure and not shared publicly.

Note: Ensure that this file is included in your .gitignore to avoid unintentionally committing it to the repository.

plaintext
Copy code
/your-repository
  /dataprepBackend
    - keys.json
3. Configure config.py
Copy the config_template.py file in the dataprepBackend folder to a new file named config.py.

bash
Copy code
cp dataprepBackend/config_template.py dataprepBackend/config.py
Edit config.py and set any necessary configuration values for your development environment. This file should contain non-sensitive configuration settings and can be version-controlled.

python
Copy code
# dataprepBackend/config.py

class Config:
    DEBUG = True
    # Add other configuration variables as needed
4. Update .gitignore
Ensure that your keys.json file and any sensitive configuration files are ignored by Git. Modify or create a .gitignore file in the root of your repository if needed.

plaintext
Copy code
# .gitignore

# Ignore keys and sensitive configuration files
/dataprepBackend/keys.json
/dataprepBackend/config.py
5. create Docker Container 
bash
docker build -t container name  .
6. Run the Application
bash 
docker run -it -p 8000:8000 container name 
Open your browser and navigate to http://localhost:8000 to view the application.

Additional Notes
Keep your keys.json file secure and do not share it in public repositories.
Always follow best practices for handling sensitive information.
If you have any questions or issues, feel free to reach out to the project maintainers.
Happy coding! 🚀
