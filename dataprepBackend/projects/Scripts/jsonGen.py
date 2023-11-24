import json

# Sample Python dictionary
my_dict = {
    "reads": "mean_normalization",
    
}

# Specify the file path where you want to save the JSON file
file_path = "output.json"

# Open the file in write mode and use json.dump() to write the dictionary to the file
with open(file_path, 'w') as json_file:
    json.dump(my_dict, json_file, indent=4)

print(f"JSON file '{file_path}' has been created.")