## Setup

To start a local Flask server and test your endpoints:

```bash
source .venv/bin/activate
python run_api.py
```

Notes on installation: 
* The warning message: "connexion.options - The swagger_ui directory could not be found." could be addressed by pip installing connexion[swagger-ui]. For Mac users, the command should be: 
 
```bash
pip install connexion['swagger=ui']
```

* Please also consider following these [instructions](https://sage-schematic.readthedocs.io/en/develop/index.html) to fill in configuration file and run the following command to obtain credential.json and token.pickle:
```bash 
schematic init --config ~/path/to/config.yml
```

Note: credentials.json and token.pickle have to be in the same directory as run_api.py. If you have obtained credentials.json before (but credentials.json is a different directory), please put config.yml and .synapseConfig in the same directory as run_api.py and run the above command again. 

After running the command above, please follow instructions in your console and visit the URL to authorize schematic. After putting in the authorization code, token.pickle and credentials.json should be automatically downloaded in your local directory. 

## Access Results
Access the Swagger UI docs at this location:
```bash
http://localhost:3001/v1/ui/
```