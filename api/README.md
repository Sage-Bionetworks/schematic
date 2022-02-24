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

* Please also consider following these [instructions](https://sage-schematic.readthedocs.io/en/develop/index.html) to fill in configuration file and run the following command to obtain credential.json:
```bash 
schematic init --config ~/path/to/config.yml
```


Access the Swagger UI docs at this location:
```bash
http://localhost:3001/v1/ui/
```