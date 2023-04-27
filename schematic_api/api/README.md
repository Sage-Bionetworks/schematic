## Setup
There are two ways to run schematic APIs: 1) start a flask server and run your application locally (preferred for external users); 2) build a docker image that allows you to run a container with flask application with uWSGI. 

To start a local Flask server and test your endpoints:

```bash
source .venv/bin/activate
python3 run_api.py
```
If you define `APP_PORT` as `3001` in `docker-compose.yml`, you should be able to see the application running when visiting `http://localhost:3001/v1/ui/`

To start a docker container that runs flask application with uWSGI:
1) Comment out line 4 to line 19 in `docker-compose.yml`. 
2) Create .env file based on env.example. Fill in `service_account_creds.json`
3) Make sure that you have all the dependencies installed, including uWSGI and flask. 
4) Build a docker image and spin up docker container `schematic-api-aws` by running: 
```bash
docker compose up
```
If you define the value of SERVER_PORT as `7080` in `.env` file, you should be able to see the application running when visiting `http://localhost:7080/v1/ui/`

By default, this command builds up two containers (`schematic` and `schematic-aws`). You could spin up two containers if you want. But only `schematic-aws` runs flask with uWSGI. 

## Notes on installation
1. The warning message: "connexion.options - The swagger_ui directory could not be found." could be addressed by pip installing connexion[swagger-ui]. For Mac users, the command should be: 
```bash
pip install connexion['swagger=ui']
```

2. Please also consider following these [instructions](https://sage-schematic.readthedocs.io/en/develop/cli_reference.html) to fill in configuration file and run the following command to obtain `credentials.json` and `token.pickle`:
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

## Notes for trouble shooting
1. ImportError: cannot import name 'soft_unicode' from 'markupsafe' 
2. ImportError: cannot import name 'json' from 'itsdangerous' 

To resolve: 

Make sure that the following libraries have the correct version: 
* itsdangerous version: ^2.0.0
* jinja2 version: >2.11.3
* markupsafe version: ^2.1.0

## Notes for using schematic features and API endpoints utilizing Google Services (e.g. manifest generation): 
Before trying out the API endpoints, please make sure that you have obtained `credentials.json`, `schematic_service_account_creds.json`, and `token.pickle`. (Instructions can be found in schematic/README.md) 


###  GET /manifest/generate

This endpoint functions similarly to the following: 
```bash
schematic manifest -c ~/path/to/config.yml get -d <synapse id> -s -oauth
```

Examples: 
1) Generate a patient manifest by using the sample data model.

    Simply click on "Try it out" on swagger UI 

2) Get an existing manifest: 

* Step 1:  Make sure you have credentials to download the desired manifest from Synapse. The "download" button should be disabled if you don't have credentials. 

* Step 2: Make sure you set asset_view to the right value. For Synapse, asset_view is the same as master_fileview in config.yml. 

* Step 3: Use parent id of the manifest for "dataset_id" parameter
    
Note: if the dataset_id you provided is invalid, it will generate an empty manifest based on the data model. 

### POST /model/submit
    
* For the input_token parameter, please use the value of `auth token` in your `.synapseConfig`

* For the dataset_id parameter, please create a test folder on synapse and use its synapse ID

* For uploading a csv file, please generate an empty manifest by using `GET /manifest/generate` endpoint and fill it out. 

For the patient manifest, "Family History" column only accepts a list. The details of the validation rules (based on the example data model) could be found [here](https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.csv)

After execution, you should be able to see your manifest.csv get uploaded to your test folder on Synapse. The execution should return the synapse ID of your submitted manifest.
