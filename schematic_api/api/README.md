# Directions to run APIs locally
## Run Schematic without uWSGI (preferred approach for external users)
### Run Schematic APIs locally 
1) To run Schematic API locally, simply install dependencies by doing: 
```
poetry install 
```
2) Get `service_account_creds.json` by doing `schematic init --config /path/to/config.yml`.
3) Run the APIs by doing: 
```
poetry run python3 run_api.py
```
You should be able to see swagger UI interface when visiting `localhost:3001/v1/ui`

### Run Schematic APIs in a docker container 
To run Schematic in a docker container, check out `docker-compose.yml` file in this repository. Please comment out the second part related to building `schematic-aws` container. You could start Schematic docker container by running: 
```
docker compose up --build --remove-orphans
```

## Run Schematic APIs with uWSGI and nginx in a docker container (preferred approach for developers)
### install uWSGI
Install uWSGI by doing: 
```
poetry install --all-extras
```
Note: this approach only works for unix OSs users or windows user with WSL

### Run Schematic APIs with uWSGI and nginx in a docker container
See steps below: 
1) Comment out the first part of `docker-compose.yml` and focus only on building container `schematic-aws`.

2) Get `schematic_service_account_creds.json` by following these [instructions](https://scribehow.com/shared/Enable_Google_Drive_and_Google_Sheets_APIs_for_project__yqfcJz_rQVeyTcg0KQCINA)

3) Make a copy of `env.example` and rename it as `.env` and keep it in the same directory as `env.example`. By default, schematic uses port 81. If port 81 is not available, please update `USE_LISTEN_PORT` in `.env` file. 

4) Copy the content of `service_account_creds.json` and put it in `.env` file after key `SERVICE_ACCOUNT_CREDS`. Remember to wrap around the credentials with single quotes.

5) Build a docker image and spin up docker container `schematic-api-aws` by running: 
```bash
docker compose up --build --remove-orphans
```
You should be able to view your application when visit: `https://127.0.0.1/v1/ui/`. You might receive an notification like this in your browser: 

<img width="400" alt="Screen Shot 2023-05-23 at 3 31 46 PM" src="https://github.com/Sage-Bionetworks/schematic/assets/55448354/b5d44f56-5375-47cf-8dbd-d4d611f594c4">

Please click on "show details" and "visit this website" to proceed. Note that the instructions might be slightly different for different browsers. 

By default, this command builds up two containers (`schematic` and `schematic-aws`). You could spin up two containers if you want. But only `schematic-aws` runs flask with uWSGI. 

## Notes on installation
1. The warning message: "connexion.options - The swagger_ui directory could not be found." could be addressed by pip installing connexion[swagger-ui]. For Mac users, the command should be: 
```bash
pip install connexion['swagger=ui']
```

2. Please also consider following these [instructions](https://scribehow.com/shared/Enable_Google_Drive_and_Google_Sheets_APIs_for_project__yqfcJz_rQVeyTcg0KQCINA) to obtain `schematic_service_account_creds.json`. Please specify the path of `schematic_service_account_creds.json` in config.yml (see google_sheets > service_acct_creds)

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
Before trying out the API endpoints, please make sure that you have obtained `schematic_service_account_creds.json` (Instructions can be found in schematic/README.md) 


###  GET /manifest/generate

This endpoint functions similarly to the following: 
```bash
schematic manifest -c ~/path/to/config.yml get -d <synapse id>
```

Examples: 
1) Generate a patient manifest by using the sample data model.

    Simply click on "Try it out" on swagger UI 

2) Get an existing manifest: 

* Step 1:  Make sure you have credentials to download the desired manifest from Synapse. The "download" button should be disabled if you don't have credentials. 

* Step 2: Make sure you set asset_view to the right value. For Synapse, asset_view is the same as master_fileview_id in config.yml. 

* Step 3: Use parent id of the manifest for "dataset_id" parameter
    
Note: if the dataset_id you provided is invalid, it will generate an empty manifest based on the data model. 

### POST /model/submit
    
* For authorization, please use the value of `auth token` in your `.synapseConfig`

* For the dataset_id parameter, please create a test folder on synapse and use its synapse ID

* For uploading a csv file, please generate an empty manifest by using `GET /manifest/generate` endpoint and fill it out. 

For the patient manifest, "Family History" column only accepts a list. The details of the validation rules (based on the example data model) could be found [here](https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.csv)

After execution, you should be able to see your manifest.csv get uploaded to your test folder on Synapse. The execution should return the synapse ID of your submitted manifest.
