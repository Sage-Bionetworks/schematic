# Credentials

The `credentials.json` file is required when you are using [`OAuth2`](https://developers.google.com/identity/protocols/oauth2) to authenticate with the Google APIs.

Steps involved in the `OAuth2` [authorization flow](https://github.com/Sage-Bionetworks/schematic/blob/develop/schematic/utils/google_api_utils.py#L18):

- The app (i.e., _schematic_) looks for a `token.pickle` file first. It is a file that is created when you have authorized your account to access Google APIs for the first time. If the file already exists, then it get the credentials from it, meaning you do not have to allow access several times.
- If the `token.pickle` file does not exist, the app will load the credentials for you from the `credentials.json` file and create a `token.pickle` in your project root directory when you access any of the schematic services (e.g., through the Command Line Interface) and the authorization flow completes for the first time. For this:
  - A URL will be prompted to you when you access a _schematic_ CLI utility. Copy the URL from the console and open it in your browser.
  - If you are not already logged into your Google account, you will be prompted to log in. If you are logged into multiple Google accounts, you will be asked to select one account to use for the authorization.
  - You might see a "Google hasn’t verified this app" warning from Chrome on your browser. To resolve this, click on "Advanced" and then "Quickstart (unsafe)". Select a Google account and authorize the _Quickstart_ script to access Google Drive/Sheets under your account.