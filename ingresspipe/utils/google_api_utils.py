def execute_google_api_requests(service, requests_body, **kwargs):
    """
    Execute google API requests batch; attempt to execute in parallel.

    Args:
        service: google api service; for now assume google sheets service that is instantiated and authorized
        service_type: default batchUpdate; TODO: add logic for values update
        kwargs: google API service parameters
    Return: google API response
    """

    if "spreadsheet_id" in kwargs and "service_type" in kwargs and kwargs["service_type"] == "batch_update":
        # execute all requests
        response = service.spreadsheets().batchUpdate(spreadsheetId=kwargs["spreadsheet_id"], body = requests_body).execute()
        
        return response