from schematic.utils.remove_sensitive_data_utils import (
    redact_string,
    redacted_sensitive_data_in_exception,
)


class TestFilterSensitiveData:
    def test_redact_string(self) -> None:
        # given a string with sensitive data, make sure that they are redacted
        sensitive_data = "googleapiclient.errors.HttpError: <HttpError 400 when requesting https://sheets.googleapis.com/v4/spreadsheets/11234budyhf:batchUpdate?fields=%2A&alt=json returned abc>"
        redacted_data = redact_string(sensitive_data)
        assert (
            redacted_data
            == "googleapiclient.errors.HttpError: <HttpError 400 when requesting [REDACTED_GOOGLE_SHEETS]:batchUpdate?fields=%2A&alt=json returned abc>"
        )

    def test_redacted_sensitive_data_in_exception(self) -> None:
        # given a dictionary of exception attributes, make sure that sensitive data is redacted
        exception_attributes = {
            "exception.message": "googleapiclient.errors.HttpError: <HttpError 400 when requesting https://sheets.googleapis.com/v4/spreadsheets/11234budyhf:batchUpdate?fields=%2A&alt=json returned>",
            "exception.stacktrace": 'Traceback (most recent call last):\n  File "<stdin>", line 1, in <module>\n  File "<string>", line 1, in <module>\n  File "/usr/local/lib/python3.7/dist-packages/googleapiclient/_helpers.py", line 134, in positional_wrapper\n    return wrapped(*args, **kwargs)\n  File "/usr/local/lib/python3.7/dist-packages/googleapiclient/http.py", line 905, in execute\n    raise HttpError(resp, content, uri=self.uri)\ngoogleapiclient.errors.HttpError: <HttpError 400 when requesting https://sheets.googleapis.com/v4/spreadsheets/11234budyhf:batchUpdate?fields=%2A&alt=json returned>',
        }
        redacted_exception_attributes = redacted_sensitive_data_in_exception(
            exception_attributes
        )
        assert (
            redacted_exception_attributes["exception.message"]
            == "googleapiclient.errors.HttpError: <HttpError 400 when requesting [REDACTED_GOOGLE_SHEETS]:batchUpdate?fields=%2A&alt=json returned>"
        )
        assert (
            redacted_exception_attributes["exception.stacktrace"]
            == 'Traceback (most recent call last):\n  File "<stdin>", line 1, in <module>\n  File "<string>", line 1, in <module>\n  File "/usr/local/lib/python3.7/dist-packages/googleapiclient/_helpers.py", line 134, in positional_wrapper\n    return wrapped(*args, **kwargs)\n  File "/usr/local/lib/python3.7/dist-packages/googleapiclient/http.py", line 905, in execute\n    raise HttpError(resp, content, uri=self.uri)\ngoogleapiclient.errors.HttpError: <HttpError 400 when requesting [REDACTED_GOOGLE_SHEETS]:batchUpdate?fields=%2A&alt=json returned>'
        )
