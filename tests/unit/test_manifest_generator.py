from schematic.manifest.generator import ManifestGenerator
import pytest
from schematic.schemas.generator import SchemaGenerator
from unittest.mock import Mock
from unittest.mock import patch

@pytest.fixture()
def ManifestGeneratorMock(helpers):
    mg = ManifestGenerator(
        path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
        title="Example",
        root="Patient"
    )
    yield mg

@pytest.fixture
def mock_create_blank_google_sheet():
    'Mock creating a new google sheet'
    er = Mock()
    er.return_value = "mock_spreadsheet_id"
    yield er

@pytest.fixture
def mock_create_json_schema():
    er = Mock()
    er.return_value = {'$schema': 'http://json-schema.org/draft-07/schema#', '$id': 'http://example.com/Example', 'title': 'Example', 'type': 'object', 'properties': {'Patient ID': {'not': {'type': 'null'}, 'minLength': 1}, 'Year of Birth': {}, 'Sex': {'enum': ['Other', 'Male', 'Female']}, 'Component': {}, 'Diagnosis': {'enum': ['Healthy', 'Cancer']}, 'Family History': {'type': 'array', 'items': {'enum': ['Breast', 'Lung', 'Prostate', 'Skin', 'Colorectal', '']}, 'maxItems': 5}, 'Cancer Type': {'enum': ['Breast', 'Lung', 'Prostate', 'Skin', 'Colorectal', '']}}, 'required': ['Patient ID', 'Sex', 'Diagnosis'], 'allOf': [{'if': {'properties': {'Diagnosis': {'enum': ['Cancer']}}, 'required': ['Diagnosis']}, 'then': {'properties': {'Family History': {'type': 'array', 'items': {'enum': ['Breast', 'Lung', 'Prostate', 'Skin', 'Colorectal']}, 'maxItems': 5}}, 'required': ['Family History']}}, {'if': {'properties': {'Diagnosis': {'enum': ['Cancer']}}, 'required': ['Diagnosis']}, 'then': {'properties': {'Cancer Type': {'enum': ['Breast', 'Lung', 'Prostate', 'Skin', 'Colorectal']}}, 'required': ['Cancer Type']}}]}
    yield er

@pytest.fixture
def required_metadata_field_example():
    required_metadata_field = {'Year of Birth': [], 'Diagnosis': ['Cancer', 'Healthy'], 'Sex': ['Male', 'Female', 'Other'], 'Patient ID': [], 'Component': [], 'Family History': ['Colorectal', 'Breast', 'Skin', 'Prostate', 'Lung', ''], 'Cancer Type': ['Colorectal', 'Breast', 'Skin', 'Prostate', 'Lung', '']}
    yield required_metadata_field

@pytest.fixture
def get_empty_gsheet(ManifestGeneratorMock):
    mg = ManifestGeneratorMock
    spreadsheet_id = mg._create_blank_manifest(title='Example')
    yield spreadsheet_id


class TestManifestGenerator:
    @pytest.mark.parametrize("title", [None, "mock_title"])
    def test_init(self, title, helpers):
        generator = ManifestGenerator(
            title=title,
            path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
            additional_metadata=False,
            use_annotations=False,
            root="Patient"
        )

        assert type(generator.title) is str
        if title: 
            assert generator.title=="mock_title"
        else:
            assert generator.title=="Patient - Manifest"
        assert generator.additional_metadata == False
        assert generator.use_annotations == False
        assert generator.root== "Patient"
        assert type(generator.sg) is SchemaGenerator
        assert generator.is_file_based == False
    
    ### all google sheet related functions
    @pytest.mark.parametrize("column_index", [0, 2, 26, 27])
    def test_column_to_letter(self, column_index, ManifestGeneratorMock):
        """Find google sheet letter representation of a column index integer"""
        mg=ManifestGeneratorMock
        letter = mg._column_to_letter(column_index)
        if column_index == 2:
            assert letter== "C"
        elif column_index == 0: 
            assert letter == "A"
        elif column_index == 26:
            assert letter == "AA"
        else: 
            assert letter=="AB"


    def test_column_to_sheet_ranges(self, ManifestGeneratorMock):
        """map a set of column indexes to a set of Google sheet API ranges: each range includes exactly one column"""
        mg=ManifestGeneratorMock
        column_idxs=[5, 6]
        ranges = mg._columns_to_sheet_ranges(column_idxs)
        assert {'startColumnIndex': 6, 'endColumnIndex': 7} in ranges
        assert {'startColumnIndex': 5, 'endColumnIndex': 6} in ranges

    @pytest.mark.parametrize("is_node_required", [True, False])
    def test_dependency_formatting(self, ManifestGeneratorMock, is_node_required):
        """Given a column index and an equality argument (e.g. one of valid values for the given column fields), generate a conditional formatting rule based on a custom formula encoding the logic:

        'if a cell in column idx is equal to condition argument, then set specified formatting'
        """

        # return different background color "is_node_required" has different values
        mg=ManifestGeneratorMock
        formatting_rule = mg._column_to_cond_format_eq_rule(4, "Cancer", required=is_node_required)

        if is_node_required: 
            assert formatting_rule["format"]["backgroundColor"] == {"red": 0.9215, "green": 0.9725, "blue": 0.9803,}
        else: 
            formatting_rule["format"]["backgroundColor"] == {"red": 1.0, "green": 1.0, "blue": 0.9019,}
    

    def test_copy_google_file(self, config, ManifestGeneratorMock, mock_create_blank_google_sheet):
        '''
        Copy an existing file.
        '''
        mg=ManifestGeneratorMock
        mock_create_blank_google_sheet = mg._gdrive_copy_file(config["style"]["google_manifest"]["master_template_id"], "Example")
        assert type(mock_create_blank_google_sheet) == str

    def test_create_blank_manifest(self, ManifestGeneratorMock, mock_create_blank_google_sheet):
        '''
        Test generating a blank manifest
        '''
        mg=ManifestGeneratorMock
        mock_create_blank_google_sheet = mg._create_blank_manifest(title='Example')
        assert type(mock_create_blank_google_sheet) == str
        assert len(mock_create_blank_google_sheet) > 0


    def test_create_empty_manifest_spreadsheet(self, ManifestGeneratorMock):
        '''
        Create an empty manifest spreadsheet regardless if master_template_id is provided
        Note: _create_empty_manifest_spreadsheet is only a wrapper around _create_blank_manifest and _gdrive_copy_file
        '''
        mg=ManifestGeneratorMock

        spreadsheet_id = mg._create_empty_manifest_spreadsheet(title="Example")
        assert type(spreadsheet_id) == str
        assert len(spreadsheet_id) > 0
    
    def test_get_cell_borders(self, ManifestGeneratorMock):
        '''
        set color of cell border
        '''
        mg=ManifestGeneratorMock
        mock_cell_range  = {'sheetId': 0, 'startRowIndex': 0}
        border_style_req = mg._get_cell_borders(mock_cell_range)
        assert border_style_req["updateBorders"]['range'] == mock_cell_range
        assert border_style_req["updateBorders"]['bottom']["color"] == {
            "red": 226.0 / 255.0,
            "green": 227.0 / 255.0,
            "blue": 227.0 / 255.0,
        }

    def test_set_permission(self, ManifestGeneratorMock, get_empty_gsheet):
        '''
        Test setting google sheet permission
        '''
        mg=ManifestGeneratorMock

        # set permission
        spreadsheet_id = get_empty_gsheet

        # now retrieve permission
        permission = mg.drive_service.permissions().list(fileId=spreadsheet_id).execute()
        for info in permission["permissions"]: 
            if info["id"] == "anyoneWithLink":
                assert info["type"] == "anyone"
                assert info["role"] == "writer"

    def test_store_valid_values_as_data_dictionary(self, ManifestGeneratorMock):
        '''
        store valid values in google sheet (sheet 2). This step is required for "ONE OF RANGE" validation
        '''
        with patch('schematic.manifest.generator.ManifestGenerator._execute_spreadsheet_service') as MockClass:
            # mocking response of _execute_spreadsheet_service
            instance = MockClass.return_value
            instance.method.return_value = 'test ressponse'

            # use ManifestGenerator
            mg = ManifestGeneratorMock
            valid_values = mg._store_valid_values_as_data_dictionary(column_id=2, valid_values=[{'userEnteredValue': 'Cancer'}, {'userEnteredValue': 'Healthy'}], spreadsheet_id="mock_spreadsheet_id")
            assert {'userEnteredValue': '=Sheet2!C2:C3'} in valid_values
    
    @pytest.mark.parametrize("validation_type", ["ONE_OF_RANGE","ONE_OF_LIST"])
    @pytest.mark.parametrize("strict", [True, False]) # strict could also be set to None
    @pytest.mark.parametrize("custom_ui", [True, False]) # should not be set to None
    @pytest.mark.parametrize("input_message", ["test message", '']) # should not be set to None; But if could be set to an empty string
    @pytest.mark.parametrize("column_id", [2, 3])
    def test_get_column_data_validation_values(self, ManifestGeneratorMock, column_id, validation_type, strict, custom_ui, input_message):
        '''
        get data validation values
        '''
        with patch('schematic.manifest.generator.ManifestGenerator._execute_spreadsheet_service') as MockClass:
            # mocking response of _execute_spreadsheet_service
            instance = MockClass.return_value
            instance.method.return_value = 'test ressponse'

            # use ManifestGenerator
            mg = ManifestGeneratorMock
            validation_body = mg._get_column_data_validation_values(column_id=column_id, valid_values=[{'userEnteredValue': 'Cancer'}, {'userEnteredValue': 'Healthy'}], validation_type=validation_type, spreadsheet_id="mock_spreadsheet_id", strict=strict, input_message=input_message, custom_ui=custom_ui)
            req_body = validation_body["requests"][0]["setDataValidation"]
            assert req_body["range"]["startColumnIndex"] == column_id
            assert req_body["range"]["endColumnIndex"] == column_id + 1

            if validation_type: 
                assert req_body["rule"]["condition"]["type"] == validation_type
            else: 
                assert req_body["rule"]["condition"]["type"] == "ONE_OF_LIST"
            

            assert req_body["rule"]["inputMessage"] == input_message


            assert req_body["rule"]["strict"] == strict

            assert req_body["rule"]["showCustomUi"] == custom_ui


    ######################
    ########### start testing functions related to dealing with json schema
    #######################
    # Note: when refactoring, we should put fucntions related to loading schema and getting requirements in the same class
    @pytest.mark.parametrize("schema_path_provided", [True, False])
    def test_get_json_schema(self, helpers, ManifestGeneratorMock, schema_path_provided):
        '''
        Test loading json schema
        '''
        mg=ManifestGeneratorMock

        if schema_path_provided: 
            json_filepath = helpers.get_data_path("example.model.jsonld")
        else: 
            json_filepath = None
        loaded_json_schema = mg._get_json_schema(json_schema_filepath=json_filepath)

        assert type(loaded_json_schema) == dict
        if not schema_path_provided: 
            assert loaded_json_schema["title"] == "Example"
            assert type(loaded_json_schema["properties"]) == dict

        # note: _get_json_schema should maybe be deprecated in refactoring. In the future, we should just use schemaGenerator to load schema

    @pytest.mark.parametrize("req", ["Family History", "Diagnosis"])
    def test_get_valid_values_from_jsonschema_property(self, ManifestGeneratorMock, req, mock_create_json_schema):
        '''
            Get valid values for a manifest attribute based on the corresponding
            values of node's properties in JSONSchema
        '''
        # note: _get_valid_values_from_jsonschema_property function should only be relevant to json schema generated by "_get_json_schema"

        mg=ManifestGeneratorMock

        loaded_json_schema = mock_create_json_schema()

        # get valid values from json schema property: 
        val_values = mg._get_valid_values_from_jsonschema_property(loaded_json_schema["properties"][req])

        assert type(val_values) == list
        if req == "Family History":
            assert 'Prostate' and 'Colorectal' and 'Breast' and 'Skin' and 'Lung' in val_values
        else: 
            assert 'Cancer' and 'Healthy' in val_values

    @pytest.mark.parametrize("fields", [["Diagnosis"], ["Sex", "Family History"]])
    def test_get_required_metadata_fields(self, ManifestGeneratorMock, fields, mock_create_json_schema):
        '''
        For the root node gather dependency requirements (all attributes linked to this node)
        and corresponding allowed values constraints (i.e. valid values).
        '''
        mg=ManifestGeneratorMock
        loaded_json_schema = mock_create_json_schema()
        required_metadata_fields = mg._get_required_metadata_fields(loaded_json_schema, fields)

        assert type(required_metadata_fields) == dict
        if fields == ["Diagnosis"]:
            assert "Healthy" and "Cancer" in required_metadata_fields["Diagnosis"]
        else: 
            assert "Other" and "Male" and "Female" in required_metadata_fields["Sex"]
            assert "Lung" and "Colorectal" and "Skin" in required_metadata_fields["Family History"]

    def test_gather_dependency_requirements(self, ManifestGeneratorMock, mock_create_json_schema):
        """Gathering dependency requirements and allowed value constraints
            for conditional dependencies, if any"""
        mg=ManifestGeneratorMock
        loaded_json_schema = mock_create_json_schema()
        mock_required_attribute = {'Test attribute': ['Test']}
        req_dict = mg._gather_dependency_requirements(loaded_json_schema, mock_required_attribute)
        assert "Test attribute" in req_dict
        assert 'Test' in req_dict["Test attribute"]

    @pytest.mark.parametrize("additional_metadata", [{"Mock attribute": "Test", "Filename": ["Test file name", "Test file name"]}, {"Mock attribute": "Test", "Filename": ["Test file name"]}])
    def test_add_root_to_component(self, helpers, additional_metadata):
        '''If 'Component' is in the column set, add root node as a
        metadata component entry in the first row of that column.
        '''
        mock_class = ManifestGenerator(path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
        title="Example",
        root="BulkRNA-seqAssay",
        additional_metadata=additional_metadata
        )
        mock_required_metadata_fields={"Component": [], "Mock status": ["Test1", "Test2"]}
        mock_class._add_root_to_component(mock_required_metadata_fields)

        # number of time that "BulkRNA-seqAssay" should appear
        time_to_repeat = len(additional_metadata["Filename"])

        # add 'Component': [root] in additional_metadata_fields
        assert mock_class.additional_metadata["Component"] == ["BulkRNA-seqAssay"] * time_to_repeat
        assert mock_class.additional_metadata["Mock attribute"] == "Test"


    




    
    


















        


