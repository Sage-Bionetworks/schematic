from schematic.manifest.generator import ManifestGenerator
import pytest
from schematic.schemas.generator import SchemaGenerator

@pytest.fixture
def ManifestGeneratorMock(helpers):

    mg = ManifestGenerator(
        path_to_json_ld=helpers.get_data_path("example.model.jsonld"),
        title="Example",
        root="Patient"
    )

    yield mg

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
    

    def test_copy_google_file(self, config, ManifestGeneratorMock):
        '''
        Copy an existing file.
        '''
        mg=ManifestGeneratorMock
        s_id = mg._gdrive_copy_file(config["style"]["google_manifest"]["master_template_id"], "Example")
        assert type(s_id) == str

    def test_create_blank_manifest(self, ManifestGeneratorMock):
        '''
        Test generating a blank manifest
        '''
        mg=ManifestGeneratorMock
        spreadsheet_id = mg._create_blank_manifest(title='Example')
        assert type(spreadsheet_id) == str
        assert len(spreadsheet_id) > 0


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

    def test_set_permission(self, ManifestGeneratorMock):
        mg=ManifestGeneratorMock
        spreadsheet_id = mg._create_blank_manifest(title='Example')

        # set permission
        mg._set_permissions(spreadsheet_id)

        # now retrieve permission
        permission = mg.drive_service.permissions().list(fileId=spreadsheet_id).execute()
        for info in permission["permissions"]: 
            if info["id"] == "anyoneWithLink":
                assert info["type"] == "anyone"
                assert info["role"] == "writer"










        


