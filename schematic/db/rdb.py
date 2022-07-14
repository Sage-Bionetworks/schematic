from typing import Any, Dict, Optional, Text, List
from os import path

import logging

import networkx as nx

from schematic import schemas
from schematic.schemas.explorer import SchemaExplorer
from schematic.schemas.generator import SchemaGenerator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class RDB(object):

    def __init__(self,
                 path_to_json_ld: str = None,
                 requires_component_relationship: str = "requiresComponent"
                 ) -> None:

        """Create / Initialize object of type RDB (Relation DataBase) .

        Methods that are part of this module can be used to generate generic relational database table objects
        based on a provided JSON-LD schema.org schema specification.

        Args:
            path_to_json_ld: Path to the JSON-LD file that is representing a schema.org data model.
            requires_component_relationship: dependency relationship between two nodes A, B;
            A requires B is interpreted as 'B is 1-to-many A'. By convention, schema properties in the format 'B_id' and 'A_id' 
            are interpreted as primary keys of B and A respectively; B_id acts as a foreign key in table A.
            Note: foreign/primary keys formats follow schema.org property label format (camelCase_Id) 
        Returns:
            None
        """
        self.path_to_json_ld = path_to_json_ld
        self.schema_name = path.basename(self.path_to_json_ld).split(".rdb.model.jsonld")[0]

        self.requires_component_relationship = requires_component_relationship

        # instantiate a schema generator to retrieve db schema graph from metadata model graph
        self.sg = SchemaGenerator(
                    self.path_to_json_ld, 
                    requires_component_relationship = requires_component_relationship
        )

        # get metadata model schema graph
        mm_graph = self.sg.se.get_nx_schema()
       
        # the set of tables in the RDB schema corresponds includes the set of components in the metadata model schema
        # the set of components in the metadata model schema is defined as the set of Valid Values of the 
        # 'Component' attribute in the schema
        # if 'Component' exists, get its corresponding set of tables 
        self.table_labels = self.sg.get_node_range('Component', display_names = False)
        
        
        # instantiate db schema graph
        # get subgraph on requires_component edges (to infer relationships between tables in db schema graph)
        # note that all nodes in the graph are added to the set of relational tables in the db in addition
        # to the tables specified in the 'Component' attribute
        self.db_schema_graph = self.sg.get_subgraph_by_edge_type(mm_graph, self.requires_component_relationship)
        self.table_labels.extend(self.db_schema_graph.nodes())

        # get a set of DB tables
        self.tables = self.generate_tables()

    def generate_tables(self) -> Dict:

        """Generate a set of tables in the format
        {
            'table1':{
                        'attributes':{
                                'attribute1':{'type':'TEXT'},
                                'attribute2':{'type':'TEXT'}',
                                ...
                        }
                        'primary_key':'pk',
                        'foreign_keys':['fk1', 'fk2']
            },
            'table2':{
                ...
            }
            ...
        }

        TODO: support multiple PKs 

        Returns: A dictionary of tables
        """
        
        tables = {}

        # iterate over tables and retrieve corresponding table properties 
        for table_label in self.table_labels:
            # get table attributes; assume that the set of table attributes
            # corresponds to the properties of metadata model schema class 
            # there should be at least one class property ow table will not be created
            # assume the primary key, if any, is a class property of the form <tableLabel>_id

            table_attributes = self.sg.se.find_class_specific_properties(table_label)

            if not table_attributes:
                continue
            
            # process table attributes (e.g. extract type)
            # TODO define rdb compatible primitive attribute types in metadata model schema
            # for now assume all attributes are string; abstract a get_table method

            attributes = {}
            primary_key = self.sg.se.get_property_label_from_display_name(table_label+'_id', strict_camel_case = True)

            # get foreign keys based on db schema graph 
            foreign_keys = self.get_table_foreign_keys(table_label)
            foreign_keys.extend(self.get_additional_foreign_keys(table_label))

            # set the schema for a set of table attributes
            for attr in set(table_attributes):
                attributes[attr] = {'type':'TEXT'}

            table = {
                        'attributes': attributes,
                        'primary_key': primary_key,
                        'foreign_keys': foreign_keys
            }

            tables[table_label] = table
        logger.debug("Instantiated tables in RDB model: ")
        logger.debug(tables)

        return tables

    def get_primary_key_table_from_id(self, dependency_id):
        pk_table_name = dependency_id.capitalize().replace('_id', '')
        return pk_table_name

    def get_additional_foreign_keys(self, table_label):
        """Find foreign keys with alternate names than table names.
        These are assumed to have a 'depends on' that only contains the
        primary key name. 
        Should allow reference of FK to PK in same table.
        Should allow reference of FK with name not matching the PK in another table.
        
        Returns:
        ['Donor.parentDonorId']
        Will match the FK parentDonorId to the PK in the Donor table?
        """
        table_attributes = self.sg.se.find_class_specific_properties(table_label)
        foreign_keys = []
        for attr in table_attributes:
            dependencies = self.sg.get_node_dependencies(attr)
            if len(dependencies) == 1 and '_id' in dependencies[0]:
                foreign_keys.append(
                    self.get_primary_key_table_from_id(dependencies[0])
                    + "." + attr
                )
        return foreign_keys


    def get_table_foreign_keys(self, table_label:str, table_prefix:bool = True) -> List[str]:
        
        """ Given the db schema graph, infer foreign keys between tables; 
        if edge (A, B) exists between tables A and B in the schema DB graph 
        (A requires B in metadata model json-ld schema) then B_id is assumed to
        be a foreign key in A; note that A might have multiple foreign keys. 
        
        Args:
            table_label: name of table to get foreign keys
            table_prefix: if true include foreign key source table as prefix to key attribute; ow return attribute
        Returns:
            An ordered list of *all* table labels to be updated
        """
        connected_tables = self.db_schema_graph.neighbors(table_label)

        if table_prefix:
            foreign_keys = [ct + "." + self.sg.se.get_property_label_from_display_name(ct + '_id', strict_camel_case = True) for ct in connected_tables]
        else:
            foreign_keys = [self.sg.se.get_property_label_from_display_name(ct + '_id', strict_camel_case = True) for ct in connected_tables]
         
        return foreign_keys


    def get_attr_from_fk(self, fk:str) -> str:

        """ given a FK strip the table prefix and return column name

        Args:
            fk: foreign key containing a prefix

        Returns:
            Column name corresponding to FK w/o table prefix
        """
        
        fk_col = fk.split(".")[1]

        return fk_col
        

    def get_tables_update_order(self) -> List[str]:
        
        """Order tables so that if A requires component B (i.e. B is one-to-many A), then 
        table B is updated before table A. This is equivalent of a reversed topological 
        sort of the db schema graph
        
        Returns:
            An ordered list of *all* table labels to be updated
        """

        return list(reversed(list(nx.topological_sort(self.db_schema_graph)))) 


    def get_property_labels_from_table_attrs(self, attributes: list) -> Dict:
        """Given a set of table attributes (i.e. column headers) convert them to 
        schema property labels.

        Args:
           attributes: a list of string attributes (i.e. column headers from a dataframe) 
        Returns:
            A dictionary of schema property labels corresponding to attributes
        """
        attr_pl = {attr:self.sg.se.get_property_label_from_display_name(attr, strict_camel_case = True) for attr in attributes}

        return attr_pl


    def get_target_update_tables(self, attributes) -> List[str]:

        """Given a set of attributes (e.g. columns in input table) and the RDB schema graph,
        find all tables targeted for update (i.e. set of tables where each table contains at least one
        attribute from the set of input attributes); return tables in update order ensuring 
        join tables are updated after entity tables are updated; this may not be necessary for most 
        usecases
        
        Returns:
            An ordered list of matching target table labels to be updated
        """

        # ensure attribute names are converted to schema property labels
        attributes = self.get_property_labels_from_table_attrs(attributes).values()

        # get the set of tables that need to be updated
        target_tables = []
        for table_label in self.tables.keys():

            # if a table has a column-set that's part of the provided attributes and the provided attributes include the primary key of this table, then target the table for update
            if not set(attributes).isdisjoint(set(self.tables[table_label]['attributes'].keys())) and self.tables[table_label]['primary_key'] in attributes:
                target_tables.append(table_label)

        # get all db tables in the order they must be updated
        all_ordered_tables = self.get_tables_update_order()

        # sort target tables in the order they must be updated
        target_tables = sorted(target_tables, key = lambda x: all_ordered_tables.index(x))

        return target_tables
