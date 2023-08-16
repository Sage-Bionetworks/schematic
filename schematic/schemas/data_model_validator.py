import networkx as nx

from schematic.schemas.data_model_relationships import (
	DataModelRelationships
	)

class DataModelValidator():
	'''
	Check for consistency within data model.
	'''
	def __init__(
		self,
		graph,
		):
		'''
		TODO: put blacklisted chars and reserved_names in some global space where they can be accessed centrally
		'''
		self.graph = graph
		self.DMR = DataModelRelationships()
		# Removed check for spaces in display name since we get rid of those.
		self.blacklisted_chars = ['(', ')', '.', '-']
		self.reserved_names = {'entityId'}

	def run_checks(self):
		error_checks = [
					self.check_graph_has_required_node_fields(),
					self.check_is_dag(),
					self.check_reserved_names()
					]
		warning_checks = [
					self.check_blacklisted_characters(),
					]
		errors = [error for error in error_checks if error]
		warnings = [warning for warning in warning_checks if warning]
		return errors

	def check_graph_has_required_node_fields(self):
		'''
		Checks that each node is assigned a label.
		'''
		# Get all the fields that should be recorded per node
		rel_dict = self.DMR.relationships_dictionary
		node_fields = []
		for k, v in rel_dict.items():
			if 'node_label' in v.keys():
				node_fields.append(v['node_label'])

		error = []
		missing_fields = []
		# Check that nodes have labels
		for node, node_dict in self.graph.nodes(data=True):
			missing_fields.extend([(node, f) for f in node_fields if f not in node_dict.keys()])

		if missing_fields:
			for nf in missing_fields:
				error.append(f'For entry: {nf[0]}, the required field {nf[1]} is missing in the data model graph, please double check your model and generate the graph again.')
		return error

	def check_is_dag(self):
		'''
		TODO:
			- Check with Milen. This might be too simple of a check.
			- Try wit topological sort as well. Benchmark against current approach.
			- Add unit test to verify this works properly.

		
		if nx.number_of_selfloops(self.graph)!=0 and nx.is_directed(self.graph) == False:
			error = f'Schematic requires that data models are Directed Acyclic Graphs (DAGs). ' \
					f'Model supplied is not a DAG, please check your model.'
			return error
		'''
		error = []
		if not nx.is_directed_acyclic_graph(self.graph):
			# Attempt to find any cycles:
			cycles = nx.simple_cycles(self.graph)
			if cycles:
				for cycle in cycles:
					error.append(f'Schematic requires models be a directed acyclic graph (DAG). Your graph is not a DAG, we found a loop between: {cycle[0]} and {cycle[1]}, please remove this loop from your model and submit again.')
			else:
				error.append(f'Schematic requires models be a directed acyclic graph (DAG). Your graph is not a DAG, we could not locate the sorce of the error, please inspect your model.')
		return error

	def check_blacklisted_characters(self):
		""" We strip these characters in store, so not sure if it matter if we have them now, maybe add warning 
		"""
		warning = []
		for node, node_dict in self.graph.nodes(data=True):
			if any(bl_char in node_dict['displayName'] for bl_char in self.blacklisted_chars):
				node_display_name = node_dict['displayName']
				blacklisted_characters_found = [bl_char for bl_char in self.blacklisted_chars if bl_char in node_dict['displayName'] ]
				blacklisted_characters_str= ','.join(blacklisted_characters_found)
				warning.append(f'Node: {node_display_name} contains a blacklisted character(s): {blacklisted_characters_str}, they will be striped if used in Synapse annotations.')
		return warning

	def check_reserved_names(self):
		'''
		# TODO: the error message is odd, what are the actual names that should be used? Not attribute or componenet...
		'''
		error = []
		reserved_names_found = [(name, node) for node in self.graph.nodes
											 for name in self.reserved_names
											 if name.lower() == node.lower()
								]
		if reserved_names_found:
			for reserved_name, node_name in reserved_names_found:
				error.append(f'Your data model entry name: {node_name} overlaps with the reserved name: {reserved_name}. Please change this name in your data model.')
		return error


	def check_namespace_overlap(self):
		'''
		Check if name is repeated.
		Implement in the future
		'''
		warning = []
		return warning

	def check_for_orphan_attributes(self):
		'''
		Check if attribute is specified but not connected to another attribute or component.
		Implement in future
		'''
		warning = []
		return warning

	def check_namespace_similarity(self):
		""" 
		Using AI, check if submitted attributes or valid values are similar to other ones, warn users.
		Implement in future
		"""
		warning=[]
		return warning
