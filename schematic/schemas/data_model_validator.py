import networkx as nx

class DataModelValidator():
	'''
	Check for consistency within data model.
	'''
	def __init__(
		self,
		data_model,
		):
		self.data_model = data_model

	def run_checks(self):
		checks = [
					self.check_has_name(),
					self.check_is_dag(),
					self.check_namespace_overlap(),
					self.check_for_orphan_attributes(),
					self.check_namespace_similarity(),
					]
		errors = [error for error in checks if error]
		return errors

	def check_has_name(self):
		'''Checks that each node is assigned a label.
		'''
		error = []

		# Check that nodes have labels
		node_labels = nx.get_node_attributes(self.data_model, "label")
		for k, v in node_labels.items():
			if not v:
				error.append(f'Node {k} does not have a label attached.')
				breakpoint()
		return error

	def check_is_dag(self):
		'''
		TODO:
			- Check with Milen. This might be too simple of a check.
			- Try wit topological sort as well. Benchmark against current approach.
			- Add unit test to verify this works properly.

		'''
		if nx.number_of_selfloops(self.data_model)!=0 and nx.is_directed(self.data_model) == False:
			error = f'Schematic requires that data models are Directed Acyclic Graphs (DAGs). ' \
					f'Model supplied is not a DAG, please check your model.'
			return error


	def check_namespace_overlap(self):
		'''
		Check if name is repeated.
		TODO:
			- Add unit test to verify this works properly.
			- The way this looks, it wont find namespace overlaps,
			Have to go back to loading the csv and looking before overlaps have been removed.
			Look for duplicate attributes.
			Look for valid values that overlap with attributes and flag.
		'''
		error = []
		if len(self.data_model.nodes.keys()) != set(list(self.data_model.nodes.keys())):
			all_node_names = list(self.data_model.nodes.keys())
			for n_name in self.data_model.nodes.keys():
				all_node_names = [i for i in all_node_names if i != n_name]
				if n_name in all_node_names:
					error.append(f'There appears to be a namespace overlap, {n_name} appears at least twice.')
		
		return error

	def check_for_orphan_attributes(self):
		error = []
		return error

	def check_namespace_similarity(self):
		""" Checks to see if names are incredibly similar save for formatting. Raise warning not error.
		"""
		error=[]
		return error

	def check_required_filled(self):
		return