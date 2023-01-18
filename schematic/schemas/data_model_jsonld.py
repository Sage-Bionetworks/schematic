class DataModelJsonLD(object):
	'''
	Interface to JSONLD_object
	'''

	def __init__(data_model_graph):


	def generate_data_model_jsonld(self):
		'''
		Will call JSONLD_object class to create properties and classes in the process.
		'''
		pass

class JSONLD_object():
	'''
	Decorator class design
	Base decorator class.
	'''
	_template: template = None

	def __init__(self, to_template) -> None:
		self.to_template()

	def _create_template(self):
		'''
		Returns jsonld_class_template or jsonld_property_template
		'''
		return self._template

	@property
	def to_template(self):
		return self._template.to_template()

class JSONLD_property(JSONLD_object):
	'''
	Property Decorator
	'''
	def to_template(self)
		return JSONLD_property(self._template.to_template())

	def explore_property():
		return

	def edit_property():
		return

class JSONLD_class(JSONLD_object):
	'''
	Class Decorator
	'''
	def to_template(self)
		return JSONLD_class(self._template.to_template())

	def explore_class():
		return

	def edit_class():
		return
