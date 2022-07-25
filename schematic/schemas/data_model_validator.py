class DataModelValidator():
	'''
	Check for consistency within data model.
	'''
	def __init__(
		data_model,
		run_all_checks: bool = True,
		):
		data_model = self.data_model
		if run_all_checks:
			'''
			If there are errors log them.
			'''
			errors = self.run_checks(data_model)

	def run_checks(self):
		checks = [
					self.check_has_name(),
					self.check_is_dag(),
					self.check_json_(),
					self.check_name_is_valid(),
					self.check_name_overlap()
					]
		errors = [error for check in checks]
		return errors

	def check_has_name(self):
		error = None
		return error

	def check_is_dag(self):
		error = None
		return

	def check_json(self):
		'''
		Standard JSON validation.
		'''
		error = None
		return

	def check_name_is_valid(self):
		error = None
		return

	def check_name_overlap(self):
		'''
		Check if name is repeated in a valid value
		'''
		error = None
		return