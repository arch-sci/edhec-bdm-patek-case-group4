install:
	@pip install -e .

# Runs the main extraction
data:
	@python -m patek_analysis.data

# Runs the ML bonus
model:
	@python -m patek_analysis.model

# Runs the API extraction 
fx:
	@python -m patek_analysis.fx_rates