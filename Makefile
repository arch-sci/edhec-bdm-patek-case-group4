# Setup
install:
	@pip install -e .

# case 1: "Run the full pipeline"
# Fetches data -> Updates FX -> Trains Model -> Cleans up
# we want the local CSV to contain the price_EUR column, we need to run the fx step before the data step in your Makefile
pipeline: fx data model
	@echo "🚀 Full pipeline complete. Check your CSV files."

data:
	@python -m patek_analysis.data

fx:
	@python -m patek_analysis.fx_rates

model:
	@python -m patek_analysis.model

# case 2: "I want to analyze in a Notebook"
# Starts Jupyter inside the container, accessible at localhost:8888
notebook:
	@jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''

# case 3: "Reset everything"
clean:
	@rm -f *.csv
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} +
	@echo "🧹 Cleaned workspace."