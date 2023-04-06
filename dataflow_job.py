import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

# Define the pipeline options
options = PipelineOptions()
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'
options.view_as(beam.options.pipeline_options.StandardOptions).project = 'finalproject-382922'
options.view_as(beam.options.pipeline_options.StandardOptions).job_name = 'dataflow-job'
options.view_as(beam.options.pipeline_options.StandardOptions).region = 'us-central1'
options.view_as(beam.options.pipeline_options.StandardOptions).temp_location = 'gs://finalproject-382922-bucket/tmp'
options.view_as(beam.options.pipeline_options.StandardOptions).staging_location = 'gs://finalproject-382922-bucket/staging'

# Define a custom ParDo transform to drop rows with missing or empty values
class DropEmptyRows(beam.DoFn):
    def process(self, element):
        if all(val.strip() != '' for val in element.split(',')):
            yield element

# Define a custom ParDo transform to normalize the data
class NormalizeData(beam.DoFn):
    def process(self, element):
        data = element.split(',')
        # Example of normalizing x and y columns by dividing them by 1000
        data[2] = str(float(data[2]) / 1000)
        data[3] = str(float(data[3]) / 1000)
        yield ','.join(data)

# Define a custom ParDo transform to select relevant features for model training
class SelectFeatures(beam.DoFn):
    def process(self, element):
        data = element.split(',')
        # Example of selecting relevant columns for traffic prediction
        selected_data = [drivingDirection[7], traveledDistance[8], minXVelocity[9], maxXVelocity[10], meanXVelocity[11], minDHW[12], minTHW[13], minTTC[14],numLaneChanges[15]]
        yield ','.join(selected_data)

# Define a custom ParDo transform to convert data into key-value pairs for model training
class ConvertToKeyValuePairs(beam.DoFn):
    def process(self, element):
        data = element.split(',')
        key = data[0]
        value = ','.join(data[1:])
        yield (key, value)

# Define a custom ParDo transform to split data into train and test sets
class SplitData(beam.DoFn):
    def process(self, element):
        key, value = element
        train_data, test_data = train_test_split(value, test_size=0.2, random_state=42)
        yield ('train', ','.join([key, train_data]))
        yield ('test', ','.join([key, test_data]))

# Define a custom ParDo transform to train a linear regression model
class TrainModel(beam.DoFn):
    def process(self, element):
        key, value = element
        X_train, y_train = [], []
        for line in value.split('\n'):
            data = line.split(',')
            X_train.append(list(map(float, data[1:])))
            y_train.append(float(data[0]))
        model = LinearRegression()
        model.fit(X_train, y_train)
        yield (key, model)

# Define a custom ParDo transform to make predictions using the trained model
class MakePredictions(beam.DoFn):
    def process(self, element, model):
        key, value = element
        X_test, y_test = [], []
        for line in value.split('\n'):
            data = line.split(',')
            X_test.append(list(map(float, data[1:])))
	y_test.append(float(data[0]))
	y_pred = model.predict(X_test)
	yield (key, (y_test, y_pred))

def run_pipeline():
p = beam.Pipeline(options=options)

# Read input data
input_data = (p | 'ReadData' >> beam.io.ReadFromText('gs://finalproject-382922-bucket/input_data.csv'))

# Drop empty rows
drop_empty_rows = (input_data | 'DropEmptyRows' >> beam.ParDo(DropEmptyRows()))

# Normalize data
normalized_data = (drop_empty_rows | 'NormalizeData' >> beam.ParDo(NormalizeData()))

# Select relevant features
selected_features = (normalized_data | 'SelectFeatures' >> beam.ParDo(SelectFeatures()))

# Convert data into key-value pairs
key_value_pairs = (selected_features | 'ConvertToKeyValuePairs' >> beam.ParDo(ConvertToKeyValuePairs()))

# Split data into train and test sets
train_test_data = (key_value_pairs | 'SplitData' >> beam.ParDo(SplitData()))

# Group train and test data by key
grouped_data = (train_test_data | 'GroupByKey' >> beam.GroupByKey())

# Train model
trained_models = (grouped_data | 'TrainModel' >> beam.ParDo(TrainModel()))

# Make predictions
predictions = (trained_models | 'MakePredictions' >> beam.ParDo(MakePredictions()))

# Write predictions to output
(predictions | 'WritePredictions' >> beam.io.WriteToText('gs://finalproject-382922-bucket/predictions'))

# Run the pipeline
result = p.run()
result.wait_until_finish()

