"""Creates pubsub topics and subscriptions for a new google analytics property."""
# gcloud deployment-manager deployments create aws-kinesis-order-stream --template add-streaming-source.py --properties streamId:order-stream,location:EU
# gcloud deployment-manager deployments update aws-kinesis-order-stream --template add-streaming-source.py --properties streamId:order-stream,location:EU
# gcloud deployment-manager deployments create ga-property-ua-1234567-89 --template add-streaming-source.py --properties streamId:UA-1234567-89,location:EU
# gcloud deployment-manager deployments update ga-property-ua-1234567-89 --template add-streaming-source.py --properties streamId:UA-1234567-89,location:EU

def AlphaNum(stream):
  return "".join([ c if c.isalnum() else "" for c in stream ])

def GenerateConfig(context):
  """Generate configuration."""

  resources = []

# BigQuery dataset to store entities from processor pipeline
  resources.append({
      'name': 'bigquery-dataset-' + context.properties['streamId'] + '-entities', #bigquery-dataset-order-stream-entities
      'type': 'bigquery.v2.dataset',
      'properties': {
          'datasetReference': {
              'datasetId': AlphaNum(context.properties['streamId'])
          },
          'location': context.properties['location']
      }
  })

  return {'resources': resources}