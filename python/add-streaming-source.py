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

# topic used by collector to push measurement protocol payloads
  resources.append({
      'name': 'pubsub-topic-' + context.properties['streamId'], #pubsub-topic-order-stream
      'type': 'pubsub.v1.topic',
      'properties': {
          'topic': context.properties['streamId'] #order-stream
      }
  })

# subscription used to backup payloads to bigquery
  resources.append({
      'name': 'pubsub-subscription-' + context.properties['streamId'] + '-backup', #pubsub-subscription-order-stream-backup
      'type': 'pubsub.v1.subscription',
      'properties': {
          'subscription': context.properties['streamId'] + '-backup', #order-stream-backup
          'topic': '$(ref.pubsub-topic-' + context.properties['streamId'] + '.name)' #order-stream
      }
  })

# subscription used for processor pipeline that transforms measurement protocol payloads to events
  resources.append({
      'name': 'pubsub-subscription-' + context.properties['streamId'] + '-processor', #pubsub-subscription-order-stream-procssor
      'type': 'pubsub.v1.subscription',
      'properties': {
          'subscription': context.properties['streamId'] + '-processor', #order-stream-processor
          'topic': '$(ref.pubsub-topic-' + context.properties['streamId'] + '.name)' #order-stream
      }
  })

# topic used by processor pipeline to push events as stream
  resources.append({
      'name': 'pubsub-topic-' + context.properties['streamId'] + '-entities', #pubsub-topic-order-stream-entities
      'type': 'pubsub.v1.topic',
      'properties': {
          'topic': context.properties['streamId']+ '-entities' #order-stream-entities
      }
  })

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