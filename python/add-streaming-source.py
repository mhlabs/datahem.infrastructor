"""Creates pubsub topics and subscriptions for a new google analytics property."""
# gcloud deployment-manager deployments create aws-kinesis-orderstream --template add-streaming-source.py --properties streamId:orderstream,location:EU
# gcloud deployment-manager deployments create ga-property-ua123456789 --template add-streaming-source.py --properties streamId:ua123456789,location:EU

def AlphaNum(stream):
  return "".join([ c if c.isalnum() else "" for c in stream ]).lower()

def GenerateConfig(context):
  """Generate configuration."""

  resources = []

# topic used by collector to push measurement protocol payloads
  resources.append({
      'name': 'pubsub-topic-' + AlphaNum(context.properties['streamId']), #pubsub-topic-ua123456789
      'type': 'pubsub.v1.topic',
      'properties': {
          'topic': AlphaNum(context.properties['streamId']) #ua123456789
      }
  })

# subscription used to backup payloads to bigquery
  resources.append({
      'name': 'pubsub-subscription-' + AlphaNum(context.properties['streamId']) + '-backup', #pubsub-subscription-ua123456789-backup
      'type': 'pubsub.v1.subscription',
      'properties': {
          'subscription': AlphaNum(context.properties['streamId']) + '-backup', #ua123456789-backup
          'topic': '$(ref.pubsub-topic-' + AlphaNum(context.properties['streamId']) + '.name)' #ua123456789
      }
  })

# subscription used for processor pipeline that transforms measurement protocol payloads to events
  resources.append({
      'name': 'pubsub-subscription-' + AlphaNum(context.properties['streamId']) + '-processor', #pubsub-subscription-ua123456789-procssor
      'type': 'pubsub.v1.subscription',
      'properties': {
          'subscription': AlphaNum(context.properties['streamId']) + '-processor', #ua123456789-processor
          'topic': '$(ref.pubsub-topic-' + AlphaNum(context.properties['streamId']) + '.name)' #ua123456789
      }
  })

# topic used by processor pipeline to push events as stream
  resources.append({
      'name': 'pubsub-topic-' + AlphaNum(context.properties['streamId']) + '-entities', #pubsub-topic-ua123456789-entities
      'type': 'pubsub.v1.topic',
      'properties': {
          'topic': AlphaNum(context.properties['streamId'])+ '-entities' #ua123456789-entities
      }
  })

# BigQuery dataset to store entities from processor pipeline
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-entities', #bigquery-dataset-ua123456789-entities
      'type': 'bigquery.v2.dataset',
      'properties': {
          'datasetReference': {
              'datasetId': AlphaNum(context.properties['streamId'])
          },
          'location': context.properties['location']
      }
  })

  return {'resources': resources}