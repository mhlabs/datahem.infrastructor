"""Creates pubsub topics and subscriptions for a new google analytics property."""
# gcloud deployment-manager deployments create processor-bucket --template create-bucket-template.py --properties location:europe-west1,suffix:-processor --preview
# gcloud deployment-manager deployments update processor-bucket --template create-bucket-template.py --properties location:europe-west1,suffix:-processor --preview

def GenerateConfig(context):
  """Generate configuration."""

  resources = []

# template to enable api
  resources.append({
      'name': context.env['project'] + context.properties['bucketSuffix'],
      #- type: storage.v1.bucket
      'type': 'gcp-types/storage-v1:buckets',
      'properties': {
          'predefinedAcl': 'projectPrivate',
          'projection': 'full',
          'location': context.properties['location'],
          'storageClass': 'STANDARD'
      }
  })

  return {'resources': resources}