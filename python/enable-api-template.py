"""Creates pubsub topics and subscriptions for a new google analytics property."""
# gcloud deployment-manager deployments create enable-pubsub-apis --template enable-api-template.py --properties api:pubsub.googleapis.com --preview
# gcloud deployment-manager deployments update enable-pubsub-apis --template enable-api-template.py --properties api:pubsub.googleapis.com --preview

def GenerateConfig(context):
  """Generate configuration."""

  resources = []

# template to enable api
  resources.append({
      'name': context.env['project'] + '-enable-' + context.properties['api'],
      'type': 'deploymentmanager.v2.virtual.enableService',
      'properties': {
          'consumerId': 'project:' + context.env['project'] ,
          'serviceName': context.properties['api']
      }
  })

  return {'resources': resources}