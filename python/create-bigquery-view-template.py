"""Creates a bigquery dataset."""
# gcloud deployment-manager deployments create bigquery-test-view --template create-bigquery-view-template.py --properties streamId:ua123456789

def AlphaNum(stream):
  return "".join([ c if c.isalnum() else "" for c in stream ])

def GenerateConfig(context):
  """Generate configuration."""

  resources = []

# Event view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-event', #bigquery-dataset-ua123456789-view-event
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'event'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "eventCategory") as eventCategory,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "eventAction") as eventAction,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "eventLabel") as eventLabel,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "eventValue") as eventValue,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "url") as landingUrl,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "host") as landingHost,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "path") as landingPath
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "event"''',
              'useLegacySql': False
          }
      }
  })

# Exception view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-exception', #bigquery-dataset-ua123456789-view-exception
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'exception'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "exceptionDescription") as exceptionDescription,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "exceptionFatal") as exceptionFatal,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "url") as landingUrl,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "host") as landingHost,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "path") as landingPath
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "exception"''',
              'useLegacySql': False
          }
      }
  })

# Impression view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-impression', #bigquery-dataset-ua123456789-view-impression
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'impression'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productListName") as productListName,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productSku") as productSku,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productName") as productName,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productBrand") as productBrand,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productCategory") as productCategory,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productVariant") as productVariant,
(SELECT param.value.floatValue FROM UNNEST(params) param WHERE param.key = "productPrice") as productPrice,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "productPosition") as productPosition
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "productImpression"''',
              'useLegacySql': False
          }
      }
  })

# Impression view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-pageview', #bigquery-dataset-ua123456789-view-pageview
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'pageview'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "url") as url,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "host") as host,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "path") as path
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "pageview"''',
              'useLegacySql': False
          }
      }
  })

# Product view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-product', #bigquery-dataset-ua123456789-view-product
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'product'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productSku") as productSku,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productName") as productName,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productBrand") as productBrand,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productCategory") as productCategory,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productVariant") as productVariant,
(SELECT param.value.floatValue FROM UNNEST(params) param WHERE param.key = "productPrice") as productPrice,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "productQuantity") as productQuantity,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productCouponCode") as productCouponCode,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "productPosition") as productPosition,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productAction") as productAction,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "productActionList") as productActionList,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "transactionId") as transactionId,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "checkoutStep") as checkoutStep,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "checkoutStepOption") as checkoutStepOption
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type IN ("product_detail", "product_click", "product_add", "product_remove", "product_checkout", "product_purchase", "product_refund")''',
              'useLegacySql': False
          }
      }
  })

# Promotion view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-promotion', #bigquery-dataset-ua123456789-view-promotion
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'promotion'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "promotionId") as promotionId,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "promotionName") as promotionName,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "promotionCreative") as promotionCreative,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "promotionPosition") as promotionPosition,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "promotionAction") as promotionAction
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "promotion"''',
              'useLegacySql': False
          }
      }
  })

# Search view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-search', #bigquery-dataset-ua123456789-view-search
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'search'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "siteSearchTerm") as siteSearchTerm,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "siteSearchURL") as siteSearchURL,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "siteSearchPath") as siteSearchPath
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "siteSearch"''',
              'useLegacySql': False
          }
      }
  })

# Social view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-social', #bigquery-dataset-ua123456789-view-social
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'social'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "socialNetwork") as socialNetwork,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "socialAction") as socialAction,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "socialActionTarget") as socialActionTarget
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "social"''',
              'useLegacySql': False
          }
      }
  })

# Timing view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-timing', #bigquery-dataset-ua123456789-view-timing
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'timing'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "userTimingCategory") as userTimingCategory,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "userTimingVariableName") as userTimingVariableName,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "userTimingTime") as userTimingTime,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "userTimingLabel") as userTimingLabel,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "pageLoadTime") as pageLoadTime,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "dnsTime") as dnsTime,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "pageDownloadTime") as pageDownloadTime,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "redirectResponseTime") as redirectResponseTime,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "tcpConnectTime") as tcpConnectTime,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "serverResponseTime") as serverResponseTime,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "domInteractiveTime") as domInteractiveTime,
(SELECT param.value.intValue FROM UNNEST(params) param WHERE param.key = "contentLoadTime") as contentLoadTime
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "timing"''',
              'useLegacySql': False
          }
      }
  })

# Traffic view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-traffic', #bigquery-dataset-ua123456789-view-traffic
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'traffic'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "campaignName") as campaignName,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "campaignSource") as campaignSource,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "campaignMedium") as campaignMedium,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "campaignContent") as campaignContent,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "campaignKeyword") as campaignKeyword,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "campaignId") as campaignId,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "googleAdwordsId") as googleAdwordsId,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "googleDisplayId") as googleDisplayId,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "referer") as referer,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "refererHost") as refererHost,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "refererPath") as refererPath,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "url") as landingUrl,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "host") as landingHost,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "path") as landingPath
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "traffic"''',
              'useLegacySql': False
          }
      }
  })

# Traffic view
  resources.append({
      'name': 'bigquery-dataset-' + AlphaNum(context.properties['streamId']) + '-view-transaction', #bigquery-dataset-ua123456789-view-transaction
      'type': 'bigquery.v2.table',
      'properties': {
          'datasetId': AlphaNum(context.properties['streamId']),
          'tableReference': {
              'projectId': context.env["project"],
              'datasetId': AlphaNum(context.properties['streamId']),
              'tableId': 'transaction'
          },
          'view': {
              'query': '''
SELECT 
type,
clientId,
userId,
epochMillis,
date,
FORMAT_TIMESTAMP("%X", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as time,
FORMAT_TIMESTAMP("%G", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as year,
FORMAT_TIMESTAMP("%m", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as month,
FORMAT_TIMESTAMP("%d", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as day,
FORMAT_TIMESTAMP("%H", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as hour,
FORMAT_TIMESTAMP("%W", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as week,
FORMAT_TIMESTAMP("%w", TIMESTAMP_MILLIS(epochMillis), "Europe/Stockholm") as weekday,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "transactionId") as transactionId,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "affiliation") as affiliation,
(SELECT param.value.floatValue FROM UNNEST(params) param WHERE param.key = "revenue") as revenue,
(SELECT param.value.floatValue FROM UNNEST(params) param WHERE param.key = "tax") as tax,
(SELECT param.value.floatValue FROM UNNEST(params) param WHERE param.key = "shipping") as shipping,
(SELECT param.value.stringValue FROM UNNEST(params) param WHERE param.key = "couponCode") as couponCode
FROM `'''+ context.env["project"] + '.' + AlphaNum(context.properties['streamId']) + '''.entities`
WHERE type = "transaction"''',
              'useLegacySql': False
          }
      }
  })

  return {'resources': resources}