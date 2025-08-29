# Starburst Enterprise API Documentation

## Authentication

The Starburst Enterprise API uses Basic Authentication. All requests must include an Authorization header:

```
Authorization: Basic [basicHash]
```

Where `[basicHash]` is the Base64-encoded string of `username:password`.

### Python Flask Implementation

```python
import base64
import requests

def get_auth_header(username, password):
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded_credentials}"}

# Usage in Flask routes
headers = get_auth_header("your_username", "your_password")
response = requests.get(f"{base_url}/api/v1/endpoint", headers=headers)
```

## Base URL Structure

All API endpoints follow the pattern: `{base_url}/api/v{version}/{category}/{resource}`

- Version 1 endpoints: `/api/v1/`
- Internal endpoints: `/api/internal/v1/`

---

## Audit Logs

### List Access Logs

**Endpoint:** `GET /api/v1/biac/audit/accessLogs`

List Starburst built-in access control access logs.

#### Parameters

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order
- `startDate` (date-time): Start of the log entry timestamp interval to match (inclusive)
- `endDate` (date-time): End of the log entry timestamp interval to match (exclusive)
- `queryId` (date-time): Query ID for which log entries should be matched

#### Response Schema

**200 - Success**
```json
{
  "nextPageToken": "string",
  "count": 0,
  "result": [
    {
      "id": 0,
      "queryId": "string",
      "action": "string",
      "entityCategory": "string",
      "entity": "string",
      "entitySpecified": true,
      "grantOption": true,
      "accessResult": "ALLOW|DENY|ROW_FILTER|COLUMN_MASK",
      "user": "string",
      "enabledRoles": ["string"],
      "atTime": "2023-01-01T00:00:00Z"
    }
  ]
}
```

### List Change Logs

**Endpoint:** `GET /api/v1/biac/audit/changeLogs`

List Starburst built-in access control change logs.

#### Parameters

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order
- `startDate` (date-time): Start of the log entry timestamp interval to match (inclusive)
- `endDate` (date-time): End of the log entry timestamp interval to match (exclusive)
- `queryId` (date-time): Query ID for which log entries should be matched

#### Response Schema

**200 - Success**
```json
{
  "nextPageToken": "string",
  "count": 0,
  "result": [
    {
      "id": 0,
      "queryId": "string",
      "operation": "string",
      "entityKind": "string",
      "whatChanged": "string",
      "entity": "string",
      "entitySpecified": true,
      "grantOption": true,
      "affectedRole": "string",
      "user": "string",
      "enabledRoles": ["string"],
      "atTime": "2023-01-01T00:00:00Z"
    }
  ]
}
```

---

## Column Masks

### Add Column Mask to Role

**Endpoint:** `POST /api/v1/biac/roles/{roleId}/columnMasks`

Add a column mask to a role to mask column values from subjects assigned to the role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

**Request Body:**
```json
{
  "entity": {
    "category": "TABLES",
    "allEntities": false,
    "catalog": "string",
    "schema": "string",
    "table": "string",
    "tableType": "TABLE|VIEW|MATERIALIZED_VIEW",
    "columns": ["string"]
  },
  "expressionId": 0
}
```

#### Response Schema

**200 - Success**
```json
{
  "id": 0,
  "entity": {
    "category": "string",
    "allEntities": false
  },
  "expressionId": 0,
  "forceNone": false
}
```

### Create Column Mask Expression

**Endpoint:** `POST /api/v1/biac/expressions/columnMask`

Create an expression that can be applied to mask column values.

#### Request Body

```json
{
  "name": "string",
  "expression": "string",
  "description": "string"
}
```

#### Response Schema

**200 - Success**
```json
{
  "id": 0,
  "name": "string",
  "expression": "string",
  "description": "string"
}
```

### Delete Column Mask Expression

**Endpoint:** `DELETE /api/v1/biac/expressions/columnMask/{columnMaskExpressionId}`

Delete a column mask expression.

#### Parameters

**Path Parameters:**
- `columnMaskExpressionId` (integer, required): Column mask expression ID

### Delete Role Column Mask

**Endpoint:** `DELETE /api/v1/biac/roles/{roleId}/columnMasks/{columnMaskId}`

Remove a column mask from a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `columnMaskId` (integer, required): Column mask ID

### Get Column Mask

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/columnMasks/{columnMaskId}`

Get a column mask of a given role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `columnMaskId` (integer, required): Column mask ID

### Get Column Mask Expression

**Endpoint:** `GET /api/v1/biac/expressions/columnMask/{columnMaskExpressionId}`

Get an expression that can be used to mask column values.

#### Parameters

**Path Parameters:**
- `columnMaskExpressionId` (integer, required): Column mask expression ID

### List Column Mask Expressions

**Endpoint:** `GET /api/v1/biac/expressions/columnMask`

List the expressions that can be used to mask column values.

#### Parameters

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

### List Role Column Masks

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/columnMasks`

List all the column masks of a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

### Update Column Mask Expression

**Endpoint:** `PUT /api/v1/biac/expressions/columnMask/{columnMaskExpressionId}`

Update a column mask expression.

#### Parameters

**Path Parameters:**
- `columnMaskExpressionId` (integer, required): Column mask expression ID

#### Request Body

```json
{
  "name": "string",
  "expression": "string",
  "description": "string"
}
```

---

## Data Products

### Clone Data Product

**Endpoint:** `POST /api/v1/dataProduct/products/{dataProductId}/clone`

Clone an existing data product within the same domain. All datasets are cloned as well as tags and sample queries.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Request Body

```json
{
  "newName": "string",
  "newSchemaName": "string",
  "catalogName": "string",
  "dataDomainId": "uuid"
}
```

**Example:**
```json
{
  "newName": "Campaign Performance Q4",
  "newSchemaName": "campaign_performance_q4",
  "catalogName": "data_products",
  "dataDomainId": "00e29ec8-21f1-4239-afe3-05ee5a3ef57c"
}
```

### Create Data Product

**Endpoint:** `POST /api/v1/dataProduct/products`

Create a data product in DRAFT status.

#### Request Body

```json
{
  "name": "string",
  "catalogName": "string",
  "schemaName": "string",
  "dataDomainId": "uuid",
  "summary": "string",
  "description": "string",
  "views": [
    {
      "name": "string",
      "description": "string",
      "viewSecurityMode": "DEFINER|INVOKER",
      "definitionQuery": "string",
      "columns": [
        {
          "name": "string",
          "type": "string",
          "description": "string"
        }
      ],
      "markedForDeletion": false
    }
  ],
  "materializedViews": [
    {
      "name": "string",
      "description": "string",
      "definitionQuery": "string",
      "definitionProperties": {},
      "columns": [
        {
          "name": "string",
          "type": "string",
          "description": "string"
        }
      ],
      "markedForDeletion": false
    }
  ],
  "owners": [
    {
      "name": "string",
      "email": "string"
    }
  ],
  "relevantLinks": [
    {
      "label": "string",
      "url": "string"
    }
  ]
}
```

**Example:**
```json
{
  "name": "Campaign performance",
  "catalogName": "data_products",
  "schemaName": "schema_name",
  "dataDomainId": "00e29ec8-21f1-4239-afe3-05ee5a3ef57c",
  "summary": "This data product provides details about the performance of the organization's marketing campaigns.",
  "description": "How the data is generated, how frequently it is updated, and any other relevant information for the users of the data product.",
  "views": [
    {
      "name": "campaign_performance_by_quarter",
      "description": "Campaign performance data per quarter.",
      "viewSecurityMode": "DEFINER",
      "definitionQuery": "SELECT name AS region_name FROM tpch.sf1.region",
      "columns": [
        {
          "name": "region_name",
          "type": "text",
          "description": "Name of the geographic region associated with this data."
        }
      ]
    }
  ],
  "owners": [
    {
      "name": "Alice Smith",
      "email": "alice.smith@example.com"
    }
  ],
  "relevantLinks": [
    {
      "label": "Example Link",
      "url": "https://example.com"
    }
  ]
}
```

### Get Data Product

**Endpoint:** `GET /api/v1/dataProduct/products/{dataProductId}`

Get a data product by identifier.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

### Get Materialized View Refresh Metadata

**Endpoint:** `GET /api/v1/dataProduct/products/{dataProductId}/materializedViews/{viewName}/refreshMetadata`

Get refresh metadata about a materialized view that belongs to a data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID
- `viewName` (string, required): View name

#### Response Schema

**200 - Success**
```json
{
  "lastImport": {
    "status": "string",
    "scheduledTime": "2023-01-01T00:00:00Z",
    "startTime": "2023-01-01T00:00:00Z",
    "finishTime": "2023-01-01T00:00:00Z",
    "rowCount": 0,
    "error": "string"
  },
  "incrementalColumn": "string",
  "refreshInterval": "string",
  "refreshSchedule": "string",
  "refreshScheduleTimezone": "string",
  "storageSchema": "string",
  "estimatedNextRefreshTime": "2023-01-01T00:00:00Z"
}
```

### Get OpenAPI Specification

**Endpoint:** `GET /api/v1/dataProduct/openApi`

Return the OpenAPI specification for the Data Product API. Use Accept header of 'application/yaml' to request a YAML response instead of JSON.

### Get Target Catalogs

**Endpoint:** `GET /api/v1/dataProduct/catalogs`

Return all catalogs suitable to store data products.

#### Response Schema

**200 - Success**
```json
[
  {
    "catalogName": "string",
    "connectorName": "string",
    "isMaterializedViewEnabled": true
  }
]
```

### List Sample Queries

**Endpoint:** `GET /api/v1/dataProduct/products/{dataProductId}/sampleQueries`

Get the sample queries for a data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Response Schema

**200 - Success**
```json
[
  {
    "name": "Select the name of all regions",
    "description": "This query selects the name of all regions from the tpch.sf1 schema",
    "query": "SELECT * FROM tpch.sf1.region"
  }
]
```

### Reassign Domain for Data Products

**Endpoint:** `POST /api/v1/dataProduct/products/reassignDomain`

Reassign the domain for a list of data products.

#### Request Body

```json
{
  "dataProductIds": ["uuid"],
  "newDomainId": "uuid"
}
```

### Search Data Products

**Endpoint:** `GET /api/v1/dataProduct/products`

Search for data products according to the searchOptions request parameter.

#### Parameters

**Query Parameters:**
- `searchOptions` (SearchOptionsParam): URL-encoded JSON value

#### Search Options Schema

```json
{
  "searchString": "string",
  "sortKey": "NAME|CREATED_AT|CREATED_BY|UPDATED_AT|LAST_QUERIED_AT|CATALOG_NAME|SCHEMA_NAME|RATINGS_AVERAGE|BOOKMARK|STATUS",
  "sortDirection": "ASC|DESC",
  "limit": 100,
  "onlyBookmarked": false,
  "dataDomainIds": ["uuid"],
  "tagIds": ["uuid"]
}
```

### Update Data Product

**Endpoint:** `PUT /api/v1/dataProduct/products/{dataProductId}`

Update a data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Request Body
Uses the same schema as Create Data Product.

### Update Materialized View in Data Product

**Endpoint:** `PUT /api/v1/dataProduct/products/{dataProductId}/materializedViews`

Create/Update a materialized view in data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Request Body

```json
{
  "name": "string",
  "description": "string",
  "definitionQuery": "string",
  "definitionProperties": {},
  "columns": [
    {
      "name": "string",
      "type": "string",
      "description": "string"
    }
  ],
  "markedForDeletion": false
}
```

**Example:**
```json
{
  "name": "campaign_performance_by_quarter",
  "description": "Campaign performance data per quarter.",
  "definitionQuery": "SELECT name AS region_name FROM tpch.sf1.region",
  "definitionProperties": {
    "refresh_interval": "60m"
  },
  "columns": [
    {
      "name": "region_name",
      "type": "text",
      "description": "Name of the geographic region associated with this data."
    }
  ]
}
```

### Update Sample Queries

**Endpoint:** `PUT /api/v1/dataProduct/products/{dataProductId}/sampleQueries`

Update sample queries for a data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Request Body

```json
[
  {
    "name": "Select the name of all regions",
    "description": "This query selects the name of all regions from the tpch.sf1 schema",
    "query": "SELECT * FROM tpch.sf1.region"
  }
]
```

### Update View in Data Product

**Endpoint:** `PUT /api/v1/dataProduct/products/{dataProductId}/views`

Create/Updates a view in a Data Product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Request Body

```json
{
  "name": "string",
  "description": "string",
  "viewSecurityMode": "DEFINER|INVOKER",
  "definitionQuery": "string",
  "columns": [
    {
      "name": "string",
      "type": "string",
      "description": "string"
    }
  ],
  "markedForDeletion": false
}
```

**Example:**
```json
{
  "name": "campaign_performance_by_quarter",
  "description": "Campaign performance data per quarter.",
  "viewSecurityMode": "DEFINER",
  "definitionQuery": "SELECT name AS region_name FROM tpch.sf1.region",
  "columns": [
    {
      "name": "region_name",
      "type": "text",
      "description": "Name of the geographic region associated with this data."
    }
  ]
}
```

---

## Domains

### Create Domain

**Endpoint:** `POST /api/v1/dataProduct/domains`

Create a data product domain.

#### Request Body

```json
{
  "name": "string",
  "description": "string",
  "schemaLocation": "string"
}
```

**Example:**
```json
{
  "name": "Marketing",
  "description": "This domain holds data published by the marketing team.",
  "schemaLocation": "s3://my-bucket/marketing/"
}
```

#### Response Schema

**200 - Success**
```json
{
  "id": "uuid",
  "name": "string",
  "description": "string",
  "schemaLocation": "string",
  "assignedDataProducts": [
    {
      "id": "uuid",
      "name": "string"
    }
  ],
  "createdBy": "string",
  "createdAt": "2023-01-01T00:00:00Z",
  "updatedAt": "2023-01-01T00:00:00Z",
  "updatedBy": "string"
}
```

### Delete Domain

**Endpoint:** `DELETE /api/v1/dataProduct/domains/{dataDomainId}`

Delete a data product domain.

#### Parameters

**Path Parameters:**
- `dataDomainId` (UUID, required): Domain ID

### Get Domain

**Endpoint:** `GET /api/v1/dataProduct/domains/{dataDomainId}`

Get a data product domain by identifier.

#### Parameters

**Path Parameters:**
- `dataDomainId` (UUID, required): Domain ID

### List Domains

**Endpoint:** `GET /api/v1/dataProduct/domains`

List all data product domains.

#### Response Schema

**200 - Success**
Returns an array of domain objects with the same schema as Get Domain.

### Update Domain

**Endpoint:** `PUT /api/v1/dataProduct/domains/{dataDomainId}`

Update a data product domain.

#### Parameters

**Path Parameters:**
- `dataDomainId` (UUID, required): Domain ID

#### Request Body

```json
{
  "description": "string",
  "schemaLocation": "string"
}
```

---

## Entity Categories

### List Available Actions

**Endpoint:** `GET /api/v1/biac/entityCategories/{entityCategory}/actions`

List all actions that can be allowed or denied for an EntityCategory.

#### Parameters

**Path Parameters:**
- `entityCategory` (string, required): Entity category

#### Response Schema

**200 - Success**
```json
{
  "nextPageToken": "string",
  "count": 0,
  "result": ["SHOW", "CREATE", "ALTER", "DROP", "EXECUTE", "SELECT", "INSERT", "DELETE", "UPDATE", "REFRESH", "IMPERSONATE", "KILL", "SET", "PUBLISH", "READ", "WRITE"]
}
```

### List Entity Categories

**Endpoint:** `GET /api/v1/biac/entityCategories`

List all entity categories in which access control can be managed by Starburst built-in access control.

#### Parameters

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

#### Response Schema

**200 - Success**
```json
{
  "nextPageToken": "string",
  "count": 0,
  "result": ["TABLES", "SCHEMA_PROPERTIES", "TABLE_PROPERTIES", "SYSTEM_SESSION_PROPERTIES", "CATALOG_SESSION_PROPERTIES", "FUNCTIONS", "PROCEDURES", "QUERIES", "ROLES", "USERS", "DATA_PRODUCTS", "AUDIT_LOGS", "USER_INTERFACE", "SYSTEM_INFORMATION", "AI_MODELS"]
}
```

---

## Grants

### Create Grant

**Endpoint:** `POST /api/v1/biac/roles/{roleId}/grants`

Create a Starburst built-in access control grant for a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

#### Request Body

```json
{
  "effect": "ALLOW|ALLOW_WITH_GRANT_OPTION|DENY",
  "action": "SHOW|CREATE|ALTER|DROP|EXECUTE|SELECT|INSERT|DELETE|UPDATE|REFRESH|IMPERSONATE|KILL|SET|PUBLISH|READ|WRITE",
  "entity": {
    "category": "string",
    "allEntities": false
  }
}
```

#### Response Schema

**200 - Success**
```json
{
  "id": 0,
  "effect": "string",
  "action": "string",
  "entity": {
    "category": "string",
    "allEntities": false
  }
}
```

### Delete Grant

**Endpoint:** `DELETE /api/v1/biac/roles/{roleId}/grants/{grantId}`

Delete a Starburst built-in access control grant of a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `grantId` (integer, required): Grant ID

### Get Grant

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/grants/{grantId}`

Get a Starburst built-in access control grant of a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `grantId` (integer, required): Grant ID

### List Grants

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/grants`

List Starburst built-in access control grants of a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

---

## Location Grants

### Add Location Grant

**Endpoint:** `POST /api/v1/biac/roles/{roleId}/locationGrants`

Add a LocationGrant to a role to allow accessing that location by the given role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

#### Request Body

```json
{
  "location": "string",
  "grantOption": true
}
```

#### Response Schema

**200 - Success**
```json
{
  "id": 0,
  "location": "string",
  "grantOption": true
}
```

### Delete Location Grant

**Endpoint:** `DELETE /api/v1/biac/roles/{roleId}/locationGrants/{locationGrantId}`

Remove a LocationGrant from a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `locationGrantId` (integer, required): Location grant ID

### Get Location Grant

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/locationGrants/{locationGrantId}`

Get a Starburst built-in access control location grant of a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `locationGrantId` (integer, required): Location grant ID

### List Location Grants

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/locationGrants`

List Starburst built-in access control location grants of a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

---

## Role Assignments

### List Role Assignments

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/assignments`

List assignments of a Starburst built-in access control role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

#### Response Schema

**200 - Success**
```json
[
  {
    "id": 0,
    "subject": {
      "type": "USER|GROUP|ROLE",
      "username": "string",
      "groupName": "string",
      "roleId": 0
    },
    "roleAdmin": true
  }
]
```

---

## Roles

### Create Role

**Endpoint:** `POST /api/v1/biac/roles`

Create a Starburst built-in access control role.

#### Request Body

```json
{
  "name": "string",
  "description": "string"
}
```

#### Response Schema

**200 - Success**
```json
{
  "id": 0,
  "name": "string",
  "description": "string"
}
```

### Delete Role

**Endpoint:** `DELETE /api/v1/biac/roles/{roleId}`

Delete a Starburst built-in access control role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

### Get Role

**Endpoint:** `GET /api/v1/biac/roles/{roleId}`

Get a Starburst built-in access control role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

### List Roles

**Endpoint:** `GET /api/v1/biac/roles`

List Starburst built-in access control roles.

#### Parameters

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

### Update Role

**Endpoint:** `PUT /api/v1/biac/roles/{roleId}`

Update a Starburst built-in access control role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

#### Request Body

```json
{
  "name": "string",
  "description": "string"
}
```

---

## Row Filters

### Add Row Filter to Role

**Endpoint:** `POST /api/v1/biac/roles/{roleId}/rowFilters`

Add a row filter to a role to filter out table rows from queries made by subjects assigned to the role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

#### Request Body

```json
{
  "entity": {
    "category": "TABLES",
    "allEntities": false,
    "catalog": "string",
    "schema": "string",
    "table": "string",
    "tableType": "TABLE|VIEW|MATERIALIZED_VIEW",
    "columns": ["string"]
  },
  "expressionId": 0
}
```

### Create Row Filter Expression

**Endpoint:** `POST /api/v1/biac/expressions/rowFilter`

Create an expression that can be applied to filter table rows from query results.

#### Request Body

```json
{
  "name": "string",
  "expression": "string",
  "description": "string"
}
```

### Delete Role Row Filter

**Endpoint:** `DELETE /api/v1/biac/roles/{roleId}/rowFilters/{rowFilterId}`

Remove a row filter from a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `rowFilterId` (integer, required): Row filter ID

### Delete Row Filter Expression

**Endpoint:** `DELETE /api/v1/biac/expressions/rowFilter/{rowFilterExpressionId}`

Delete a row filter expression.

#### Parameters

**Path Parameters:**
- `rowFilterExpressionId` (integer, required): Row filter expression ID

### Get Row Filter

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/rowFilters/{rowFilterId}`

Get a row filter of a given role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `rowFilterId` (integer, required): Row filter ID

### Get Row Filter Expression

**Endpoint:** `GET /api/v1/biac/expressions/rowFilter/{rowFilterExpressionId}`

Get an expression that can be applied to filter table rows from query results.

#### Parameters

**Path Parameters:**
- `rowFilterExpressionId` (integer, required): Row filter expression ID

### List Role Row Filters

**Endpoint:** `GET /api/v1/biac/roles/{roleId}/rowFilters`

List all the row filters of a role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

### List Row Filter Expressions

**Endpoint:** `GET /api/v1/biac/expressions/rowFilter`

List expressions that can be applied to filter table rows from query results.

### Update Row Filter Expression

**Endpoint:** `PUT /api/v1/biac/expressions/rowFilter/{rowFilterExpressionId}`

Update a row filter expression.

#### Parameters

**Path Parameters:**
- `rowFilterExpressionId` (integer, required): Row filter expression ID

#### Request Body

```json
{
  "name": "string",
  "expression": "string",
  "description": "string"
}
```

---

## Subjects

### Create Role Assignment for Group

**Endpoint:** `POST /api/v1/biac/subjects/groups/{groupName}/assignments`

Assign a Starburst built-in access control role to the given group.

#### Parameters

**Path Parameters:**
- `groupName` (string, required): Group name

#### Request Body

```json
{
  "roleId": 0,
  "roleAdmin": true
}
```

### Create Role Assignment for Role

**Endpoint:** `POST /api/v1/biac/subjects/roles/{roleId}/assignments`

Assign a Starburst built-in access control role to the given role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

#### Request Body

```json
{
  "roleId": 0,
  "roleAdmin": true
}
```

### Create Role Assignment for User

**Endpoint:** `POST /api/v1/biac/subjects/users/{username}/assignments`

Assign a Starburst built-in access control role to the given user.

#### Parameters

**Path Parameters:**
- `username` (string, required): Username

#### Request Body

```json
{
  "roleId": 0,
  "roleAdmin": true
}
```

### Delete Role Assignment from Group

**Endpoint:** `DELETE /api/v1/biac/subjects/groups/{groupName}/assignments/{assignmentId}`

Delete an assignment to a Starburst built-in access control role from the given group.

#### Parameters

**Path Parameters:**
- `groupName` (string, required): Group name
- `assignmentId` (integer, required): Assignment ID

### Delete Role Assignment from Role

**Endpoint:** `DELETE /api/v1/biac/subjects/roles/{roleId}/assignments/{assignmentId}`

Delete an assignment to a Starburst built-in access control role from the given role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID
- `assignmentId` (integer, required): Assignment ID

### Delete Role Assignment from User

**Endpoint:** `DELETE /api/v1/biac/subjects/users/{username}/assignments/{assignmentId}`

Delete an assignment to a Starburst built-in access control role from the given user.

#### Parameters

**Path Parameters:**
- `username` (string, required): Username
- `assignmentId` (integer, required): Assignment ID

### Get Role Assignments for Group

**Endpoint:** `GET /api/v1/biac/subjects/groups/{groupName}/assignments`

Get all Starburst built-in access control roles assigned to the given group.

#### Parameters

**Path Parameters:**
- `groupName` (string, required): Group name

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

### Get Role Assignments for Role

**Endpoint:** `GET /api/v1/biac/subjects/roles/{roleId}/assignments`

Get all Starburst built-in access control roles assigned to the given role.

#### Parameters

**Path Parameters:**
- `roleId` (integer, required): Role ID

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

### Get Role Assignments for User

**Endpoint:** `GET /api/v1/biac/subjects/users/{username}/assignments`

Get all Starburst built-in access control roles assigned to the given user.

#### Parameters

**Path Parameters:**
- `username` (string, required): Username

**Query Parameters:**
- `pageToken` (string): Page token
- `pageSize` (string): Page size
- `pageSort` (string): Sorting order

---

## Tags

### Delete All Unused Tags

**Endpoint:** `DELETE /api/v1/dataProduct/tags/unused`

Delete all unused tags.

### Delete Data Product Tag

**Endpoint:** `DELETE /api/v1/dataProduct/tags/{tagId}/products/{dataProductId}`

Delete a tag from a specific data product.

#### Parameters

**Path Parameters:**
- `tagId` (UUID, required): Tag ID
- `dataProductId` (UUID, required): Data product ID

### Delete Tag

**Endpoint:** `DELETE /api/v1/dataProduct/tags/{tagId}`

Delete an unused tag with a specific identifier.

#### Parameters

**Path Parameters:**
- `tagId` (UUID, required): Tag ID

### Get Data Product Tags

**Endpoint:** `GET /api/v1/dataProduct/tags/products/{dataProductId}`

List all tags associated with a specific data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Response Schema

**200 - Success**
```json
[
  {
    "id": "uuid",
    "value": "string"
  }
]
```

### List Tags

**Endpoint:** `GET /api/v1/dataProduct/tags`

List all available tags for all data products.

#### Response Schema

**200 - Success**
```json
[
  {
    "id": "uuid",
    "value": "string"
  }
]
```

### Update Data Product Tags

**Endpoint:** `PUT /api/v1/dataProduct/tags/products/{dataProductId}`

Replace all tags for a specific data product with the supplied list of new tags.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Request Body

```json
[
  {
    "value": "marketing"
  }
]
```

### Update Tag

**Endpoint:** `PUT /api/v1/dataProduct/tags/{tagId}`

Update a specific tag.

#### Parameters

**Path Parameters:**
- `tagId` (UUID, required): Tag ID

#### Request Body

```json
{
  "value": "marketing"
}
```

---

## Workflows

### Delete Data Product

**Endpoint:** `POST /api/v1/dataProduct/products/{dataProductId}/workflows/delete`

Initiate an asynchronous task to delete the data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

**Query Parameters:**
- `skipTrinoDelete` (boolean): If true, just delete from the data product DB, don't delete any tables or schemas from Trino

#### Response Headers

**202 - Accepted**
- `Location` (string): The endpoint to poll in order to GET the status of the operation

### Get Delete Data Product Status

**Endpoint:** `GET /api/v1/dataProduct/products/{dataProductId}/workflows/delete`

Get the status of the async task to delete a data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Response Schema

**200 - Success**
```json
{
  "workflowType": "DELETE|PUBLISH|REFRESH_MATERIALIZED_VIEW",
  "status": "SCHEDULED|IN_PROGRESS|COMPLETED|ERROR",
  "errors": [
    {
      "entityType": "SCHEMA|DATASET|METADATA",
      "entityName": "string",
      "message": "string"
    }
  ],
  "isFinalStatus": true
}
```

### Get Publish Data Product Status

**Endpoint:** `GET /api/v1/dataProduct/products/{dataProductId}/workflows/publish`

Get the status of the async task that publishes a data product.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

#### Response Schema

**200 - Success**
Same schema as Get Delete Data Product Status.

### Publish Data Product

**Endpoint:** `POST /api/v1/dataProduct/products/{dataProductId}/workflows/publish`

Publish the data product by initiating an asynchronous task to populate the views, or materialized views, in the schema.

#### Parameters

**Path Parameters:**
- `dataProductId` (UUID, required): Data product ID

**Query Parameters:**
- `force` (boolean): If true the data product will be republished, even if it is already published, and all its datasets will be recreated

#### Response Headers

**202 - Accepted**
- `Location` (string): The endpoint to poll in order to GET the status of the operation

---

## Internal API Endpoints

### Filter Nested Entities

**Endpoint:** `POST /api/internal/v1/biac/filter`

Internal endpoint for filtering nested entities.

#### Request Body

```json
{
  "subject": {
    "user": "string",
    "groups": [],
    "enabledRoles": []
  },
  "actions": ["SHOW", "CREATE", "ALTER", "DROP", "EXECUTE", "SELECT", "INSERT", "DELETE", "UPDATE", "REFRESH", "IMPERSONATE", "KILL", "SET", "PUBLISH", "EDIT", "READ", "WRITE", "MANAGE", "USE", "SUBSCRIBE", "UNSUBSCRIBE"],
  "parentEntity": {
    "attributes": [],
    "onlyNonCategoryAttribute": {},
    "action": [],
    "onlyAttribute": {},
    "empty": true,
    "keys": []
  },
  "childAttributeKey": "string",
  "childAttributeValues": ["string"]
}
```

### Check Access

**Endpoint:** `POST /api/internal/v1/biac/access`

Internal endpoint for checking access permissions.

#### Request Body

```json
{
  "subject": {
    "user": "string",
    "groups": [],
    "enabledRoles": []
  },
  "action": "SHOW|CREATE|ALTER|DROP|EXECUTE|SELECT|INSERT|DELETE|UPDATE|REFRESH|IMPERSONATE|KILL|SET|PUBLISH|EDIT|READ|WRITE|MANAGE|USE|SUBSCRIBE|UNSUBSCRIBE",
  "entity": {
    "attributes": [],
    "onlyNonCategoryAttribute": {},
    "action": [],
    "onlyAttribute": {},
    "empty": true,
    "keys": []
  }
}
```

#### Response Schema

```json
{
  "access": "ALLOW|DENY"
}
```

---

## Error Handling

All API endpoints follow a consistent error response format:

### Error Response Schema

```json
{
  "errorCode": "INVALID_ARGUMENT|NOT_SUPPORTED|SQL_SYNTAX_ERROR|INVALID_SCHEMA_NAME|PERMISSION_DENIED|NOT_FOUND|ENTITY_NOT_FOUND|ALREADY_EXISTS|INTERNAL_ERROR",
  "message": "string",
  "detailsType": "string",
  "details": {
    "type": "string"
  }
}
```

### Common HTTP Status Codes

- **200**: Success
- **201**: Created
- **202**: Accepted (for async operations)
- **204**: No Content / Success with no response body
- **400**: Bad Request
- **401**: Unauthorized
- **403**: Forbidden
- **404**: Not Found
- **409**: Conflict

---

## Flask Implementation Helpers

### Base Flask Service Class

```python
import requests
import base64
from typing import Dict, Any, Optional, List
from flask import current_app

class StarburstAPIService:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self._auth_header = self._get_auth_header()
    
    def _get_auth_header(self) -> Dict[str, str]:
        credentials = f"{self.username}:{self.password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        response = requests.request(method, url, headers=self._auth_header, **kwargs)
        return response
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> requests.Response:
        return self._make_request("GET", endpoint, params=params)
    
    def post(self, endpoint: str, json_data: Optional[Dict] = None) -> requests.Response:
        return self._make_request("POST", endpoint, json=json_data)
    
    def put(self, endpoint: str, json_data: Optional[Dict] = None) -> requests.Response:
        return self._make_request("PUT", endpoint, json=json_data)
    
    def delete(self, endpoint: str) -> requests.Response:
        return self._make_request("DELETE", endpoint)
```

### Data Product Service Example

```python
from typing import List, Dict, Any, Optional
import uuid

class DataProductService(StarburstAPIService):
    
    def create_data_product(self, data_product: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new data product"""
        response = self.post("/api/v1/dataProduct/products", json_data=data_product)
        response.raise_for_status()
        return response.json()
    
    def get_data_product(self, data_product_id: uuid.UUID) -> Dict[str, Any]:
        """Get a data product by ID"""
        response = self.get(f"/api/v1/dataProduct/products/{data_product_id}")
        response.raise_for_status()
        return response.json()
    
    def search_data_products(self, search_options: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for data products"""
        import urllib.parse
        search_param = urllib.parse.quote(json.dumps(search_options))
        response = self.get("/api/v1/dataProduct/products", params={"searchOptions": search_param})
        response.raise_for_status()
        return response.json()
    
    def publish_data_product(self, data_product_id: uuid.UUID, force: bool = False) -> str:
        """Publish a data product and return the location header for status polling"""
        params = {"force": force} if force else None
        response = self.post(f"/api/v1/dataProduct/products/{data_product_id}/workflows/publish", params=params)
        response.raise_for_status()
        return response.headers.get("Location")
    
    def get_publish_status(self, data_product_id: uuid.UUID) -> Dict[str, Any]:
        """Get the publish status of a data product"""
        response = self.get(f"/api/v1/dataProduct/products/{data_product_id}/workflows/publish")
        response.raise_for_status()
        return response.json()
```

### Domain Service Example

```python
class DomainService(StarburstAPIService):
    
    def create_domain(self, name: str, description: str = None, schema_location: str = None) -> Dict[str, Any]:
        """Create a new domain"""
        domain_data = {"name": name}
        if description:
            domain_data["description"] = description
        if schema_location:
            domain_data["schemaLocation"] = schema_location
        
        response = self.post("/api/v1/dataProduct/domains", json_data=domain_data)
        response.raise_for_status()
        return response.json()
    
    def list_domains(self) -> List[Dict[str, Any]]:
        """List all domains"""
        response = self.get("/api/v1/dataProduct/domains")
        response.raise_for_status()
        return response.json()
```

This documentation provides a complete reference for integrating with the Starburst Enterprise API using Flask, maintaining all the technical detail from the original PDF while structuring it for Python development use cases.