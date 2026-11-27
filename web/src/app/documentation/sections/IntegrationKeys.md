# Using Integration Keys

## Generic API

### Params can be in query params or body (body takes precedence):

| Name      |              | Description                                                                                                                                                         |
| --------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `token`   | **Required** | The integration key to use.                                                                                                                                         |
| `summary` | **Required** | Short description of the alert sent as SMS and voice.                                                                                                               |
| `details` | _optional_   | Additional information about the alert, supports markdown.                                                                                                          |
| `action`  | _optional_   | If set to `close`, it will close any matching alerts.                                                                                                               |
| `dedup`   | _optional_   | All calls for the same service with the same `dedup` string will update the same alert (if open) or create a new one. Defaults to using summary & details together. |
| `meta`    | _optional_   | Additional key/value metadata to attach to the alert.                                                                                                               |

#### Metadata

Metadata can be used to attach additional information to the alert, form or query parameters can specify the metadata like this:

```url
/api/v2/generic/incomming?token=<token>&meta=example_key=example_value&meta=example_key2=example_value2
```

Or as a map for JSON requests:
```json
{
   "summary": "test",
   "details": "test",
   "meta": {
      "example_key": "example_value",
      "example_key2": "example_value2"
   }
}
```


### Response:

Default response is a `204` with no content. Set the `Accept` header to `application/json` for information on the created alert.

Example:

```json
{
  "AlertID": 10,
  "ServiceID": "00000000-0000-0000-0000-000000000001",
  "IsNew": true
}
```

`IsNew` will be false if the call was de-duplicated.

### Examples:

```bash
curl -XPOST https://<example.goalert.me>/api/v2/generic/incoming?token=key-here&summary=test&details=test
curl -XPOST https://<example.goalert.me>/api/v2/generic/incoming?token=key-here&summary=test&dedup=disk-check
curl -XPOST https://<example.goalert.me>/api/v2/generic/incoming?token=key-here&summary=test&action=close
```
