knox-copy
=========

Extends the Knox Node Amazon S3 client to support copying and listing buckets

## Examples

Backup a bucket full of uploads:

``` coffeescript
knoxCopy = require 'knox-copy'

client = knoxCopy.createClient
  key: '<api-key-here>'
  secret: '<secret-here>'
  bucket: 'backups'

client.copyBucket
  fromBucket: 'uploads'
  fromPrefix: '/nom-nom'
  toPrefix: "/copy_bucket/#{new Date().toISOString()}"
  (err, count) ->
     console.log "Copied #{count} files"
```

## Running Tests

Setup tests as with [knox].  You must first have an S3 account, and create
a file named _./auth_, which contains your credentials as json, for example:

[knox]: https://github.com/LearnBoost/knox#running-tests

```json
{
  "key":"<api-key-here>",
  "secret":"<secret-here>",
  "bucket":"<your-bucket-name>"
}
```

Then install the dev dependencies and execute the test suite:

    $ npm install
    $ npm test

