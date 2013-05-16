knox-copy
=========

[![Dependency Status](https://david-dm.org/goodeggs/knox-copy.png)](https://david-dm.org/goodeggs/knox-copy)

Extends the Knox Node Amazon S3 client to support copying and listing arbitrarily large buckets.  Transparently streams keys and copies files from buckets that exceed Amazon's [1000 key page size limit](http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html).

## Install

``` sh
npm install knox-copy
```

## Examples

[Stream] the keys of all the files in a bucket:

[Stream]: http://nodejs.org/api/stream.html#stream_readable_stream

``` coffeescript
knoxCopy = require 'knox-copy'

client = knoxCopy.createClient
  key: '<api-key-here>'
  secret: '<secret-here>'
  bucket: 'mrbucket'

client.streamKeys(prefix: 'buckets/of/fun')
.on 'data', (key) -> console.log key
```

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
  toPrefix: "/upload_backups/#{new Date().toISOString()}"
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

