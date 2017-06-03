qs = require 'querystring'
{isArray} = require 'util'
{waterfall} = require 'async'
knox = require 'knox'
{Parser} = require 'xml2js'
KeyStream = require './key_stream'
backoff = require('oibackoff').backoff
  algorithm: 'exponential'
  delayRatio: 0.5
  maxTries: 4
streamWorker = require 'stream-worker'

stripLeadingSlash = (key) -> key.replace /^\//, ''

swapPrefix = (key, oldPre, newPre) ->
  "#{newPre}/#{stripLeadingSlash(key.slice(oldPre.length))}"

ensureBuffer = (data) ->
  Buffer.isBuffer(data) and data or new Buffer(data)

###
Read a whole stream into a buffer.
###
guzzle = (stream, cb) ->
  buffers = []
  stream.on 'data', (chunk) ->
    buffers.push(ensureBuffer(chunk))
  stream.on 'error', (err) ->
    cb err
  stream.on 'end', ->
    cb null, Buffer.concat(buffers)

###
Callback gets a JSON representation of a page of S3 Object keys.
###
knox::listPageOfKeys = ({maxKeys, marker, prefix, headers}, cb) ->
  maxKeys ?= 1000
  headers ?= {}
  prefix = stripLeadingSlash prefix if prefix?
  marker = stripLeadingSlash marker if marker?

  error = (err) ->
    cb new Error("Error listing bucket #{{marker, prefix}}:", err)

  waterfall [
    # Start request
    (next) =>
      req = @request('GET', '/', headers)
      req.path += "?" + qs.stringify {'max-keys': maxKeys, prefix, marker}

      req.on 'error', error
      req.on 'response', (res) ->
        if res.statusCode is 200
          # Buffer full response
          # Unfortunately we miss the first few chunks if we wire
          # guzzle in the waterfall without pausing the response stream.
          guzzle res, next
        else
          error res
      req.end()

    # Parse XML
    (new Parser explicitArray: false, explicitRoot: false).parseString

    # Normalize
    #   Contents always exists, always an array
    #   IsTruncated string -> Boolean
    (page, next) ->
      page.IsTruncated = page.IsTruncated is 'true'
      page.Contents =
        if isArray(page.Contents) then page.Contents
        else if page.Contents? then [page.Contents]
        else []
      next null, page

  ], (err, page) ->
    if err?
      error err
    else
      cb null, page

knox::streamKeys = ({prefix, maxKeysPerRequest}={}) ->
  return new KeyStream {prefix, client: @, maxKeysPerRequest}

knox::copyBucket = ({fromBucket, fromPrefix, toPrefix, headers}, cb) ->
  fromBucket ?= @bucket
  fromClient = knox.createClient {@key, @secret, bucket: fromBucket, @token}
  fromPrefix = fromPrefix and stripLeadingSlash(fromPrefix) or ''
  toPrefix = toPrefix and stripLeadingSlash(toPrefix) or ''

  # number of keys copied
  count = 0

  # abort the copy on the first unrecoverable error
  failed = false
  fail = (err) ->
    return if failed
    failed = true
    cb err, count

  keyStream = fromClient.streamKeys prefix: fromPrefix
  keyStream.on 'error', fail

  streamWorker keyStream, 5,
    (key, done) =>
      toKey = swapPrefix(key, fromPrefix, toPrefix)
      backoff(
        (cb) =>
          fromClient.copyFileTo key, @bucket, toKey, headers, (err, res) ->
            if err?
              cb err
            else if res.statusCode isnt 200
              cb new Error "#{res.statusCode} response copying key #{key}"
            else
              cb()
        (err) =>
          if err?
            fail err
          count++
          done()
      )
    ->
      if not failed
        cb null, count

module.exports = knox
