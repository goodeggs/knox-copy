qs = require 'querystring'
{isArray} = require 'util'
{queue, waterfall} = require 'async'
knox = require 'knox'
{Parser} = require 'xml2js'

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
Returns an http request.  Optional callback receives the response.
###
knox::copyFromBucket = (fromBucket, fromKey, toKey, headers, cb) ->
  if typeof headers is 'function'
    cb = headers
    headers = {}

  headers['x-amz-copy-source'] = "/#{fromBucket}/#{stripLeadingSlash fromKey}"
  headers['Content-Length'] = 0 # avoid chunking
  req = @request 'PUT', toKey, headers

  if cb?
    req.on('response', (res) -> cb(null, res))
    req.on('error', (err) -> cb new Error("Error copying #{fromKey}:", err))
    req.end()
  return req

###
Callback gets a JSON representation of the page of S3 Object keys.
###
knox::listBucketPage = ({maxKeys, marker, prefix, headers}, cb) ->
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

knox::copyBucket = ({fromBucket, fromPrefix, toPrefix}, cb) ->
  fromBucket ?= @bucket
  fromClient = knox.createClient {@key, @secret, bucket: fromBucket}
  fromPrefix = fromPrefix and stripLeadingSlash(fromPrefix) or ''
  toPrefix = toPrefix and stripLeadingSlash(toPrefix) or ''

  # maximum simultaneus Object copy or key listing operations
  concurrentRequests = 5

  # number of keys to list per request.  The copy queue caps at ~1.5x this size.
  maxKeys = 500

  # number of keys copied
  count = 0

  # true if some worker is replenishing keys
  keysPending = false

  # true once all keys have been queued for copying
  keysExhausted = false

  keysRunningLow = -> q.length() < (maxKeys / 2)

  # pagination index
  marker = null

  # abort the copy on the first unrecoverable error
  failed = false
  fatalError = (err) ->
    failed = true
    cb err, count

  # enqueue the next page of keys as needed
  replenishKeys = (done) ->
    done ?= (->)
    keysPending = true
    fromClient.listBucketPage {prefix: fromPrefix, maxKeys, marker}, (err, page) ->
      fatalError(err) if err?

      unless failed
        # extract and queue up the keys
        keys = page.Contents.map (o) -> o.Key
        q.push keys

        if page.IsTruncated
          # advance the marker a page
          marker = keys.slice(-1)[0]
        else
          keysExhausted = true

      keysPending = false
      done(keys.length)

  # worker to copy a single key
  worker = (key, done) =>
    # burn queue after unrecoverable error
    return done() if failed

    toKey = swapPrefix(key, fromPrefix, toPrefix)
    @copyFromBucket fromBucket, key, toKey, (err, res) ->
      fatalError(err) if err?
      fatalError({key, statusCode: res.statusCode}) if res.statusCode isnt 200
      return done() if failed

      count++
      if not (keysExhausted or keysPending) and keysRunningLow()
        replenishKeys done
      else
        done()

  q = queue worker, concurrentRequests

  # queue up the first batch of keys
  replenishKeys (count) ->
    # Halt copying empty bucket
    unless count > 0
      cb null, count

  q.drain = ->
    # Guard! We drain the queue after calling back with an error
    unless failed
      cb null, count

module.exports = knox