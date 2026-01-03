const express = require('express')
const fetch = require('node-fetch')
const dotenv = require('dotenv')
const { mountFarmsApi } = require('./farmsApi')
const { initDb, isDbEnabled } = require('./db')

dotenv.config()

const PORT = Number(process.env.PORT || 3001)
const ALLOW_ORIGIN = process.env.FARMS_ALLOW_ORIGIN || process.env.CORS_ALLOW_ORIGIN || '*'
const RIPPLE_DATA_BASE = process.env.RIPPLE_DATA_API_BASE || 'https://data.ripple.com/v2'
const BITHOMP_BASE = process.env.BITHOMP_API_BASE || 'https://bithomp.com/api/v2'
const BITHOMP_API_KEY = process.env.BITHOMP_API_KEY || ''
const BITHOMP_API_KEY_HEADER = process.env.BITHOMP_API_KEY_HEADER || 'x-bithomp-token'
const XRPSCAN_BASE = process.env.XRPSCAN_API_BASE || 'https://api.xrpscan.com/api/v1'

const app = express()
app.set('etag', false)

const normalizeOrigin = (value = '') => value.trim().replace(/\/$/, '')

const parseAllowedOrigins = (value) => {
  if (!value) return ['*']
  return value
    .split(',')
    .map((entry) => normalizeOrigin(entry))
    .filter(Boolean)
}

const allowedOrigins = parseAllowedOrigins(ALLOW_ORIGIN)

app.use((req, res, next) => {
  const requestOrigin = normalizeOrigin(req.headers.origin || '')
  const allowAll = allowedOrigins.includes('*')
  if (allowAll) {
    res.setHeader('Access-Control-Allow-Origin', '*')
  } else if (requestOrigin && allowedOrigins.includes(requestOrigin)) {
    res.setHeader('Access-Control-Allow-Origin', requestOrigin)
  } else if (allowedOrigins[0]) {
    res.setHeader('Access-Control-Allow-Origin', allowedOrigins[0])
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-bithomp-token')
  res.setHeader('Cache-Control', 'no-store')
  if (req.method === 'OPTIONS') {
    res.status(200).send('')
    return
  }
  next()
})

app.get('/', (req, res) => {
  res.json({ status: 'ok' })
})

const cacheStore = new Map()

const getCachedResponse = (key, { allowStale = false } = {}) => {
  const entry = cacheStore.get(key)
  if (!entry) return null
  const now = Date.now()
  if (entry.expiresAt > now) {
    return entry
  }
  if (allowStale && entry.staleUntil > now) {
    return entry
  }
  cacheStore.delete(key)
  return null
}

const cacheResponse = (key, data, status, contentType, ttlMs, staleMultiplier = 2) => {
  if (!ttlMs || ttlMs <= 0) return
  const now = Date.now()
  cacheStore.set(key, {
    expiresAt: now + ttlMs,
    staleUntil: now + ttlMs * staleMultiplier,
    body: data,
    status,
    contentType,
  })
}

const forwardJsonResponse = async (
  targetUrl,
  req,
  res,
  headers = {},
  cacheKey,
  cacheTtl,
  { allowStaleOnError = false } = {},
) => {
  if (cacheKey) {
    const cached = getCachedResponse(cacheKey)
    if (cached) {
      res.status(cached.status)
      res.setHeader('Content-Type', cached.contentType)
      res.send(cached.body)
      return
    }
  }

  const response = await fetch(targetUrl, {
    method: 'GET',
    headers: {
      'User-Agent': 'xrpbescswap-farms-proxy/1.0',
      Accept: 'application/json',
      ...headers,
    },
  })
  const text = await response.text()

  if (!response.ok && cacheKey && allowStaleOnError) {
    const stale = getCachedResponse(cacheKey, { allowStale: true })
    if (stale) {
      res.status(stale.status)
      res.setHeader('Content-Type', stale.contentType)
      res.send(stale.body)
      return
    }
  }
  if (response.status === 429 && cacheKey) {
    const stale = getCachedResponse(cacheKey, { allowStale: true })
    if (stale) {
      cacheResponse(
        cacheKey,
        stale.body,
        stale.status,
        stale.contentType,
        cacheTtl,
      )
      res.status(stale.status)
      res.setHeader('Content-Type', stale.contentType)
      res.send(stale.body)
      return
    }
  }

  res.status(response.status)
  res.setHeader('Content-Type', response.headers.get('content-type') || 'application/json')
  res.send(text)

  if (cacheKey && response.ok) {
    cacheResponse(
      cacheKey,
      text,
      response.status,
      response.headers.get('content-type') || 'application/json',
      cacheTtl,
    )
  }
}

app.get('/ripple-data/v2/*', async (req, res) => {
  try {
    const targetPath = req.originalUrl.replace(/^\/ripple-data\/v2/, '')
    await forwardJsonResponse(`${RIPPLE_DATA_BASE}${targetPath}`, req, res)
  } catch (error) {
    res.status(502).json({ error: 'Failed to fetch ripple-data' })
  }
})

app.get('/bithomp/api/v2/*', async (req, res) => {
  try {
    const targetPath = req.originalUrl.replace(/^\/bithomp\/api\/v2/, '')
    const headers = {}
    if (BITHOMP_API_KEY) {
      headers[BITHOMP_API_KEY_HEADER] = BITHOMP_API_KEY
    }
    const cacheKey = `bithomp:${targetPath}`
    let cacheTtl = 0
    if (targetPath.includes('trustlines/tokens')) {
      cacheTtl = 5 * 60 * 1000
    } else if (targetPath.includes('amms/search')) {
      cacheTtl = 5 * 60 * 1000
    }
    await forwardJsonResponse(
      `${BITHOMP_BASE}${targetPath}`,
      req,
      res,
      headers,
      cacheKey,
      cacheTtl,
      { allowStaleOnError: true },
    )
  } catch (error) {
    res.status(502).json({ error: 'Failed to fetch bithomp data' })
  }
})

app.get('/xrpscan/api/v1/*', async (req, res) => {
  try {
    const targetPath = req.originalUrl.replace(/^\/xrpscan\/api\/v1/, '')
    await forwardJsonResponse(`${XRPSCAN_BASE}${targetPath}`, req, res)
  } catch (error) {
    res.status(502).json({ error: 'Failed to fetch xrpscan data' })
  }
})

mountFarmsApi(app)

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Farms API listening on :${PORT}`)
})

if (isDbEnabled()) {
  initDb()
    .then(() => {
      // eslint-disable-next-line no-console
      console.log('Farms DB ready')
    })
    .catch((error) => {
      // eslint-disable-next-line no-console
      console.error(`Farms DB init failed: ${error.message || error}`)
    })
}
