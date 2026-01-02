const express = require('express')
const fetch = require('node-fetch')
const dotenv = require('dotenv')
const { mountFarmsApi } = require('./farmsApi')
const { initDb, isDbEnabled } = require('./db')

dotenv.config()

const PORT = Number(process.env.PORT || 3001)
const ALLOW_ORIGIN = process.env.FARMS_ALLOW_ORIGIN || process.env.CORS_ALLOW_ORIGIN || '*'
const RIPPLE_DATA_BASE = process.env.RIPPLE_DATA_API_BASE || 'https://data.ripple.com/v2'

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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization')
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

app.get('/ripple-data/v2/*', async (req, res) => {
  try {
    const targetPath = req.originalUrl.replace(/^\/ripple-data\/v2/, '')
    const response = await fetch(`${RIPPLE_DATA_BASE}${targetPath}`)
    const text = await response.text()
    res.status(response.status)
    res.setHeader('Content-Type', response.headers.get('content-type') || 'application/json')
    res.send(text)
  } catch (error) {
    res.status(502).json({ error: 'Failed to fetch ripple-data' })
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
