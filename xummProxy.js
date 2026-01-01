const express = require('express')
const fetch = require('node-fetch')
const dotenv = require('dotenv')

dotenv.config()

const XUMM_API_BASE = process.env.XUMM_API_BASE || 'https://xumm.app/api/v1/platform'
const XUMM_API_KEY = process.env.XUMM_API_KEY
const XUMM_API_SECRET = process.env.XUMM_API_SECRET
const PORT = Number(process.env.XUMM_PROXY_PORT || 3001)
const ALLOW_ORIGIN = process.env.XUMM_PROXY_ALLOW_ORIGIN || '*'

if (!XUMM_API_KEY || !XUMM_API_SECRET) {
  // eslint-disable-next-line no-console
  console.error('Missing XUMM_API_KEY or XUMM_API_SECRET')
  process.exit(1)
}

const app = express()

app.use(express.json({ limit: '1mb' }))
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', ALLOW_ORIGIN)
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization')
  if (req.method === 'OPTIONS') {
    res.status(200).send('')
    return
  }
  next()
})

const buildHeaders = () => ({
  'Content-Type': 'application/json',
  'x-api-key': XUMM_API_KEY,
  'x-api-secret': XUMM_API_SECRET,
})

const forward = async (path, method, body, res) => {
  const response = await fetch(`${XUMM_API_BASE}${path}`, {
    method,
    headers: buildHeaders(),
    body: body ? JSON.stringify(body) : undefined,
  })

  const text = await response.text()
  res.status(response.status)
  res.setHeader('Content-Type', response.headers.get('content-type') || 'application/json')
  res.send(text)
}

app.post('/xumm/ping', async (req, res) => {
  try {
    await forward('/ping', 'POST', req.body ?? {}, res)
  } catch (error) {
    res.status(500).json({ error: 'Failed to ping XUMM' })
  }
})

app.post('/xumm/payload', async (req, res) => {
  try {
    await forward('/payload', 'POST', req.body ?? {}, res)
  } catch (error) {
    res.status(500).json({ error: 'Failed to create XUMM payload' })
  }
})

app.get('/xumm/payload/:uuid', async (req, res) => {
  try {
    await forward(`/payload/${req.params.uuid}`, 'GET', null, res)
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch XUMM payload status' })
  }
})

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`XUMM proxy listening on :${PORT}`)
})
