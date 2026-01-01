const express = require('express')
const dotenv = require('dotenv')
const { mountFarmsApi } = require('./farmsApi')
const { initDb, isDbEnabled } = require('./db')

dotenv.config()

const PORT = Number(process.env.PORT || 3001)
const ALLOW_ORIGIN = process.env.FARMS_ALLOW_ORIGIN || process.env.CORS_ALLOW_ORIGIN || '*'

const app = express()

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

app.get('/', (req, res) => {
  res.json({ status: 'ok' })
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
