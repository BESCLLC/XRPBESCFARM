const express = require('express')
const fetch = require('node-fetch')
const dotenv = require('dotenv')
const path = require('path')
const fs = require('fs')
const BigNumber = require('bignumber.js')
const {
  Client,
  dropsToXrp,
  Wallet,
  getBalanceChanges,
  convertStringToHex,
  isValidXAddress,
  xAddressToClassicAddress,
} = require('xrpl')
const { initDb, withTransaction, isDbEnabled } = require('./db')

dotenv.config()

const FARM_ID = 'besc-xrp'
const BESC_SYMBOL = 'BESC'
const BESC_ISSUER = process.env.FARMS_BESC_ISSUER || process.env.REACT_APP_BESC_ISSUER || 'rUnjJv3iLPNWTAVXoNUfHD9CBm7tcwkjLa'
const BESC_CURRENCY =
  process.env.FARMS_BESC_CURRENCY ||
  process.env.REACT_APP_BESC_CURRENCY ||
  '4245534300000000000000000000000000000000'
const FARMS_REWARD_SEED = process.env.FARMS_REWARD_SEED || ''
const FARMS_ESCROW_SEED = process.env.FARMS_ESCROW_SEED || FARMS_REWARD_SEED || ''
const FARMS_ESCROW_ADDRESS = process.env.FARMS_ESCROW_ADDRESS || ''
const FARM_SIGNAL_ADDRESS =
  process.env.FARMS_SIGNAL_ADDRESS || process.env.REACT_APP_FARMS_SIGNAL_ADDRESS || BESC_ISSUER
const EMISSION_PER_DAY = Number(
  process.env.FARMS_EMISSION_PER_DAY || process.env.REACT_APP_FARMS_EMISSION_PER_DAY || '1000',
)
const XRPL_WS = process.env.FARMS_XRPL_WS || process.env.REACT_APP_XRPL_WS || 'wss://xrplcluster.com'
const STORE_PATH = path.join(__dirname, 'farmsStore.json')
const DB_SCALE = 18

const BITHOMP_API_BASE = process.env.BITHOMP_API_BASE || 'https://bithomp.com/api/v2'
const BITHOMP_API_KEY = process.env.BITHOMP_API_KEY || process.env.REACT_APP_BITHOMP_API_KEY || ''
const BITHOMP_API_KEY_HEADER = process.env.BITHOMP_API_KEY_HEADER || 'x-bithomp-token'

const RIPPLE_DATA_BASE = process.env.RIPPLE_DATA_API_BASE || 'https://data.ripple.com/v2'
const API_PREFIX = process.env.FARMS_API_PREFIX || process.env.REACT_APP_FARMS_API_PREFIX || '/api'

const CACHE_TTL_MS = 30 * 1000
let farmMetaCache = { data: null, fetchedAt: 0 }
let xrpUsdCache = { value: null, fetchedAt: 0 }

const logPrefix = '[farms]'
const logInfo = (...args) => console.log(logPrefix, ...args)
const logWarn = (...args) => console.warn(logPrefix, ...args)
const logError = (...args) => console.error(logPrefix, ...args)

const buildHeaders = () => {
  const headers = {}
  if (BITHOMP_API_KEY) {
    headers[BITHOMP_API_KEY_HEADER] = BITHOMP_API_KEY
  }
  return headers
}

const now = () => Date.now()

const readStore = () => {
  try {
    if (!fs.existsSync(STORE_PATH)) {
      return { farms: {} }
    }
    const raw = fs.readFileSync(STORE_PATH, 'utf8')
    return JSON.parse(raw)
  } catch (error) {
    return { farms: {} }
  }
}

const writeStore = (store) => {
  try {
    fs.writeFileSync(STORE_PATH, JSON.stringify(store, null, 2))
  } catch (error) {
    // ignore persistence errors
  }
}

const getFarmState = (store) => {
  if (!store.farms[FARM_ID]) {
    store.farms[FARM_ID] = {
      totalStaked: 0,
      rewardPerTokenStored: 0,
      lastUpdate: now(),
      users: {},
      processedTxs: {},
    }
  }
  return store.farms[FARM_ID]
}

const updateFarmRewards = (farmState) => {
  const current = now()
  const elapsedSeconds = Math.max((current - farmState.lastUpdate) / 1000, 0)
  if (elapsedSeconds > 0 && farmState.totalStaked > 0) {
    const rewardRate = Number.isFinite(EMISSION_PER_DAY) ? EMISSION_PER_DAY / 86400 : 0
    farmState.rewardPerTokenStored += (elapsedSeconds * rewardRate) / farmState.totalStaked
  }
  farmState.lastUpdate = current
}

const updateUserRewards = (farmState, account) => {
  if (!farmState.users[account]) {
    farmState.users[account] = {
      balance: 0,
      rewards: 0,
      rewardPerTokenPaid: farmState.rewardPerTokenStored,
    }
  }
  const user = farmState.users[account]
  const earned = user.balance * (farmState.rewardPerTokenStored - user.rewardPerTokenPaid)
  if (earned > 0) {
    user.rewards += earned
  }
  user.rewardPerTokenPaid = farmState.rewardPerTokenStored
  return user
}

const toDb = (value) => new BigNumber(value || 0).toFixed(DB_SCALE)

const fromDb = (value) => new BigNumber(value || 0)

const ensureFarmStateRow = async (client) => {
  await client.query(
    `
      INSERT INTO farm_state (farm_id, total_staked, reward_per_token, last_update)
      VALUES ($1, 0, 0, $2)
      ON CONFLICT (farm_id) DO NOTHING
    `,
    [FARM_ID, now()],
  )
  const result = await client.query(
    `
      SELECT farm_id, total_staked, reward_per_token, last_update
      FROM farm_state
      WHERE farm_id = $1
      FOR UPDATE
    `,
    [FARM_ID],
  )
  return result.rows[0]
}

const ensureUserRow = async (client, account, rewardPerToken) => {
  await client.query(
    `
      INSERT INTO farm_users (farm_id, account, balance, rewards, reward_per_token_paid)
      VALUES ($1, $2, 0, 0, $3)
      ON CONFLICT (farm_id, account) DO NOTHING
    `,
    [FARM_ID, account, toDb(rewardPerToken)],
  )
  const result = await client.query(
    `
      SELECT farm_id, account, balance, rewards, reward_per_token_paid
      FROM farm_users
      WHERE farm_id = $1 AND account = $2
      FOR UPDATE
    `,
    [FARM_ID, account],
  )
  return result.rows[0]
}

const applyFarmRewardsDb = (farmRow) => {
  const current = now()
  const lastUpdate = Number(farmRow.last_update || 0)
  const elapsedSeconds = Math.max((current - lastUpdate) / 1000, 0)
  const totalStaked = fromDb(farmRow.total_staked)
  let rewardPerToken = fromDb(farmRow.reward_per_token)
  const emission = new BigNumber(EMISSION_PER_DAY || 0)
  const rewardRate = emission.isFinite() ? emission.div(86400) : new BigNumber(0)
  if (elapsedSeconds > 0 && totalStaked.gt(0)) {
    rewardPerToken = rewardPerToken.plus(rewardRate.times(elapsedSeconds).div(totalStaked))
  }
  return { totalStaked, rewardPerToken, lastUpdate: current }
}

const applyUserRewardsDb = (userRow, rewardPerToken) => {
  const balance = fromDb(userRow.balance)
  const rewards = fromDb(userRow.rewards)
  const rewardPerTokenPaid = fromDb(userRow.reward_per_token_paid)
  const earned = balance.times(rewardPerToken.minus(rewardPerTokenPaid))
  const nextRewards = rewards.plus(earned)
  return { balance, rewards: nextRewards, rewardPerTokenPaid: rewardPerToken }
}

const recordActionDb = async (client, account, action, amount, txHash) => {
  const result = await client.query(
    `
      INSERT INTO farm_actions (tx_hash, farm_id, account, action, amount)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (tx_hash) DO NOTHING
    `,
    [txHash, FARM_ID, account, action, toDb(amount)],
  )
  return result.rowCount > 0
}

const decodeHex = (hex) => {
  if (!hex) return ''
  if (typeof hex !== 'string') return ''
  const trimmed = hex.trim()
  const isHex = /^[0-9a-fA-F]+$/.test(trimmed)
  if (!isHex || trimmed.length % 2 !== 0) {
    return trimmed
  }
  try {
    return Buffer.from(trimmed, 'hex').toString('utf8')
  } catch (error) {
    return ''
  }
}

let escrowWalletCache = null

const getEscrowWallet = () => {
  if (!FARMS_ESCROW_SEED) {
    throw new Error('Missing FARMS_ESCROW_SEED for LP custody')
  }
  if (!escrowWalletCache) {
    const wallet = Wallet.fromSeed(FARMS_ESCROW_SEED)
    if (FARMS_ESCROW_ADDRESS && wallet.classicAddress !== FARMS_ESCROW_ADDRESS) {
      throw new Error('FARMS_ESCROW_SEED does not match FARMS_ESCROW_ADDRESS')
    }
    escrowWalletCache = wallet
  }
  return escrowWalletCache
}

const getEscrowAddress = () => {
  if (FARMS_ESCROW_ADDRESS) return FARMS_ESCROW_ADDRESS
  if (!FARMS_ESCROW_SEED) return ''
  return getEscrowWallet().classicAddress
}

const parseLooseMemo = (data) => {
  if (!data) return null
  const actionMatch = data.match(/action"\s*:\s*"([^"]+)"/i) || data.match(/action\s*[:=]\s*"?([a-z]+)"/i)
  const farmMatch =
    data.match(/farmid"\s*:\s*"([^"]+)"/i) ||
    data.match(/farmld"\s*:\s*"([^"]+)"/i) ||
    data.match(/farmid\s*[:=]\s*"?([a-z0-9-_]+)"/i)
  if (!actionMatch || !farmMatch) return null
  const amountMatch =
    data.match(/amount"\s*:\s*"([^"]+)"/i) || data.match(/amount\s*[:=]\s*"?([0-9.]+)/i)
  const typeMatch = data.match(/type"\s*:\s*"([^"]+)"/i)
  const normalized = {
    action: actionMatch[1],
    farmId: farmMatch[1],
  }
  if (amountMatch?.[1]) {
    const cleaned = amountMatch[1].replace(/[^0-9.]/g, '')
    if (cleaned) {
      normalized.amount = cleaned
    }
  }
  if (typeMatch?.[1]) {
    normalized.type = typeMatch[1]
  }
  return normalized
}

const parseMemo = (memos = []) => {
  for (const memo of memos) {
    const data = decodeHex(memo?.Memo?.MemoData)
    const type = decodeHex(memo?.Memo?.MemoType)
    if (!data) continue
    try {
      const parsed = JSON.parse(data)
      const normalized = { ...parsed, type: type || parsed.type }
      if (!normalized.farmId && normalized.farmld) {
        normalized.farmId = normalized.farmld
      }
      if (typeof normalized.amount === 'string') {
        const cleaned = normalized.amount.replace(/[^0-9.]/g, '')
        normalized.amount = cleaned || normalized.amount
      }
      return normalized
    } catch (error) {
      const fallback = parseLooseMemo(data)
      if (!fallback) {
        continue
      }
      const normalized = { ...fallback, type: type || fallback.type }
      if (!normalized.farmId && normalized.farmld) {
        normalized.farmId = normalized.farmld
      }
      if (typeof normalized.amount === 'string') {
        const cleaned = normalized.amount.replace(/[^0-9.]/g, '')
        normalized.amount = cleaned || normalized.amount
      }
      return normalized
    }
  }
  return null
}

let xrplClientPromise = null
let indexerStarted = false
let indexerInFlight = false
const FARMS_INDEX_INTERVAL_MS = Number(process.env.FARMS_INDEX_INTERVAL_MS || '30000')

const getXrplClient = async () => {
  if (!xrplClientPromise) {
    xrplClientPromise = (async () => {
      const client = new Client(XRPL_WS)
      await client.connect()
      return client
    })()
  }
  return xrplClientPromise
}

const fetchXrpUsd = async () => {
  if (xrpUsdCache.value && now() - xrpUsdCache.fetchedAt < CACHE_TTL_MS) {
    return xrpUsdCache.value
  }
  try {
    const response = await fetch(`${RIPPLE_DATA_BASE}/exchange_rates/XRP/USD`)
    if (response.ok) {
      const data = await response.json()
      const rate = Number(data?.rate || data?.result?.rate || data?.exchange_rates?.[0]?.rate)
      if (Number.isFinite(rate) && rate > 0) {
        xrpUsdCache = { value: rate, fetchedAt: now() }
        return rate
      }
    }
  } catch (error) {
    // ignore
  }
  return null
}

const fetchBescStats = async () => {
  const url = `${BITHOMP_API_BASE}/trustlines/token/${BESC_ISSUER}/${BESC_CURRENCY}?statistics=true&convertCurrencies=usd`
  const response = await fetch(url, { headers: buildHeaders() })
  if (!response.ok) {
    throw new Error('Failed to fetch token stats')
  }
  return response.json()
}

const fetchAmmData = async () => {
  const url = `${BITHOMP_API_BASE}/amms/search?currency=XRP&currency2=${BESC_CURRENCY}&issuer2=${BESC_ISSUER}`
  const response = await fetch(url, { headers: buildHeaders() })
  if (!response.ok) {
    throw new Error('Failed to fetch AMM data')
  }
  const data = await response.json()
  return data?.amms?.[0] || null
}

const loadFarmMeta = async () => {
  if (farmMetaCache.data && now() - farmMetaCache.fetchedAt < CACHE_TTL_MS) {
    return farmMetaCache.data
  }

  const [ammResult, statsResult, xrpUsd] = await Promise.allSettled([
    fetchAmmData(),
    fetchBescStats(),
    fetchXrpUsd(),
  ])

  const amm = ammResult.status === 'fulfilled' ? ammResult.value : null
  const stats = statsResult.status === 'fulfilled' ? statsResult.value : null

  let reserveXrp = 0
  let reserveBesc = 0
  if (amm?.amount && typeof amm.amount === 'string') {
    reserveXrp = Number(dropsToXrp(amm.amount))
    reserveBesc = amm?.amount2?.value ? Number(amm.amount2.value) : 0
  } else if (amm?.amount2 && typeof amm.amount2 === 'string') {
    reserveXrp = Number(dropsToXrp(amm.amount2))
    reserveBesc = amm?.amount?.value ? Number(amm.amount.value) : 0
  } else {
    reserveXrp = amm?.amount ? Number(dropsToXrp(amm.amount)) : 0
    reserveBesc = amm?.amount2?.value ? Number(amm.amount2.value) : 0
  }
  const priceFromStats =
    Number(stats?.statistics?.priceXrpSpot ?? stats?.statistics?.priceXrp ?? stats?.priceXrpSpot ?? stats?.priceXrp) ||
    0
  const priceFromAmm = reserveXrp > 0 && reserveBesc > 0 ? reserveXrp / reserveBesc : 0
  const priceXrp = priceFromStats || priceFromAmm
  const rewardPriceUsd = xrpUsd && priceXrp ? priceXrp * xrpUsd : null
  const liquidityXrp = reserveXrp + reserveBesc * priceXrp
  const liquidityUsd = xrpUsd ? liquidityXrp * xrpUsd : null
  const lpToken = amm?.lpTokenBalance
  const lpTokenSupply = lpToken?.value || null
  const emissionPerDay = Number.isFinite(EMISSION_PER_DAY) ? EMISSION_PER_DAY : 0
  const apr =
    liquidityXrp > 0 && priceXrp > 0
      ? (emissionPerDay * 365 * priceXrp * 100) / liquidityXrp
      : 0

  const meta = {
    id: FARM_ID,
    name: 'BESC/XRP',
    apr,
    multiplier: '3X',
    liquidity: liquidityUsd ?? liquidityXrp,
    liquidityUsd,
    liquidityXrp,
    rewardSymbol: BESC_SYMBOL,
    rewardPriceUsd,
    stakingSymbol: 'BESC-XRP LP',
    lpTokenCurrency: lpToken?.currency || null,
    lpTokenIssuer: lpToken?.issuer || null,
    lpTokenSymbol: 'BESC/XRP LP',
    lpTokenSupply,
    tradingFee: amm?.tradingFee ?? null,
    rewardPerDay: emissionPerDay,
  }

  farmMetaCache = { data: meta, fetchedAt: now() }
  return meta
}

const parsePaymentAmount = (txFields) => {
  const rawAmount = txFields.Amount || txFields.amount
  if (!rawAmount) return null
  if (typeof rawAmount === 'string') {
    return { type: 'xrp', value: rawAmount }
  }
  if (typeof rawAmount === 'object') {
    return {
      type: 'iou',
      currency: rawAmount.currency || rawAmount.Currency,
      issuer: rawAmount.issuer || rawAmount.Issuer,
      value: rawAmount.value || rawAmount.Value,
    }
  }
  if (typeof rawAmount === 'number') {
    return { type: 'xrp', value: rawAmount.toString() }
  }
  return null
}

const normalizeAddress = (address) => {
  if (!address) return ''
  if (isValidXAddress(address)) {
    try {
      return xAddressToClassicAddress(address).classicAddress
    } catch (error) {
      return address
    }
  }
  return address
}

const normalizeCurrency = (currency) => {
  if (!currency) return ''
  const upper = String(currency).toUpperCase()
  if (upper.length <= 3) {
    return convertStringToHex(upper).padEnd(40, '0')
  }
  return upper.padEnd(40, '0')
}

const fetchValidatedTx = async (client, txHash) => {
  const maxAttempts = 12
  const delayMs = 1000
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      const response = await client.request({ command: 'tx', transaction: txHash })
      const tx = response.result
      if (tx?.validated) {
        return tx
      }
    } catch (error) {
      // ignore and retry
    }
    await new Promise((resolve) => setTimeout(resolve, delayMs))
  }
  throw new Error('Transaction not validated yet')
}

const getTxFields = (tx) => tx?.tx_json || tx?.transaction || tx?.tx || tx || {}

const getIouBalanceDelta = (meta, account, currency, issuer) => {
  if (!meta || !account || !currency || !issuer) return null
  try {
    const changes = getBalanceChanges(meta)
    const accountKey = normalizeAddress(account)
    const accountChanges = changes?.[accountKey]
    if (!Array.isArray(accountChanges)) return null
    const targetCurrency = normalizeCurrency(currency)
    const targetIssuer = normalizeAddress(issuer)
    const entry = accountChanges.find((change) => {
      const changeCurrency = normalizeCurrency(change.currency)
      const changeIssuer = normalizeAddress(change.issuer)
      return changeCurrency === targetCurrency && changeIssuer === targetIssuer
    })
    if (!entry?.value) return null
    const value = new BigNumber(entry.value)
    if (!value.isFinite() || value.lte(0)) return null
    return value
  } catch (error) {
    return null
  }
}

const processStakeTxEntry = async (entry, metaData, lpTokenCurrency, lpTokenIssuer, escrowAddress) => {
  const txFields = getTxFields(entry)
  const txHash = txFields.hash || entry?.hash
  if (!txHash) return false
  if ((txFields.TransactionType || txFields.transactionType) !== 'Payment') return false
  const destination = normalizeAddress(txFields.Destination || txFields.destination)
  if (destination !== normalizeAddress(escrowAddress)) return false

  const memo = parseMemo(txFields.Memos || txFields.memos)
  const memoAction = memo?.action ? String(memo.action).toLowerCase() : ''
  const memoFarmId = memo?.farmId ? String(memo.farmId).toLowerCase() : ''
  if (memoAction !== 'stake' || memoFarmId !== String(FARM_ID).toLowerCase()) {
    return false
  }

  const delta = getIouBalanceDelta(metaData, escrowAddress, lpTokenCurrency, lpTokenIssuer)
  const amount = parsePaymentAmount(txFields)
  const stakeValue =
    delta ||
    (amount &&
      amount.type === 'iou' &&
      normalizeCurrency(amount.currency) === normalizeCurrency(lpTokenCurrency) &&
      normalizeAddress(amount.issuer) === normalizeAddress(lpTokenIssuer) &&
      new BigNumber(amount.value))
  if (!stakeValue || !stakeValue.isFinite() || stakeValue.lte(0)) {
    return false
  }

  const account = txFields.Account || txFields.account
  if (!account) return false

  if (isDbEnabled()) {
    await applyStakeDb({ account, action: 'stake', amount: stakeValue.toString(), txHash })
    logInfo('Indexed stake', { account, txHash, amount: stakeValue.toString() })
    return true
  }

  const store = readStore()
  const farmState = getFarmState(store)
  if (farmState.processedTxs[txHash]) {
    return false
  }
  updateFarmRewards(farmState)
  const user = updateUserRewards(farmState, account)
  user.balance += stakeValue.toNumber()
  farmState.totalStaked += stakeValue.toNumber()
  farmState.processedTxs[txHash] = true
  writeStore(store)
  logInfo('Indexed stake (file store)', { account, txHash, amount: stakeValue.toString() })
  return true
}

const indexEscrowStakes = async () => {
  if (indexerInFlight) {
    return { skipped: true, reason: 'in_flight' }
  }
  indexerInFlight = true
  let scanned = 0
  let recorded = 0
  try {
    const escrowAddress = getEscrowAddress()
    if (!escrowAddress) return { skipped: true, reason: 'missing_escrow' }
    const meta = await loadFarmMeta()
    if (!meta?.lpTokenCurrency || !meta?.lpTokenIssuer) {
      return { skipped: true, reason: 'missing_lp_token' }
    }
    const client = await getXrplClient()
    const response = await client.request({
      command: 'account_tx',
      account: escrowAddress,
      ledger_index_min: -1,
      ledger_index_max: -1,
      limit: 50,
    })
    const transactions = response?.result?.transactions || []
    scanned = transactions.length
    logInfo('Indexer scan', { escrowAddress, count: scanned })
    for (const entry of transactions) {
      if (entry?.validated === false) continue
      const tx = entry?.tx || entry
      const metaData = entry?.meta || entry?.metaData || tx?.meta
      const applied = await processStakeTxEntry(
        tx,
        metaData,
        meta.lpTokenCurrency,
        meta.lpTokenIssuer,
        escrowAddress,
      )
      if (applied) {
        recorded += 1
      }
    }
    return { scanned, recorded }
  } catch (error) {
    logError('Indexer error', error?.message || error)
    return { scanned, recorded, error: error?.message || String(error) }
  } finally {
    indexerInFlight = false
  }
}

const startEscrowIndexer = () => {
  if (indexerStarted) return
  if (!FARMS_INDEX_INTERVAL_MS || FARMS_INDEX_INTERVAL_MS <= 0) return
  indexerStarted = true
  logInfo('Indexer started', { intervalMs: FARMS_INDEX_INTERVAL_MS })
  setInterval(indexEscrowStakes, FARMS_INDEX_INTERVAL_MS)
  indexEscrowStakes()
}

const verifyAction = async ({ account, action, farmId, txHash, destinations, expectAmount }) => {
  if (!account || !action || !farmId || !txHash) {
    throw new Error('Missing request fields')
  }
  const client = await getXrplClient()
  const tx = await fetchValidatedTx(client, txHash)
  const txFields = getTxFields(tx)
  const txType = txFields.TransactionType || txFields.transactionType
  if (txType !== 'Payment') {
    throw new Error('Invalid transaction type')
  }
  const sender = normalizeAddress(txFields.Account || txFields.account)
  if (sender !== normalizeAddress(account)) {
    throw new Error('Account mismatch')
  }
  const destination = normalizeAddress(txFields.Destination || txFields.destination)
  const allowedDestinations = (destinations || [FARM_SIGNAL_ADDRESS, BESC_ISSUER])
    .filter(Boolean)
    .map((addr) => normalizeAddress(addr))
  if (!allowedDestinations.includes(destination)) {
    throw new Error('Invalid destination')
  }
  const memo = parseMemo(txFields.Memos || txFields.memos)
  const memoAction = memo?.action ? String(memo.action).toLowerCase() : ''
  const memoFarmId = memo?.farmId ? String(memo.farmId).toLowerCase() : ''
  const targetFarmId = String(farmId).toLowerCase()
  const targetAction = String(action).toLowerCase()
  if (!memo || memoAction !== targetAction || memoFarmId !== targetFarmId) {
    throw new Error('Missing farm memo')
  }
  const meta = tx?.meta || tx?.metaData || txFields?.meta
  let amount = parsePaymentAmount(txFields)
  if (expectAmount?.type === 'xrp') {
    if (!amount || amount.type !== 'xrp') {
      throw new Error('Invalid payment asset')
    }
  }
  if (expectAmount?.type === 'iou') {
    const expectedCurrency = expectAmount.currency ? normalizeCurrency(expectAmount.currency) : null
    const expectedIssuer = expectAmount.issuer ? normalizeAddress(expectAmount.issuer) : null
    const amountMatches =
      amount &&
      amount.type === 'iou' &&
      (!expectedCurrency || normalizeCurrency(amount.currency) === expectedCurrency) &&
      (!expectedIssuer || normalizeAddress(amount.issuer) === expectedIssuer)
  if (!amountMatches) {
    const delta = getIouBalanceDelta(meta, destination, expectedCurrency, expectedIssuer)
    if (!delta) {
      logWarn('Stake asset mismatch', {
        destination,
        expectedCurrency,
        expectedIssuer,
        amount,
        hasMeta: Boolean(meta),
      })
      throw new Error('Invalid payment asset')
    }
    amount = {
      type: 'iou',
      currency: expectAmount.currency,
      issuer: expectAmount.issuer,
      value: delta.toString(),
    }
  }
  }
  return { memo, amount, txFields, meta }
}

const hasTrustline = async (account, issuer, currency) => {
  const client = await getXrplClient()
  const response = await client.request({ command: 'account_lines', account })
  return response.result.lines.some(
    (line) => line.account === issuer && line.currency === currency,
  )
}

const getTokenBalance = async (account, issuer, currency) => {
  const client = await getXrplClient()
  const response = await client.request({ command: 'account_lines', account })
  const line = response.result.lines.find(
    (item) => item.account === issuer && item.currency === currency,
  )
  return Number(line?.balance || 0)
}

const getRewardWalletStatus = async () => {
  if (!FARMS_REWARD_SEED) {
    return { configured: false, reason: 'Missing FARMS_REWARD_SEED' }
  }
  const wallet = Wallet.fromSeed(FARMS_REWARD_SEED)
  const address = wallet.classicAddress
  const isIssuer = address === BESC_ISSUER
  if (isIssuer) {
    return { configured: true, address, isIssuer, hasTrustline: true, balance: null }
  }
  const hasLine = await hasTrustline(address, BESC_ISSUER, BESC_CURRENCY)
  const balance = await getTokenBalance(address, BESC_ISSUER, BESC_CURRENCY)
  return { configured: true, address, isIssuer, hasTrustline: hasLine, balance }
}

const getEscrowWalletStatus = async (lpCurrency, lpIssuer) => {
  const address = getEscrowAddress()
  if (!address) {
    return { configured: false, reason: 'Missing FARMS_ESCROW_ADDRESS' }
  }
  const hasSeed = Boolean(FARMS_ESCROW_SEED)
  if (!lpCurrency || !lpIssuer) {
    return { configured: true, address, hasSeed }
  }
  const hasLine = await hasTrustline(address, lpIssuer, lpCurrency)
  const balance = await getTokenBalance(address, lpIssuer, lpCurrency)
  return { configured: true, address, hasSeed, hasTrustline: hasLine, balance }
}

const formatValue = (value) => {
  const amount = new BigNumber(value || 0)
  if (!amount.isFinite() || amount.lte(0)) return '0'
  const fixed = amount.toFixed(6)
  return fixed.replace(/\.?0+$/, '')
}

const formatTokenAmount = (value) => {
  const amount = new BigNumber(value || 0)
  if (!amount.isFinite() || amount.lte(0)) return '0'
  const fixed = amount.toFixed(16)
  return fixed.replace(/\.?0+$/, '')
}

const sendRewardPayment = async (account, amount) => {
  if (!FARMS_REWARD_SEED) {
    throw new Error('Missing FARMS_REWARD_SEED for reward payouts')
  }
  const amountBn = new BigNumber(amount || 0)
  if (!amountBn.isFinite() || amountBn.lte(0)) {
    return { hash: null }
  }
  const client = await getXrplClient()
  const wallet = Wallet.fromSeed(FARMS_REWARD_SEED)

  const hasLine = await hasTrustline(account, BESC_ISSUER, BESC_CURRENCY)
  if (!hasLine) {
    throw new Error('User missing BESC trustline')
  }

  if (wallet.classicAddress !== BESC_ISSUER) {
    const treasuryBalance = await getTokenBalance(wallet.classicAddress, BESC_ISSUER, BESC_CURRENCY)
    if (treasuryBalance < amountBn.toNumber()) {
      throw new Error('Treasury has insufficient BESC balance')
    }
  }

  const tx = {
    TransactionType: 'Payment',
    Account: wallet.classicAddress,
    Destination: account,
    Amount: {
      currency: BESC_CURRENCY,
      issuer: BESC_ISSUER,
      value: formatValue(amountBn),
    },
    Memos: [
      {
        Memo: {
          MemoType: Buffer.from('FARM').toString('hex').toUpperCase(),
          MemoData: Buffer.from(JSON.stringify({ farmId: FARM_ID, type: 'reward' })).toString('hex').toUpperCase(),
        },
      },
    ],
  }

  const prepared = await client.autofill(tx)
  const signed = wallet.sign(prepared)
  const result = await client.submitAndWait(signed.tx_blob)
  const outcome = result?.result?.meta?.TransactionResult
  if (outcome !== 'tesSUCCESS') {
    throw new Error(`Reward payout failed: ${outcome || 'unknown'}`)
  }
  return { hash: signed.hash }
}

const sendLpPayment = async (account, amount, currency, issuer) => {
  if (!currency || !issuer) {
    throw new Error('Missing LP token details')
  }
  const amountBn = new BigNumber(amount || 0)
  if (!amountBn.isFinite() || amountBn.lte(0)) {
    throw new Error('Invalid unstake amount')
  }
  const client = await getXrplClient()
  const wallet = getEscrowWallet()

  const hasLine = await hasTrustline(wallet.classicAddress, issuer, currency)
  if (!hasLine) {
    throw new Error('Escrow wallet missing LP trustline')
  }
  const balance = await getTokenBalance(wallet.classicAddress, issuer, currency)
  if (balance < amountBn.toNumber()) {
    throw new Error('Escrow wallet has insufficient LP balance')
  }

  const tx = {
    TransactionType: 'Payment',
    Account: wallet.classicAddress,
    Destination: account,
    Amount: {
      currency,
      issuer,
      value: formatTokenAmount(amountBn),
    },
    Memos: [
      {
        Memo: {
          MemoType: Buffer.from('FARM').toString('hex').toUpperCase(),
          MemoData: Buffer.from(JSON.stringify({ farmId: FARM_ID, action: 'unstake' }))
            .toString('hex')
            .toUpperCase(),
        },
      },
    ],
  }

  const prepared = await client.autofill(tx)
  const signed = wallet.sign(prepared)
  const result = await client.submitAndWait(signed.tx_blob)
  const outcome = result?.result?.meta?.TransactionResult
  if (outcome !== 'tesSUCCESS') {
    throw new Error(`LP payout failed: ${outcome || 'unknown'}`)
  }
  return { hash: signed.hash }
}

const loadFarmSnapshotDb = async (account) => {
  await initDb()
  return withTransaction(async (client) => {
    const farmRow = await ensureFarmStateRow(client)
    const farmState = applyFarmRewardsDb(farmRow)
    await client.query(
      `
        UPDATE farm_state
        SET reward_per_token = $1, last_update = $2
        WHERE farm_id = $3
      `,
      [toDb(farmState.rewardPerToken), farmState.lastUpdate, FARM_ID],
    )

    let userData = {}
    if (account) {
      const userRow = await ensureUserRow(client, account, farmState.rewardPerToken)
      const userState = applyUserRewardsDb(userRow, farmState.rewardPerToken)
      await client.query(
        `
          UPDATE farm_users
          SET rewards = $1, reward_per_token_paid = $2
          WHERE farm_id = $3 AND account = $4
        `,
        [toDb(userState.rewards), toDb(userState.rewardPerTokenPaid), FARM_ID, account],
      )
      userData = {
        stakedBalance: formatValue(userState.balance),
        earned: formatValue(userState.rewards),
      }
    }

    return {
      totalStaked: formatValue(farmState.totalStaked),
      userData,
    }
  })
}

const applyStakeDb = async ({ account, action, amount, txHash }) => {
  await initDb()
  const delta = new BigNumber(amount || 0)
  if (!delta.isFinite() || delta.lte(0)) {
    throw new Error('Invalid stake amount')
  }

  return withTransaction(async (client) => {
    const farmRow = await ensureFarmStateRow(client)
    const farmState = applyFarmRewardsDb(farmRow)
    const userRow = await ensureUserRow(client, account, farmState.rewardPerToken)
    const userState = applyUserRewardsDb(userRow, farmState.rewardPerToken)

    const applied = await recordActionDb(client, account, action, delta, txHash)

    let totalStaked = farmState.totalStaked
    if (applied) {
      if (action === 'stake') {
        userState.balance = userState.balance.plus(delta)
        totalStaked = totalStaked.plus(delta)
      } else {
        const actual = BigNumber.min(delta, userState.balance)
        userState.balance = userState.balance.minus(actual)
        totalStaked = BigNumber.max(totalStaked.minus(actual), new BigNumber(0))
      }
    }

    await client.query(
      `
        UPDATE farm_state
        SET total_staked = $1, reward_per_token = $2, last_update = $3
        WHERE farm_id = $4
      `,
      [toDb(totalStaked), toDb(farmState.rewardPerToken), farmState.lastUpdate, FARM_ID],
    )
    await client.query(
      `
        UPDATE farm_users
        SET balance = $1, rewards = $2, reward_per_token_paid = $3
        WHERE farm_id = $4 AND account = $5
      `,
      [toDb(userState.balance), toDb(userState.rewards), toDb(userState.rewardPerTokenPaid), FARM_ID, account],
    )

    return {
      stakedBalance: formatValue(userState.balance),
      totalStaked: formatValue(totalStaked),
    }
  })
}

const applyUnstakeDb = async ({ account, amount, txHash, lpTokenCurrency, lpTokenIssuer }) => {
  await initDb()
  const requested = new BigNumber(amount || 0)
  if (!requested.isFinite() || requested.lte(0)) {
    throw new Error('Invalid unstake amount')
  }

  return withTransaction(async (client) => {
    const farmRow = await ensureFarmStateRow(client)
    const farmState = applyFarmRewardsDb(farmRow)
    const userRow = await ensureUserRow(client, account, farmState.rewardPerToken)
    const userState = applyUserRewardsDb(userRow, farmState.rewardPerToken)

    const applied = await recordActionDb(client, account, 'unstake', requested, txHash)
    if (!applied) {
      return {
        stakedBalance: formatValue(userState.balance),
        totalStaked: formatValue(farmState.totalStaked),
        lpTxHash: null,
      }
    }

    const actual = BigNumber.min(requested, userState.balance)
    if (actual.lte(0)) {
      throw new Error('Nothing to unstake')
    }

    const payout = await sendLpPayment(account, actual, lpTokenCurrency, lpTokenIssuer)

    userState.balance = userState.balance.minus(actual)
    const totalStaked = BigNumber.max(farmState.totalStaked.minus(actual), new BigNumber(0))

    await client.query(
      `
        UPDATE farm_state
        SET total_staked = $1, reward_per_token = $2, last_update = $3
        WHERE farm_id = $4
      `,
      [toDb(totalStaked), toDb(farmState.rewardPerToken), farmState.lastUpdate, FARM_ID],
    )
    await client.query(
      `
        UPDATE farm_users
        SET balance = $1, rewards = $2, reward_per_token_paid = $3
        WHERE farm_id = $4 AND account = $5
      `,
      [toDb(userState.balance), toDb(userState.rewards), toDb(userState.rewardPerTokenPaid), FARM_ID, account],
    )

    return {
      stakedBalance: formatValue(userState.balance),
      totalStaked: formatValue(totalStaked),
      lpTxHash: payout?.hash,
    }
  })
}

const prepareClaimDb = async ({ account, txHash }) => {
  await initDb()
  return withTransaction(async (client) => {
    const farmRow = await ensureFarmStateRow(client)
    const farmState = applyFarmRewardsDb(farmRow)
    const userRow = await ensureUserRow(client, account, farmState.rewardPerToken)
    const userState = applyUserRewardsDb(userRow, farmState.rewardPerToken)

    if (userState.rewards.lte(0)) {
      throw new Error('No rewards to claim')
    }

    const inserted = await client.query(
      `
        INSERT INTO farm_claims (tx_hash, farm_id, account, status, reward_amount)
        VALUES ($1, $2, $3, 'pending', $4)
        ON CONFLICT (tx_hash) DO NOTHING
      `,
      [txHash, FARM_ID, account, toDb(userState.rewards)],
    )

    if (!inserted.rowCount) {
      throw new Error('Claim transaction already processed')
    }

    await client.query(
      `
        UPDATE farm_state
        SET reward_per_token = $1, last_update = $2
        WHERE farm_id = $3
      `,
      [toDb(farmState.rewardPerToken), farmState.lastUpdate, FARM_ID],
    )
    await client.query(
      `
        UPDATE farm_users
        SET rewards = $1, reward_per_token_paid = $2
        WHERE farm_id = $3 AND account = $4
      `,
      [toDb(userState.rewards), toDb(userState.rewardPerTokenPaid), FARM_ID, account],
    )

    return userState.rewards
  })
}

const finalizeClaimDb = async ({ account, txHash, claimable, rewardTxHash, status }) => {
  await initDb()
  return withTransaction(async (client) => {
    const farmRow = await ensureFarmStateRow(client)
    const farmState = applyFarmRewardsDb(farmRow)
    const userRow = await ensureUserRow(client, account, farmState.rewardPerToken)
    const userState = applyUserRewardsDb(userRow, farmState.rewardPerToken)
    const claimAmount = new BigNumber(claimable || 0)
    const remaining =
      status === 'paid'
        ? BigNumber.max(userState.rewards.minus(claimAmount), new BigNumber(0))
        : userState.rewards

    await client.query(
      `
        UPDATE farm_state
        SET reward_per_token = $1, last_update = $2
        WHERE farm_id = $3
      `,
      [toDb(farmState.rewardPerToken), farmState.lastUpdate, FARM_ID],
    )
    await client.query(
      `
        UPDATE farm_users
        SET rewards = $1, reward_per_token_paid = $2
        WHERE farm_id = $3 AND account = $4
      `,
      [toDb(remaining), toDb(userState.rewardPerTokenPaid), FARM_ID, account],
    )
    await client.query(
      `
        UPDATE farm_claims
        SET status = $1, reward_tx_hash = $2, updated_at = NOW()
        WHERE tx_hash = $3
      `,
      [status, rewardTxHash || null, txHash],
    )

    return remaining
  })
}

const mountFarmsApi = (app) => {
  app.use(express.json({ limit: '1mb' }))

  app.get(`${API_PREFIX}/farms`, async (req, res) => {
    try {
      if (isDbEnabled()) {
        const account = req.query.account
        const snapshot = await loadFarmSnapshotDb(account)
        const meta = await loadFarmMeta()
        res.json({
          farms: [
            {
              ...meta,
              ...snapshot.userData,
              totalStaked: snapshot.totalStaked,
            },
          ],
        })
        return
      }
      const meta = await loadFarmMeta()
      const store = readStore()
      const farmState = getFarmState(store)
      updateFarmRewards(farmState)

      const account = req.query.account
      let userData = {}
      if (account) {
        const user = updateUserRewards(farmState, account)
        userData = {
          stakedBalance: user.balance.toString(),
          earned: user.rewards.toString(),
        }
      }
      writeStore(store)
      res.json({ farms: [{ ...meta, ...userData, totalStaked: farmState.totalStaked.toString() }] })
    } catch (error) {
      res.status(500).json({ error: error.message || 'Failed to load farms' })
    }
  })

  app.get(`${API_PREFIX}/farms/status`, async (req, res) => {
    try {
      const meta = await loadFarmMeta()
      const rewardWallet = await getRewardWalletStatus()
      const escrowWallet = await getEscrowWalletStatus(meta?.lpTokenCurrency, meta?.lpTokenIssuer)
      let user = null
      const account = req.query.account
      if (account) {
        const userHasTrustline = await hasTrustline(account, BESC_ISSUER, BESC_CURRENCY)
        const userBalance = await getTokenBalance(account, BESC_ISSUER, BESC_CURRENCY)
        user = { account, hasTrustline: userHasTrustline, balance: userBalance }
      }
      res.json({
        rewardWallet,
        escrowWallet,
        user,
        signalAddress: FARM_SIGNAL_ADDRESS,
        issuer: BESC_ISSUER,
        currency: BESC_CURRENCY,
        lpTokenCurrency: meta?.lpTokenCurrency || null,
        lpTokenIssuer: meta?.lpTokenIssuer || null,
      })
    } catch (error) {
      res.status(500).json({ error: error.message || 'Failed to load farm status' })
    }
  })

  app.post(`${API_PREFIX}/farms/stake`, async (req, res) => {
    try {
      const { account, farmId, amount, txHash } = req.body || {}
      const meta = await loadFarmMeta()
      if (!meta?.lpTokenCurrency || !meta?.lpTokenIssuer) {
        throw new Error('LP token not available')
      }
      const escrowAddress = getEscrowAddress()
      if (!escrowAddress) {
        throw new Error('Missing FARMS_ESCROW_ADDRESS')
      }
      const verified = await verifyAction({
        account,
        action: 'stake',
        farmId,
        txHash,
        destinations: [escrowAddress],
        expectAmount: { type: 'iou', currency: meta.lpTokenCurrency, issuer: meta.lpTokenIssuer },
      })
      const stakeDelta = getIouBalanceDelta(
        verified?.meta,
        escrowAddress,
        meta.lpTokenCurrency,
        meta.lpTokenIssuer,
      )
      if (!stakeDelta) {
        throw new Error('Unable to verify escrow balance delta')
      }
      const stakeAmount = stakeDelta.toString()
      if (isDbEnabled()) {
        const result = await applyStakeDb({ account, action: 'stake', amount: stakeAmount, txHash })
        logInfo('Stake recorded', { account, txHash, amount: stakeAmount })
        res.json(result)
        return
      }
      const stakeNumber = Number(stakeAmount || 0)
      if (!Number.isFinite(stakeNumber) || stakeNumber <= 0) {
        throw new Error('Invalid stake amount')
      }
      const store = readStore()
      const farmState = getFarmState(store)
      updateFarmRewards(farmState)
      const user = updateUserRewards(farmState, account)
      user.balance += stakeNumber
      farmState.totalStaked += stakeNumber
      writeStore(store)
      logInfo('Stake recorded (file store)', { account, txHash, amount: stakeAmount })
      res.json({ stakedBalance: user.balance.toString(), totalStaked: farmState.totalStaked.toString() })
    } catch (error) {
      logWarn('Stake failed', { txHash: req?.body?.txHash, error: error?.message || error })
      res.status(500).json({ error: error.message || 'Failed to stake' })
    }
  })

  app.post(`${API_PREFIX}/farms/unstake`, async (req, res) => {
    try {
      const { account, farmId, amount, txHash } = req.body || {}
      const meta = await loadFarmMeta()
      if (!meta?.lpTokenCurrency || !meta?.lpTokenIssuer) {
        throw new Error('LP token not available')
      }
      const signalAddress = FARM_SIGNAL_ADDRESS || getEscrowAddress() || BESC_ISSUER
      const verified = await verifyAction({
        account,
        action: 'unstake',
        farmId,
        txHash,
        destinations: [signalAddress],
        expectAmount: { type: 'xrp' },
      })
      const unstakeAmount = verified?.memo?.amount || amount || 0
      if (isDbEnabled()) {
        const result = await applyUnstakeDb({
          account,
          amount: unstakeAmount,
          txHash,
          lpTokenCurrency: meta.lpTokenCurrency,
          lpTokenIssuer: meta.lpTokenIssuer,
        })
        logInfo('Unstake recorded', { account, txHash, amount: unstakeAmount, lpTxHash: result?.lpTxHash })
        res.json(result)
        return
      }
      const unstakeNumber = Number(unstakeAmount || 0)
      if (!Number.isFinite(unstakeNumber) || unstakeNumber <= 0) {
        throw new Error('Invalid unstake amount')
      }
      const store = readStore()
      const farmState = getFarmState(store)
      updateFarmRewards(farmState)
      const user = updateUserRewards(farmState, account)
      const actual = Math.min(unstakeNumber, user.balance)
      if (actual <= 0) {
        throw new Error('Nothing to unstake')
      }
      const payout = await sendLpPayment(account, actual, meta.lpTokenCurrency, meta.lpTokenIssuer)
      user.balance -= actual
      farmState.totalStaked = Math.max(farmState.totalStaked - actual, 0)
      writeStore(store)
      logInfo('Unstake recorded (file store)', { account, txHash, amount: unstakeAmount, lpTxHash: payout?.hash })
      res.json({
        stakedBalance: user.balance.toString(),
        totalStaked: farmState.totalStaked.toString(),
        lpTxHash: payout?.hash,
      })
    } catch (error) {
      logWarn('Unstake failed', { txHash: req?.body?.txHash, error: error?.message || error })
      res.status(500).json({ error: error.message || 'Failed to unstake' })
    }
  })

  app.post(`${API_PREFIX}/farms/claim`, async (req, res) => {
    try {
      const { account, farmId, txHash } = req.body || {}
      const signalAddress = FARM_SIGNAL_ADDRESS || getEscrowAddress() || BESC_ISSUER
      await verifyAction({
        account,
        action: 'claim',
        farmId,
        txHash,
        destinations: [signalAddress],
        expectAmount: { type: 'xrp' },
      })
      if (isDbEnabled()) {
        let claimable = null
        let payout = null
        try {
          claimable = await prepareClaimDb({ account, txHash })
          payout = await sendRewardPayment(account, claimable)
          await finalizeClaimDb({
            account,
            txHash,
            claimable,
            rewardTxHash: payout?.hash,
            status: 'paid',
          })
          logInfo('Claim paid', { account, txHash, amount: formatValue(claimable), rewardTxHash: payout?.hash })
          res.json({ claimed: formatValue(claimable), rewardSymbol: BESC_SYMBOL, rewardTxHash: payout?.hash })
        } catch (error) {
          if (claimable) {
            await finalizeClaimDb({
              account,
              txHash,
              claimable,
              rewardTxHash: payout?.hash,
              status: 'failed',
            })
          }
          throw error
        }
        return
      }
      const store = readStore()
      const farmState = getFarmState(store)
      if (farmState.processedTxs[txHash]) {
        throw new Error('Claim transaction already processed')
      }
      updateFarmRewards(farmState)
      const user = updateUserRewards(farmState, account)
      const claimable = user.rewards
      const payout = await sendRewardPayment(account, claimable)
      user.rewards = 0
      farmState.processedTxs[txHash] = true
      writeStore(store)
      logInfo('Claim paid (file store)', { account, txHash, amount: claimable.toString(), rewardTxHash: payout?.hash })
      res.json({ claimed: claimable.toString(), rewardSymbol: BESC_SYMBOL, rewardTxHash: payout.hash })
    } catch (error) {
      logWarn('Claim failed', { txHash: req?.body?.txHash, error: error?.message || error })
      res.status(500).json({ error: error.message || 'Failed to claim' })
    }
  })

  app.get(`${API_PREFIX}/farms/indexer/run`, async (req, res) => {
    try {
      const result = await indexEscrowStakes()
      res.json(result)
    } catch (error) {
      res.status(500).json({ error: error?.message || 'Failed to run indexer' })
    }
  })

  app.post(`${API_PREFIX}/farms/indexer/tx`, async (req, res) => {
    try {
      const { txHash } = req.body || {}
      if (!txHash) {
        throw new Error('Missing txHash')
      }
      const escrowAddress = getEscrowAddress()
      if (!escrowAddress) {
        throw new Error('Missing FARMS_ESCROW_ADDRESS')
      }
      const meta = await loadFarmMeta()
      if (!meta?.lpTokenCurrency || !meta?.lpTokenIssuer) {
        throw new Error('LP token not available')
      }
      const client = await getXrplClient()
      const response = await client.request({ command: 'tx', transaction: txHash })
      const tx = response?.result
      if (!tx?.validated) {
        throw new Error('Transaction not validated')
      }
      const txFields = getTxFields(tx)
      const metaData = tx?.meta || tx?.metaData || txFields?.meta
      const applied = await processStakeTxEntry(
        txFields,
        metaData,
        meta.lpTokenCurrency,
        meta.lpTokenIssuer,
        escrowAddress,
      )
      res.json({ applied })
    } catch (error) {
      res.status(500).json({ error: error?.message || 'Failed to process tx' })
    }
  })

  startEscrowIndexer()
}

module.exports = { mountFarmsApi }
