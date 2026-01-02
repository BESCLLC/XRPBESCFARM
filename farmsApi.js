const express = require('express')
const fetch = require('node-fetch')
const dotenv = require('dotenv')
const path = require('path')
const fs = require('fs')
const BigNumber = require('bignumber.js')
const { Client, dropsToXrp, Wallet } = require('xrpl')
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
  try {
    return Buffer.from(hex, 'hex').toString('utf8')
  } catch (error) {
    return ''
  }
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
      // ignore invalid memo
    }
  }
  return null
}

let xrplClientPromise = null

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

const getTxFields = (tx) => tx?.tx_json || tx || {}

const verifyAction = async ({ account, action, farmId, txHash }) => {
  if (!account || !action || !farmId || !txHash) {
    throw new Error('Missing request fields')
  }
  const client = await getXrplClient()
  const tx = await fetchValidatedTx(client, txHash)
  const txFields = getTxFields(tx)
  const txType = txFields.TransactionType
  if (txType !== 'Payment') {
    throw new Error('Invalid transaction type')
  }
  if (txFields.Account !== account) {
    throw new Error('Account mismatch')
  }
  const destination = txFields.Destination
  if (destination !== FARM_SIGNAL_ADDRESS && destination !== BESC_ISSUER) {
    throw new Error('Invalid destination')
  }
  const memo = parseMemo(txFields.Memos)
  const memoAction = memo?.action ? String(memo.action).toLowerCase() : ''
  if (!memo || memoAction !== action || memo.farmId !== farmId) {
    throw new Error('Missing farm memo')
  }
  return memo
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

const formatValue = (value) => {
  const amount = new BigNumber(value || 0)
  if (!amount.isFinite() || amount.lte(0)) return '0'
  const fixed = amount.toFixed(6)
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

  app.post(`${API_PREFIX}/farms/stake`, async (req, res) => {
    try {
      const { account, farmId, amount, txHash } = req.body || {}
      const memo = await verifyAction({ account, action: 'stake', farmId, txHash })
      const stakeAmount = memo.amount || amount || 0
      if (isDbEnabled()) {
        const result = await applyStakeDb({ account, action: 'stake', amount: stakeAmount, txHash })
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
      res.json({ stakedBalance: user.balance.toString(), totalStaked: farmState.totalStaked.toString() })
    } catch (error) {
      res.status(500).json({ error: error.message || 'Failed to stake' })
    }
  })

  app.post(`${API_PREFIX}/farms/unstake`, async (req, res) => {
    try {
      const { account, farmId, amount, txHash } = req.body || {}
      const memo = await verifyAction({ account, action: 'unstake', farmId, txHash })
      const unstakeAmount = memo.amount || amount || 0
      if (isDbEnabled()) {
        const result = await applyStakeDb({ account, action: 'unstake', amount: unstakeAmount, txHash })
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
      user.balance -= actual
      farmState.totalStaked = Math.max(farmState.totalStaked - actual, 0)
      writeStore(store)
      res.json({ stakedBalance: user.balance.toString(), totalStaked: farmState.totalStaked.toString() })
    } catch (error) {
      res.status(500).json({ error: error.message || 'Failed to unstake' })
    }
  })

  app.post(`${API_PREFIX}/farms/claim`, async (req, res) => {
    try {
      const { account, farmId, txHash } = req.body || {}
      await verifyAction({ account, action: 'claim', farmId, txHash })
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
      res.json({ claimed: claimable.toString(), rewardSymbol: BESC_SYMBOL, rewardTxHash: payout.hash })
    } catch (error) {
      res.status(500).json({ error: error.message || 'Failed to claim' })
    }
  })
}

module.exports = { mountFarmsApi }
