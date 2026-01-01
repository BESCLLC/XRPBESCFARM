const { Pool } = require('pg')

const DATABASE_URL = process.env.DATABASE_URL || ''
const DATABASE_SSL = process.env.DATABASE_SSL === 'true'
const PGSSLMODE = process.env.PGSSLMODE || ''

const urlHasSslMode = DATABASE_URL.includes('sslmode=require')
const useSsl = DATABASE_SSL || PGSSLMODE === 'require' || urlHasSslMode

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: useSsl ? { rejectUnauthorized: false } : undefined,
    })
  : null

let initPromise = null

const initDb = async () => {
  if (!pool) {
    throw new Error('DATABASE_URL is not set')
  }
  if (!initPromise) {
    initPromise = (async () => {
      const client = await pool.connect()
      try {
        await client.query(`
          CREATE TABLE IF NOT EXISTS farm_state (
            farm_id TEXT PRIMARY KEY,
            total_staked NUMERIC NOT NULL DEFAULT 0,
            reward_per_token NUMERIC NOT NULL DEFAULT 0,
            last_update BIGINT NOT NULL
          );
        `)
        await client.query(`
          CREATE TABLE IF NOT EXISTS farm_users (
            farm_id TEXT NOT NULL,
            account TEXT NOT NULL,
            balance NUMERIC NOT NULL DEFAULT 0,
            rewards NUMERIC NOT NULL DEFAULT 0,
            reward_per_token_paid NUMERIC NOT NULL DEFAULT 0,
            PRIMARY KEY (farm_id, account)
          );
        `)
        await client.query(`
          CREATE TABLE IF NOT EXISTS farm_actions (
            tx_hash TEXT PRIMARY KEY,
            farm_id TEXT NOT NULL,
            account TEXT NOT NULL,
            action TEXT NOT NULL,
            amount NUMERIC NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );
        `)
        await client.query(`
          CREATE TABLE IF NOT EXISTS farm_claims (
            tx_hash TEXT PRIMARY KEY,
            farm_id TEXT NOT NULL,
            account TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            reward_amount NUMERIC NOT NULL DEFAULT 0,
            reward_tx_hash TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
          );
        `)
      } finally {
        client.release()
      }
    })()
  }
  return initPromise
}

const withTransaction = async (handler) => {
  if (!pool) {
    throw new Error('DATABASE_URL is not set')
  }
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    const result = await handler(client)
    await client.query('COMMIT')
    return result
  } catch (error) {
    try {
      await client.query('ROLLBACK')
    } catch (rollbackError) {
      // ignore rollback errors
    }
    throw error
  } finally {
    client.release()
  }
}

const isDbEnabled = () => Boolean(pool)

module.exports = {
  initDb,
  withTransaction,
  isDbEnabled,
}
