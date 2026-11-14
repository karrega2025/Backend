// src/db.js — Camada unificada de banco de dados (Supabase PostgreSQL nativo)
import { pgPool, isPostgresConfigured } from './supabase.js';

class PgConnectionWrapper {
  constructor(client) {
    this.client = client;
  }

  async beginTransaction() {
    await this.client.query('BEGIN');
  }

  async commit() {
    await this.client.query('COMMIT');
  }

  async rollback() {
    await this.client.query('ROLLBACK');
  }

  release() {
    this.client.release();
  }

  async query(sql, params = []) {
    return executeQuery(this.client, sql, params);
  }
}

function convertSqlToPostgres(sql) {
  let paramIdx = 1;
  let pgSql = sql.replace(/\?/g, () => `$${paramIdx++}`);

  // Compatibilidade de funções de data MySQL -> PostgreSQL
  pgSql = pgSql.replace(/DATE_SUB\(NOW\(\),\s*INTERVAL\s+([0-9]+)\s+HOUR\)/gi, "NOW() - INTERVAL '$1 hours'");
  pgSql = pgSql.replace(/DATE_SUB\(NOW\(\),\s*INTERVAL\s+([0-9]+)\s+DAY\)/gi, "NOW() - INTERVAL '$1 days'");
  pgSql = pgSql.replace(/DATE_SUB\(NOW\(\),\s*INTERVAL\s+([0-9]+)\s+MONTH\)/gi, "NOW() - INTERVAL '$1 months'");
  pgSql = pgSql.replace(/DATE_FORMAT\(([^,]+),\s*'([^']+)'\)/gi, (match, col, fmt) => {
    let pgFmt = fmt.replace(/%Y/g, 'YYYY').replace(/%m/g, 'MM').replace(/%d/g, 'DD');
    return `TO_CHAR(${col}, '${pgFmt}')`;
  });

  // Em INSERT, adiciona RETURNING id se não houver RETURNING para pegar insertId
  if (/^\s*INSERT\s+INTO/i.test(pgSql) && !/RETURNING/i.test(pgSql)) {
    pgSql += ' RETURNING id';
  }

  return pgSql;
}

async function executeQuery(executor, sql, params = []) {
  const pgSql = convertSqlToPostgres(sql);
  const res = await executor.query(pgSql, params);
  const rows = res.rows || [];
  const insertId = rows[0]?.id || null;
  const affectedRows = res.rowCount || 0;

  rows.insertId = insertId;
  rows.affectedRows = affectedRows;

  return [rows, { insertId, affectedRows }];
}

export const db = {
  isPostgres: isPostgresConfigured,

  async query(sql, params = []) {
    if (pgPool) {
      return executeQuery(pgPool, sql, params);
    }
    throw new Error('Supabase PostgreSQL não está configurado');
  },

  async getConnection() {
    if (pgPool) {
      const client = await pgPool.connect();
      return new PgConnectionWrapper(client);
    }
    throw new Error('Supabase PostgreSQL não está configurado');
  },
};

export default db;
