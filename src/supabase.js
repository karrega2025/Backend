import { createClient } from '@supabase/supabase-js';
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
const supabaseDbUrl = process.env.SUPABASE_DB_URL || process.env.DATABASE_URL;

export const isSupabaseConfigured = Boolean(supabaseUrl && supabaseKey);
export const isPostgresConfigured = Boolean(supabaseDbUrl);

// 1. Cliente Supabase (para Storage, Realtime e Auth)
export const supabase = isSupabaseConfigured
  ? createClient(supabaseUrl, supabaseKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    })
  : null;

// 2. Pool de Conexões PostgreSQL direto (para queries SQL de alta performance e PostGIS)
export const pgPool = isPostgresConfigured
  ? new Pool({
      connectionString: supabaseDbUrl,
      ssl: { rejectUnauthorized: false },
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    })
  : null;

// 3. Helper para Upload de Ficheiros no Supabase Storage
export async function uploadToStorage(bucketName, filePath, base64OrBuffer, contentType = 'image/jpeg') {
  if (!supabase) {
    console.warn(`[Supabase Storage] Supabase não configurado. Ignorando upload para ${bucketName}/${filePath}`);
    return null;
  }

  try {
    let buffer;
    if (Buffer.isBuffer(base64OrBuffer)) {
      buffer = base64OrBuffer;
    } else if (typeof base64OrBuffer === 'string') {
      const cleanBase64 = base64OrBuffer.replace(/^data:image\/[a-zA-Z]+;base64,/, '');
      buffer = Buffer.from(cleanBase64, 'base64');
    } else {
      return null;
    }

    // Tentar criar o bucket se não existir
    try {
      await supabase.storage.createBucket(bucketName, { public: true });
    } catch (_) {
      // Ignora se já existir
    }

    const { data, error } = await supabase.storage
      .from(bucketName)
      .upload(filePath, buffer, {
        contentType,
        upsert: true,
      });

    if (error) {
      console.error(`[Supabase Storage] Erro no upload para ${bucketName}/${filePath}:`, error.message);
      return null;
    }

    // Obter URL pública
    const { data: urlData } = supabase.storage.from(bucketName).getPublicUrl(filePath);
    return urlData.publicUrl;
  } catch (err) {
    console.error(`[Supabase Storage] Exceção no upload:`, err.message);
    return null;
  }
}

// 4. Inicializador de Buckets
export async function initializeStorageBuckets() {
  if (!supabase) return;
  const buckets = ['avatars', 'documentos-cargueiros', 'veiculos'];
  for (const b of buckets) {
    try {
      await supabase.storage.createBucket(b, { public: true });
    } catch (_) {}
  }
}
