// index.js
import express from "express";
import http from "http";
import { Server as SocketIOServer } from "socket.io";
import cors from "cors";
import dotenv from "dotenv";
import mysql from "mysql2/promise";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import db from "./db.js";
import adminRoutes from "./admin.js"; // painel administrativo
import { enviarWhatsApp, mensagem } from "./whatsapp.js";
import {
  supabase,
  pgPool,
  uploadToStorage,
  initializeStorageBuckets,
  isSupabaseConfigured,
  isPostgresConfigured,
} from "./supabase.js";

dotenv.config();
initializeStorageBuckets().catch(() => {});

const app = express();
const server = http.createServer(app);
const io = new SocketIOServer(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE"],
  },
});

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
const JWT_SECRET = process.env.JWT_SECRET || "karrega_jwt_token_secret_key_2026_prod_secure";

// Configuração de eventos WebSockets em tempo real
io.on("connection", (socket) => {
  // Cliente e cargueiro entram na sala da viagem para escuta em tempo real
  socket.on("join_trip", (tripId) => {
    if (tripId) socket.join(`trip:${tripId}`);
  });

  socket.on("leave_trip", (tripId) => {
    if (tripId) socket.leave(`trip:${tripId}`);
  });

  // Cargueiros entram na sala do seu tipo de veículo para receber novos fretes instantaneamente
  socket.on("join_driver_feed", (tipoTransporte) => {
    const canonical = (tipoTransporte || "ligeiro").toString().toLowerCase().trim();
    socket.join(`driver_feed:${canonical}`);
    socket.join("driver_feed:all");
  });

  socket.on("leave_driver_feed", (tipoTransporte) => {
    const canonical = (tipoTransporte || "ligeiro").toString().toLowerCase().trim();
    socket.leave(`driver_feed:${canonical}`);
  });

  // Rastreio em tempo real: cargueiro transmite GPS para o cliente sem requisições HTTP
  socket.on("stream_driver_location", (data) => {
    if (data && data.tripId) {
      socket.to(`trip:${data.tripId}`).emit("driver_location_update", data);
    }
  });
});

export function emitTripEvent(tripId, eventName, payload = {}) {
  try {
    io.to(`trip:${tripId}`).emit(eventName, { tripId, timestamp: new Date(), ...payload });
  } catch (err) {
    console.warn("[WebSocket] Erro ao emitir evento de viagem:", err.message);
  }
}

export function emitNewTripEvent(tipoTransporte, tripData = {}) {
  try {
    const canonical = (tipoTransporte || "ligeiro").toString().toLowerCase().trim();
    io.to(`driver_feed:${canonical}`).emit("new_trip_available", tripData);
    io.to("driver_feed:all").emit("new_trip_available", tripData);
  } catch (err) {
    console.warn("[WebSocket] Erro ao emitir novo frete:", err.message);
  }
}

function generateToken(user, role) {
  return jwt.sign(
    { id: user.id, numero: user.numero, role },
    JWT_SECRET,
    { expiresIn: '30d' }
  );
}

app.use(cors({ origin: process.env.ALLOWED_ORIGIN || '*' }));
app.use(express.json({ limit: '20mb' }));

// Ensure OPTIONS preflight requests are handled and CORS headers returned
app.options('*', cors({ origin: process.env.ALLOWED_ORIGIN || '*' }));

// Conexão unificada exclusiva com Supabase PostgreSQL (Aiven descontinuada)
const pool = db;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TRIP_STATUS = {
  DRAFT: 'rascunho',
  WAITING_PROPOSALS: 'aguardando_propostas',
  NEGOTIATING: 'em_negociacao',
  WAITING_CONFIRMATION: 'aguardando_confirmacao',
  CONFIRMED: 'confirmada',
  TO_PICKUP: 'a_caminho_recolha',
  PICKED_UP: 'carga_recolhida',
  IN_TRANSIT: 'em_transito',
  WAITING_DELIVERY_CONFIRMATION: 'aguardando_confirmacao_entrega',
  DONE: 'feito',
  CANCELLED: 'cancelado',
  EXPIRED: 'expirada',
  DISPUTED: 'em_disputa',
};

const ACTIVE_VISIBLE_STATUSES = [
  TRIP_STATUS.WAITING_PROPOSALS,
  TRIP_STATUS.NEGOTIATING,
  TRIP_STATUS.WAITING_CONFIRMATION,
  TRIP_STATUS.CONFIRMED,
  TRIP_STATUS.TO_PICKUP,
  TRIP_STATUS.PICKED_UP,
  TRIP_STATUS.IN_TRANSIT,
  TRIP_STATUS.WAITING_DELIVERY_CONFIRMATION,
  'pendente',
  'aceito',
];

function transportTypeAliases(value) {
  const type = String(value || '').trim().toLowerCase();
  if (['caminhao', 'camião', 'caminhão', 'pesado'].includes(type)) {
    return ['caminhao', 'camião', 'caminhão', 'pesado'];
  }
  if (['ligeiro', 'carro', 'camioneta'].includes(type)) {
    return ['ligeiro', 'carro', 'camioneta'];
  }
  if (type === 'motocicleta') return ['motocicleta'];
  return [type];
}

async function getOperationalConfig(conn = pool) {
  const [rows] = await conn.query('SELECT * FROM configuracoes_operacionais WHERE id = 1 LIMIT 1');
  return {
    ...(rows[0] || {}),
    antecedencia_minima_horas: rows[0]?.antecedencia_minima_horas ?? 72,
    prazo_confirmacao_horas: rows[0]?.prazo_confirmacao_horas ?? 24,
    cancelamento_automatico: rows[0]?.cancelamento_automatico ?? 1,
    taxa_karrega_percentual: rows[0]?.taxa_karrega_percentual !== undefined ? Number(rows[0].taxa_karrega_percentual) : 0.00,
    bonus_boas_vindas: rows[0]?.bonus_boas_vindas !== undefined ? Number(rows[0].bonus_boas_vindas) : 0.00,
  };
}

async function addTripHistory(conn, viagemId, fromStatus, toStatus, actorTipo = 'sistema', actorId = null, observacao = null) {
  try {
    await conn.query(
      'INSERT INTO viagem_historico (viagem_id, status_anterior, status_novo, actor_tipo, actor_id, observacao) VALUES (?, ?, ?, ?, ?, ?)',
      [viagemId, fromStatus || null, toStatus, actorTipo, actorId, observacao]
    ); 
  } catch (err) {
    console.error('Erro ao gravar histórico da viagem:', err.message);
  }
}

async function notifyTripParticipants(viagemId, event, targets = ['cliente', 'transportador']) {
  try {
    const [rows] = await pool.query(
      `SELECT c.nome AS cliente_nome, c.numero AS cliente_numero,
              t.nome AS transportador_nome, t.numero AS transportador_numero,
              v.origin, v.destination, v.scheduled_at, v.valor, v.preco_final
       FROM viagens v
       LEFT JOIN clientes c ON v.cliente_id = c.id
       LEFT JOIN transportadores t ON v.transportador_id = t.id
       WHERE v.id = ? LIMIT 1`,
      [viagemId]
    );
    const trip = rows[0];
    if (!trip) return;

    // Emissão em tempo real para a sala da viagem via WebSockets (elimina necessidade de polling curto)
    emitTripEvent(viagemId, event, {
      viagem_id: viagemId,
      status: event,
      origem: trip.origin,
      destino: trip.destination,
      cliente_nome: trip.cliente_nome,
      cargueiro_nome: trip.transportador_nome,
      preco: trip.preco_final || trip.valor,
    });
    const details = [
      `Pedido KR-${String(viagemId).padStart(6, '0')}`,
      trip.origin && `Origem: ${trip.origin}`,
      trip.destination && `Destino: ${trip.destination}`,
      trip.scheduled_at && `Data: ${new Date(trip.scheduled_at).toLocaleString('pt-AO')}`,
      (trip.preco_final || trip.valor) && `Preço: ${Number(trip.preco_final || trip.valor).toLocaleString('pt-AO')} Kz`,
    ].filter(Boolean).join('\n');
    if (targets.includes('cliente')) {
      const text = mensagem(event, trip.cliente_nome || 'cliente');
      await enviarWhatsApp(trip.cliente_numero, details && text ? `${text}\n\n${details}` : text);
    }
    if (targets.includes('transportador')) {
      const text = mensagem(event, trip.transportador_nome || 'cargueiro');
      await enviarWhatsApp(trip.transportador_numero, details && text ? `${text}\n\n${details}` : text);
    }
  } catch (err) {
    console.error('Falha ao notificar viagem por WhatsApp:', err.message);
  }
}

async function expirePendingConfirmations() {
  const config = await getOperationalConfig();
  if (!config.cancelamento_automatico) return { expired: 0 };

  const [rows] = await pool.query(
    `SELECT id, status FROM viagens
     WHERE status = ?
       AND confirmacao_limite_at IS NOT NULL
       AND confirmacao_limite_at <= NOW()
     LIMIT 100`,
    [TRIP_STATUS.WAITING_CONFIRMATION]
  );

  let expired = 0;
  for (const trip of rows) {
    const conn = await pool.getConnection();
    let didExpire = false;
    try {
      await conn.beginTransaction();
      const [result] = await conn.query(
        `UPDATE viagens
         SET status = ?, cancel_reason = ?
         WHERE id = ? AND status = ?`,
        [TRIP_STATUS.EXPIRED, 'Prazo de confirmação expirado', trip.id, TRIP_STATUS.WAITING_CONFIRMATION]
      );
      if (result.affectedRows > 0) {
        await conn.query(
          "UPDATE propostas SET status = 'recusado' WHERE viagem_id = ? AND status IN ('pendente', 'selecionada', 'aceito')",
          [trip.id]
        );
        await addTripHistory(conn, trip.id, trip.status, TRIP_STATUS.EXPIRED, 'sistema', null, 'Prazo de confirmação expirado automaticamente');
        expired += 1;
        didExpire = true;
      }
      await conn.commit();
    } catch (err) {
      try { await conn.rollback(); } catch (_) {}
      console.error('Erro ao expirar confirmação:', err.message);
    } finally {
      conn.release();
    }
    if (didExpire) await notifyTripParticipants(trip.id, 'viagem_expirada');
  }

  return { expired };
}

function parseDateOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

// Função de inicialização para não derrubar o servidor
async function initDatabase() {
  try {
    if (pool.isPostgres) {
      console.log("🟢 Conectado com sucesso ao Supabase PostgreSQL. Tabelas operacionais ativas.");
      return;
    }
    const sqlPath = path.join(process.cwd(), "init_db.sql");
    if (fs.existsSync(sqlPath)) {
      const sql = fs.readFileSync(sqlPath, "utf8");
      console.log("🟢 Inicializando o banco de dados...");
      await pool.query(sql);

      try {
        await pool.query("ALTER TABLE viagens ADD COLUMN valor DECIMAL(12,2) DEFAULT 5000.00");
        console.log("✅ Coluna 'valor' adicionada à tabela 'viagens'.");
      } catch (_) {
        // Ignora se a coluna já existe
      }

      const migrations = [
        "ALTER TABLE viagens MODIFY COLUMN status ENUM('rascunho','aguardando_propostas','em_negociacao','aguardando_confirmacao','confirmada','a_caminho_recolha','carga_recolhida','em_transito','aguardando_confirmacao_entrega','feito','cancelado','expirada','em_disputa','pendente','aceito') NOT NULL DEFAULT 'aguardando_propostas'",
        "ALTER TABLE propostas MODIFY COLUMN status ENUM('pendente','selecionada','aceito','recusado') NOT NULL DEFAULT 'pendente'",
        "ALTER TABLE viagens ADD COLUMN preco_final DECIMAL(12,2) NULL",
        "ALTER TABLE viagens ADD COLUMN confirmacao_limite_at DATETIME NULL",
        "ALTER TABLE viagens ADD COLUMN entrega_registada_at DATETIME NULL",
        "ALTER TABLE viagens ADD COLUMN cliente_confirmou_entrega_at DATETIME NULL",
        "ALTER TABLE viagens ADD COLUMN atraso_recolha_confirmado BOOLEAN NULL",
        "ALTER TABLE viagens ADD COLUMN limpeza_transporte ENUM('limpo','moderado','sujo') NULL",
        "ALTER TABLE viagens ADD COLUMN cancel_reason VARCHAR(255) NULL",
        "ALTER TABLE viagens ADD COLUMN pin_entrega VARCHAR(4) NULL",
        "ALTER TABLE transportadores MODIFY COLUMN foto_bi_path MEDIUMTEXT NULL",
        "ALTER TABLE transportadores ADD COLUMN tipo_transporte VARCHAR(32) DEFAULT 'ligeiro'",
        "ALTER TABLE configuracoes_operacionais ADD COLUMN bonus_boas_vindas DECIMAL(12,2) NOT NULL DEFAULT 0.00",
        "CREATE TABLE IF NOT EXISTS carteira_cargueiro (transportador_id INT PRIMARY KEY, saldo DECIMAL(12,2) NOT NULL DEFAULT 0.00, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, CONSTRAINT fk_carteira_transportador FOREIGN KEY (transportador_id) REFERENCES transportadores(id) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS carteira_recargas (id INT AUTO_INCREMENT PRIMARY KEY, transportador_id INT NOT NULL, valor DECIMAL(12,2) NOT NULL, comprovativo_path MEDIUMTEXT NULL, status ENUM('pendente', 'aprovada', 'recusada') NOT NULL DEFAULT 'pendente', observacao VARCHAR(255) NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, INDEX idx_recargas_transportador (transportador_id), INDEX idx_recargas_status (status), CONSTRAINT fk_recargas_transportador FOREIGN KEY (transportador_id) REFERENCES transportadores(id) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
      ];
      for (const sql of migrations) {
        try { await pool.query(sql); } catch (_) {}
      }

      console.log("✅ Banco de dados sincronizado.");
    }
  } catch (err) {
    console.error("❌ Alerta Banco:", err.message);
  }
}

// Inicia a sincronização em background
initDatabase();

// --- ROTAS ---

app.get('/api/health', (req, res) => res.json({ status: 'ok' }));
app.get('/', (req, res) => res.send('Karrega backend is running (DB Storage Mode)'));

app.post('/api/jobs/expire-confirmations', async (req, res) => {
  const secret = process.env.CRON_SECRET;
  const provided = req.headers.authorization?.replace('Bearer ', '') || req.query.secret;
  if (secret && provided !== secret) return res.status(401).json({ error: 'Não autorizado' });
  try {
    const result = await expirePendingConfirmations();
    return res.json({ sucesso: true, ...result });
  } catch (err) {
    console.error('Erro no job de expiração:', err);
    return res.status(500).json({ error: 'Erro ao executar job' });
  }
});

// Painel administrativo (login, stats, clientes, cargueiros, pedidos, financeiro)
app.use('/api/admin', adminRoutes(pool));

app.get('/api/configuracoes/preco-inicial', async (req, res) => {
  try {
    const [rows] = await pool.query('SELECT valor FROM valor_inicial ORDER BY id DESC LIMIT 1');
    if (!rows || rows.length === 0) {
      return res.status(404).json({ error: 'Configuração de preço inicial não encontrada' });
    }
    return res.json({ valor: Number(rows[0].valor) });
  } catch (err) {
    console.error('Erro ao buscar preço inicial:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Register client
app.post('/api/register/client', async (req, res) => {
  try {
    const { nome, numero, numero_bi, password } = req.body;
    if (!nome || !numero || !numero_bi || !password) return res.status(400).json({ error: 'Dados obrigatórios faltando' });

    const hashed = await bcrypt.hash(password, 10);
    const [resInsert] = await pool.query(
      'INSERT INTO clientes (nome, numero, numero_bi, password) VALUES (?, ?, ?, ?)',
      [nome, numero, numero_bi, hashed]
    );
    const token = generateToken({ id: resInsert.insertId, numero, nome }, 'client');
    await enviarWhatsApp(numero, mensagem('boasvindas_cliente', nome));
    return res.json({ ok: true, id: resInsert.insertId, token });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(409).json({ error: 'Número já registado' });
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Upload endpoint para o Supabase Object Storage
app.post('/api/upload', async (req, res) => {
  try {
    const { bucket = 'documentos-cargueiros', filename, file_base64, content_type = 'image/jpeg' } = req.body;
    if (!file_base64) return res.status(400).json({ error: 'Ficheiro não fornecido' });

    const safeFilename = filename || `file_${Date.now()}_${Math.random().toString(36).substring(7)}.jpg`;
    const publicUrl = await uploadToStorage(bucket, safeFilename, file_base64, content_type);

    if (!publicUrl) {
      return res.status(500).json({ error: 'Falha ao processar upload no storage' });
    }

    return res.json({ ok: true, url: publicUrl, filename: safeFilename, bucket });
  } catch (err) {
    console.error('Erro em /api/upload:', err);
    return res.status(500).json({ error: 'Erro no servidor ao carregar ficheiro' });
  }
});

// Register driver (Suporta Supabase PostgreSQL + Object Storage + MySQL fallback)
app.post('/api/register/driver', async (req, res) => {
  try {
    const {
      // Informações Pessoais
      nome,
      numero,
      numero_bi,
      data_nascimento,
      password,
      foto_perfil,      // base64 ou url
      bi_frente,        // base64 ou url
      bi_verso,         // base64 ou url
      numero_carta,
      carta_conducao,   // base64 ou url
      foto_bi_base64,   // retrocompatibilidade
      // Informações do Veículo
      tipo_transporte,
      marca,
      modelo,
      ano_fabricacao,
      cor,
      numero_placa,
      foto_veiculo_frente // base64 ou url
    } = req.body;

    if (!nome || !numero || !numero_bi || !password) {
      return res.status(400).json({ error: 'Dados obrigatórios faltando (nome, telefone, BI e senha)' });
    }

    const tipoFinal = (tipo_transporte || 'ligeiro').toLowerCase().trim();
    const hashed = await bcrypt.hash(password, 10);
    const timestamp = Date.now();

    // 1. Processar upload de fotos para o Supabase Storage se vierem em base64
    let fotoPerfilUrl = foto_perfil || null;
    let biFrenteUrl = bi_frente || foto_bi_base64 || null;
    let biVersoUrl = bi_verso || null;
    let cartaUrl = carta_conducao || null;
    let fotoVeiculoUrl = foto_veiculo_frente || null;

    if (isSupabaseConfigured) {
      const safeFolder = `cargueiro_${numero.replace(/[^0-9]/g, '')}`;
      if (fotoPerfilUrl && fotoPerfilUrl.startsWith('data:')) {
        fotoPerfilUrl = await uploadToStorage('avatars', `${safeFolder}/foto_perfil_${timestamp}.jpg`, fotoPerfilUrl);
      }
      if (biFrenteUrl && biFrenteUrl.startsWith('data:')) {
        biFrenteUrl = await uploadToStorage('documentos-cargueiros', `${safeFolder}/bi_frente_${timestamp}.jpg`, biFrenteUrl);
      }
      if (biVersoUrl && biVersoUrl.startsWith('data:')) {
        biVersoUrl = await uploadToStorage('documentos-cargueiros', `${safeFolder}/bi_verso_${timestamp}.jpg`, biVersoUrl);
      }
      if (cartaUrl && cartaUrl.startsWith('data:')) {
        cartaUrl = await uploadToStorage('documentos-cargueiros', `${safeFolder}/carta_conducao_${timestamp}.jpg`, cartaUrl);
      }
      if (fotoVeiculoUrl && fotoVeiculoUrl.startsWith('data:')) {
        fotoVeiculoUrl = await uploadToStorage('veiculos', `${safeFolder}/veiculo_frente_${timestamp}.jpg`, fotoVeiculoUrl);
      }
    }

    let driverId;
    const config = await getOperationalConfig();
    const bonus = Number(config.bonus_boas_vindas || 0);

    // 2. Se o Supabase PostgreSQL estiver configurado, salvar nas tabelas PostgreSQL
    if (pgPool) {
      try {
        const client = await pgPool.connect();
        try {
          await client.query('BEGIN');
          const cargueiroRes = await client.query(
            `INSERT INTO cargueiros 
              (nome, data_nascimento, numero, numero_bi, foto_perfil_url, bi_frente_url, bi_verso_url, numero_carta, carta_conducao_url, password_hash)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             RETURNING id`,
            [
              nome,
              data_nascimento || null,
              numero,
              numero_bi,
              fotoPerfilUrl,
              biFrenteUrl,
              biVersoUrl,
              numero_carta || null,
              cartaUrl,
              hashed,
            ]
          );
          driverId = cargueiroRes.rows[0].id;

          // Inserir informações do veículo se fornecidas
          if (numero_placa || marca || modelo) {
            await client.query(
              `INSERT INTO veiculos 
                (cargueiro_id, tipo, marca, modelo, ano_fabricacao, cor, numero_placa, foto_veiculo_frente_url)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
              [
                driverId,
                tipoFinal,
                marca || 'Desconhecida',
                modelo || 'Desconhecido',
                ano_fabricacao ? parseInt(ano_fabricacao, 10) : null,
                cor || null,
                numero_placa || `SEM-PLACA-${driverId}`,
                fotoVeiculoUrl,
              ]
            );
          }

          // Inicializar Carteira no PostgreSQL
          await client.query(
            `INSERT INTO carteira_cargueiro (cargueiro_id, saldo) VALUES ($1, $2) ON CONFLICT (cargueiro_id) DO NOTHING`,
            [driverId, bonus]
          );

          await client.query('COMMIT');
        } catch (pgErr) {
          await client.query('ROLLBACK');
          console.error('[Supabase PG Register] Erro na transação:', pgErr.message);
        } finally {
          client.release();
        }
      } catch (poolErr) {
        console.error('[Supabase PG Register] Erro de conexão:', poolErr.message);
      }
    }

    // 3. Salvar também em MySQL para retrocompatibilidade
    try {
      const [resInsert] = await pool.query(
        `INSERT INTO transportadores (
          nome, numero, numero_bi, foto_bi_path, password, tipo_transporte,
          foto_perfil_url, data_nascimento, foto_bi_verso_url, numero_carta, foto_carta_url,
          foto_veiculo_url, veiculo_marca, veiculo_modelo, veiculo_ano, veiculo_cor, veiculo_placa
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          nome, numero, numero_bi, biFrenteUrl || null, hashed, tipoFinal,
          fotoPerfilUrl || null, data_nascimento || null, biVersoUrl || null, numero_carta || null, cartaUrl || null,
          fotoVeiculoUrl || null, marca || null, modelo || null, ano_fabricacao ? parseInt(ano_fabricacao, 10) : null, cor || null, numero_placa || null
        ]
      );
      if (!driverId) driverId = resInsert.insertId;

      try {
        await pool.query(
          'INSERT INTO carteira_cargueiro (transportador_id, saldo) VALUES (?, ?)',
          [driverId, bonus]
        );
      } catch (_) {}
    } catch (mysqlErr) {
      try {
        const [resFallback] = await pool.query(
          'INSERT INTO transportadores (nome, numero, numero_bi, foto_bi_path, password, tipo_transporte) VALUES (?, ?, ?, ?, ?, ?)',
          [nome, numero, numero_bi, biFrenteUrl || null, hashed, tipoFinal]
        );
        if (!driverId) driverId = resFallback.insertId;
        try {
          await pool.query(
            'INSERT INTO carteira_cargueiro (transportador_id, saldo) VALUES (?, ?)',
            [driverId, bonus]
          );
        } catch (_) {}
      } catch (fallbackErr) {
        if (!driverId && (fallbackErr.code === 'ER_DUP_ENTRY' || mysqlErr.code === 'ER_DUP_ENTRY')) {
          return res.status(409).json({ error: 'Número já registado' });
        }
      }
    }

    if (!driverId) {
      return res.status(500).json({ error: 'Não foi possível completar o registo do motorista.' });
    }

    const token = generateToken({ id: driverId, numero, nome }, 'driver');
    await enviarWhatsApp(numero, mensagem('registo_cargueiro', nome));

    return res.json({
      ok: true,
      id: driverId,
      token,
      saldo_inicial: bonus,
      documentos: {
        foto_perfil: fotoPerfilUrl,
        bi_frente: biFrenteUrl,
        bi_verso: biVersoUrl,
        carta_conducao: cartaUrl,
        foto_veiculo: fotoVeiculoUrl,
      },
    });
  } catch (err) {
    console.error('Erro em register driver:', err);
    return res.status(500).json({ error: 'Erro interno no servidor' });
  }
});

// Login client
app.post('/api/login/client', async (req, res) => {
  try {
    const { numero, password } = req.body;
    const [rows] = await pool.query('SELECT * FROM clientes WHERE numero = ? LIMIT 1', [numero]);
    if (!rows.length || !rows[0].password) return res.status(401).json({ error: 'Credenciais inválidas' });

    const ok = await bcrypt.compare(password, rows[0].password);
    if (!ok) return res.status(401).json({ error: 'Credenciais inválidas' });

    // Bloqueia contas suspensas ou banidas pelo painel administrativo
    if (['suspenso', 'banido'].includes(rows[0].estado)) {
      return res.status(403).json({ error: 'Conta ' + rows[0].estado + '. Contacte o suporte Karrega.' });
    }

    const { password: _, ...safeUser } = rows[0];
    const token = generateToken(safeUser, 'client');
    return res.json({ ok: true, user: safeUser, token });
  } catch (err) {
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Login driver
app.post('/api/login/driver', async (req, res) => {
  try {
    const { numero, password } = req.body;
    const [rows] = await pool.query('SELECT * FROM transportadores WHERE numero = ? LIMIT 1', [numero]);
    if (!rows.length || !rows[0].password) return res.status(401).json({ error: 'Credenciais inválidas' });

    const ok = await bcrypt.compare(password, rows[0].password);
    if (!ok) return res.status(401).json({ error: 'Credenciais inválidas' });

    // Bloqueia contas suspensas ou banidas pelo painel administrativo
    if (['suspenso', 'banido'].includes(rows[0].estado)) {
      return res.status(403).json({ error: 'Conta ' + rows[0].estado + '. Contacte o suporte Karrega.' });
    }

    const { password: _, ...safeUser } = rows[0];
    const token = generateToken(safeUser, 'driver');
    return res.json({ ok: true, user: safeUser, token });
  } catch (err) {
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Listar tabelas (Debug)
app.get("/tabelas", async (req, res) => {
  try {
    const [rows] = await pool.query("SHOW TABLES");
    const tabelas = rows.map(row => Object.values(row)[0]);
    res.json({ sucesso: true, total: tabelas.length, tabelas });
  } catch (err) {
    res.status(500).json({ sucesso: false, erro: err.message });
  }
});

// Trips (Create, List, Get, Update)
app.post('/api/trips', async (req, res) => {
  try {
    let { cliente_id, cliente_numero, transportador_id, tipo, scheduled_at, status, origin, destination, descricao_da_carga, tamanho_carga, tipo_de_transporte, valor } = req.body;

    // Resolve cliente_id from numero if provided
    if (!cliente_id && cliente_numero) {
      const [crows] = await pool.query('SELECT id FROM clientes WHERE numero = ? LIMIT 1', [cliente_numero]);
      if (crows && crows.length > 0) cliente_id = crows[0].id;
    }

    if (!cliente_id) return res.status(400).json({ error: 'cliente_id ou cliente_numero é obrigatório' });

    tipo = tipo || 'na hora';
    const allowedCreateStatuses = new Set([
      TRIP_STATUS.DRAFT,
      TRIP_STATUS.WAITING_PROPOSALS,
      TRIP_STATUS.NEGOTIATING,
      'pendente',
    ]);
    const tripStatus = status === 'adiado'
      ? TRIP_STATUS.WAITING_PROPOSALS
      : (allowedCreateStatuses.has(status) ? status : TRIP_STATUS.WAITING_PROPOSALS);
    tipo_de_transporte = tipo_de_transporte || 'ligeiro';

    const scheduledDate = parseDateOrNull(scheduled_at);
    if (tipo === 'agendado') {
      if (!scheduledDate) return res.status(400).json({ error: 'scheduled_at é obrigatório para agendamentos' });
      const config = await getOperationalConfig();
      const minDate = new Date(Date.now() + Number(config.antecedencia_minima_horas) * 3600e3);
      if (scheduledDate.getTime() < minDate.getTime()) {
        return res.status(400).json({ error: `Agendamentos devem ter pelo menos ${config.antecedencia_minima_horas} horas de antecedência` });
      }
    }

    let finalValor = 0;
    if (valor || valor === 0) {
      finalValor = Number(valor);
    } else {
      try {
        const [vrows] = await pool.query('SELECT valor FROM valor_inicial ORDER BY id DESC LIMIT 1');
        finalValor = (vrows && vrows.length) ? Number(vrows[0].valor) : 5000.00;
      } catch (e) {
        finalValor = 5000.00;
      }
    }

    const [result] = await pool.query(
      'INSERT INTO viagens (cliente_id, transportador_id, status, tipo, scheduled_at, origin, destination, descricao_da_carga, tamanho_carga, tipo_de_transporte, valor) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [cliente_id, transportador_id || null, tripStatus, tipo, scheduledDate ? scheduledDate.toISOString().slice(0, 19).replace('T', ' ') : null, origin || null, destination || null, descricao_da_carga || null, tamanho_carga || null, tipo_de_transporte, finalValor]
    );
    await addTripHistory(pool, result.insertId, null, tripStatus, 'cliente', cliente_id, 'Viagem publicada');
    emitNewTripEvent(tipo_de_transporte, {
      id: result.insertId,
      tipo_de_transporte,
      origin,
      destination,
      tipo,
      descricao_da_carga,
      valor: finalValor,
      created_at: new Date(),
    });
    return res.status(201).json({ ok: true, id: result.insertId });
  } catch (err) { return res.status(500).json({ error: err.message }); }
});

app.get('/api/trips', async (req, res) => {
  try {
    const { cliente_id, transportador_id, transportador_numero } = req.query;
    let sql = `SELECT v.*, c.nome AS cliente_nome, c.numero AS cliente_numero, t.nome AS transportador_nome, t.numero AS transportador_numero FROM viagens v 
               LEFT JOIN clientes c ON v.cliente_id = c.id 
               LEFT JOIN transportadores t ON v.transportador_id = t.id`;
    const params = [];

    if (cliente_id) {
      sql += ' WHERE v.cliente_id = ?';
      params.push(cliente_id);
    } else if (transportador_id) {
      sql += ' WHERE v.transportador_id = ?';
      params.push(transportador_id);
    } else if (transportador_numero) {
      // CORRIGIDO: o número está na tabela transportadores (alias t)
      sql += ' WHERE t.numero = ?';
      params.push(transportador_numero);
    } else {
      sql += ` WHERE v.status NOT IN ('cancelado','expirada') OR v.transportador_id IS NOT NULL`;
    }

    const [rows] = await pool.query(sql, params);
    return res.json(rows);
  } catch (err) { return res.status(500).json({ error: err.message }); }
});

// Get single trip with client and driver contact details
app.get('/api/trips/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const [rows] = await pool.query(
      `SELECT v.*, c.nome AS cliente_nome, c.numero AS cliente_numero,
              t.nome AS transportador_nome, t.numero AS transportador_numero,
              t.tipo_transporte AS transportador_tipo_transporte,
              COALESCE(ROUND(AVG(a.classificacao_geral), 1), 5.0) AS motorista_estrelas,
              COUNT(a.id) AS motorista_total_avaliacoes
       FROM viagens v
       LEFT JOIN clientes c ON v.cliente_id = c.id
       LEFT JOIN transportadores t ON v.transportador_id = t.id
       LEFT JOIN avaliacoes a ON a.avaliado_id = t.id AND a.avaliador_tipo = 'cliente'
       WHERE v.id = ?
       GROUP BY v.id
       LIMIT 1`,
      [id]
    );
    if (!rows || rows.length === 0) return res.status(404).json({ error: 'Viagem não encontrada' });
    const trip = rows[0];

    // Mascarar números de contacto durante fase de negociação para evitar bypass
    const isConfirmedOrLater = [
      TRIP_STATUS.CONFIRMED,
      TRIP_STATUS.TO_PICKUP,
      TRIP_STATUS.PICKED_UP,
      TRIP_STATUS.IN_TRANSIT,
      TRIP_STATUS.WAITING_DELIVERY_CONFIRMATION,
      TRIP_STATUS.DONE,
      'aceito'
    ].includes(trip.status);

    if (!isConfirmedOrLater) {
      if (trip.cliente_numero && trip.cliente_numero.length > 5) {
        trip.cliente_numero = trip.cliente_numero.slice(0, 3) + '*****' + trip.cliente_numero.slice(-2);
      }
      if (trip.transportador_numero && trip.transportador_numero.length > 5) {
        trip.transportador_numero = trip.transportador_numero.slice(0, 3) + '*****' + trip.transportador_numero.slice(-2);
      }
    }

    return res.json(trip);
  } catch (err) { console.error('Erro get trip:', err); return res.status(500).json({ error: 'Erro no servidor' }); }
});

// List proposals for a trip (enriquecida com tipo de veículo, média de estrelas e total de avaliações)
app.get('/api/trips/:id/proposals', async (req, res) => {
  try {
    const tripId = req.params.id;
    const [rows] = await pool.query(`
      SELECT p.*, t.nome AS transportador_nome,
             CONCAT(SUBSTRING(t.numero, 1, 3), '*****', RIGHT(t.numero, 2)) AS transportador_numero,
             t.tipo_transporte AS transportador_tipo_transporte,
             COALESCE(ROUND(AVG(a.classificacao_geral), 1), 5.0) AS media_estrelas,
             COUNT(a.id) AS total_avaliacoes
      FROM propostas p
      LEFT JOIN transportadores t ON p.transportador_id = t.id
      LEFT JOIN avaliacoes a ON a.avaliado_id = t.id AND a.avaliador_tipo = 'cliente'
      WHERE p.viagem_id = ?
      GROUP BY p.id
      ORDER BY p.created_at DESC
      LIMIT 30`, [tripId]);
    return res.json({ sucesso: true, propostas: rows });
  } catch (err) {
    console.error('Erro listar propostas:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Create or update a proposal for a trip
app.post('/api/trips/:id/proposals', async (req, res) => {
  try {
    const tripId = req.params.id;
    const { transportador_id, valor_proposto, status } = req.body;
    if (!transportador_id) return res.status(400).json({ error: 'transportador_id é obrigatório' });
    if (valor_proposto === undefined || valor_proposto === null) return res.status(400).json({ error: 'valor_proposto é obrigatório' });

    // Bloqueia kargueiros não verificados ou inativos
    const [trows] = await pool.query('SELECT estado, verificado FROM transportadores WHERE id = ? LIMIT 1', [transportador_id]);
    if (!trows.length) return res.status(404).json({ error: 'Transportador não encontrado' });
    if (trows[0].estado !== 'ativo') return res.status(403).json({ error: 'Conta inativa ou suspensa. Contacte o suporte Karrega.' });
    if (trows[0].verificado !== 'verificado') {
      return res.status(403).json({ error: 'Conta em análise. Os seus documentos ainda estão a ser validados pela equipa Karrega.' });
    }

    const proposalStatus = status || 'pendente';
    const valorNum = Number(valor_proposto);
    if (Number.isNaN(valorNum)) return res.status(400).json({ error: 'valor_proposto inválido' });
    const [existingRows] = await pool.query(
      'SELECT id FROM propostas WHERE viagem_id = ? AND transportador_id = ? LIMIT 1',
      [tripId, transportador_id]
    );

    let proposalId;
    if (existingRows && existingRows.length > 0) {
      proposalId = existingRows[0].id;
      await pool.query(
        'UPDATE propostas SET valor_proposto = ?, status = ?, created_at = NOW() WHERE id = ?',
        [valor_proposto, proposalStatus, proposalId]
      );
    } else {
      const [result] = await pool.query(
        'INSERT INTO propostas (viagem_id, transportador_id, valor_proposto, status, created_at) VALUES (?, ?, ?, ?, NOW())',
        [tripId, transportador_id, valor_proposto, proposalStatus]
      );
      proposalId = result.insertId;
    }

    const nextStatus = proposalStatus === 'pendente' ? TRIP_STATUS.NEGOTIATING : TRIP_STATUS.NEGOTIATING;
    await pool.query(
      `UPDATE viagens SET status = ? WHERE id = ? AND status IN ('pendente', ?, ?)`,
      [nextStatus, tripId, TRIP_STATUS.WAITING_PROPOSALS, TRIP_STATUS.NEGOTIATING]
    );

    if (proposalStatus === 'aceito') {
      await pool.query("UPDATE propostas SET status = 'pendente' WHERE id = ?", [proposalId]);
    }
    await notifyTripParticipants(tripId, 'proposta_recebida', ['cliente']);
    emitTripEvent(tripId, 'nova_proposta', {
      id: proposalId,
      transportador_id,
      valor_proposto: valorNum,
      status: proposalStatus,
      cargueiro_nome: trows[0]?.nome,
    });

    return res.status(201).json({ sucesso: true, id: proposalId, valor_proposto: valorNum, status: proposalStatus });
  } catch (err) {
    console.error('Erro criar proposta:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Update trip desired price (sent by client during negotiation)
app.post('/api/trips/:id/price', async (req, res) => {
  try {
    const tripId = req.params.id;
    const { valor } = req.body;
    if (valor === undefined || valor === null) return res.status(400).json({ error: 'valor é obrigatório' });
    const valorNum = Number(valor);
    if (Number.isNaN(valorNum)) return res.status(400).json({ error: 'valor inválido' });

    const [result] = await pool.query('UPDATE viagens SET valor = ? WHERE id = ?', [valorNum, tripId]);
    if (result.affectedRows === 0) return res.status(404).json({ error: 'Viagem não encontrada' });

    const [rows] = await pool.query('SELECT id, valor FROM viagens WHERE id = ? LIMIT 1', [tripId]);
    return res.json({ sucesso: true, trip: rows[0] });
  } catch (err) {
    console.error('Erro atualizar preço da viagem:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Cliente seleciona uma proposta e reserva a viagem para confirmação final do cargueiro
app.post('/api/trips/:id/select-proposal', async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const tripId = req.params.id;
    const { proposal_id, transportador_id, valor } = req.body;
    await conn.beginTransaction();

    const [vrows] = await conn.query('SELECT * FROM viagens WHERE id = ? FOR UPDATE', [tripId]);
    if (!vrows.length) {
      await conn.rollback();
      conn.release();
      return res.status(404).json({ error: 'Viagem não encontrada' });
    }
    const viagem = vrows[0];
    if (![TRIP_STATUS.WAITING_PROPOSALS, TRIP_STATUS.NEGOTIATING, 'pendente'].includes(viagem.status)) {
      await conn.rollback();
      conn.release();
      return res.status(409).json({ error: 'Viagem já foi selecionada ou não está disponível' });
    }

    let proposal;
    if (proposal_id) {
      const [prows] = await conn.query('SELECT * FROM propostas WHERE id = ? AND viagem_id = ? FOR UPDATE', [proposal_id, tripId]);
      proposal = prows[0];
    } else if (transportador_id) {
      const [prows] = await conn.query('SELECT * FROM propostas WHERE viagem_id = ? AND transportador_id = ? FOR UPDATE', [tripId, transportador_id]);
      proposal = prows[0];
    }
    if (!proposal) {
      await conn.rollback();
      conn.release();
      return res.status(404).json({ error: 'Proposta não encontrada' });
    }

    const selectedValue = Number(valor ?? proposal.valor_proposto);
    const config = await getOperationalConfig(conn);
    let confirmLimit = null;
    const scheduledDate = parseDateOrNull(viagem.scheduled_at);
    if (scheduledDate) confirmLimit = new Date(scheduledDate.getTime() - Number(config.prazo_confirmacao_horas) * 3600e3);

    await conn.query(
      `UPDATE viagens SET transportador_id = ?, status = ?, valor = ?, preco_final = ?, confirmacao_limite_at = ? WHERE id = ?`,
      [proposal.transportador_id, TRIP_STATUS.WAITING_CONFIRMATION, selectedValue, selectedValue, confirmLimit, tripId]
    );
    await conn.query("UPDATE propostas SET status = 'recusado' WHERE viagem_id = ? AND id <> ?", [tripId, proposal.id]);
    await conn.query("UPDATE propostas SET status = 'selecionada' WHERE id = ?", [proposal.id]);
    await addTripHistory(conn, tripId, viagem.status, TRIP_STATUS.WAITING_CONFIRMATION, 'cliente', viagem.cliente_id, 'Proposta selecionada pelo cliente');

    await conn.commit();
    conn.release();
    await notifyTripParticipants(tripId, 'proposta_selecionada', ['transportador']);
    return res.json({ sucesso: true, status: TRIP_STATUS.WAITING_CONFIRMATION, transportador_id: proposal.transportador_id, valor: selectedValue, confirmacao_limite_at: confirmLimit });
  } catch (err) {
    try { await conn.rollback(); } catch (_) {}
    conn.release();
    console.error('Erro selecionar proposta:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Cargueiro confirma disponibilidade final depois de ser selecionado pelo cliente
app.post('/api/trips/:id/confirm', async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const tripId = req.params.id;
    const { transportador_id } = req.body;
    await conn.beginTransaction();

    const [vrows] = await conn.query('SELECT * FROM viagens WHERE id = ? FOR UPDATE', [tripId]);
    if (!vrows.length) {
      await conn.rollback();
      conn.release();
      return res.status(404).json({ error: 'Viagem não encontrada' });
    }
    const viagem = vrows[0];
    if (viagem.status !== TRIP_STATUS.WAITING_CONFIRMATION && viagem.status !== 'aceito') {
      await conn.rollback();
      conn.release();
      return res.status(400).json({ error: 'Viagem não está a aguardar confirmação' });
    }
    if (transportador_id && Number(viagem.transportador_id) !== Number(transportador_id)) {
      await conn.rollback();
      conn.release();
      return res.status(403).json({ error: 'Transportador não autorizado para esta viagem' });
    }
    if (viagem.confirmacao_limite_at && new Date(viagem.confirmacao_limite_at).getTime() < Date.now()) {
      await conn.query("UPDATE viagens SET status = ? WHERE id = ?", [TRIP_STATUS.EXPIRED, tripId]);
      await addTripHistory(conn, tripId, viagem.status, TRIP_STATUS.EXPIRED, 'sistema', null, 'Prazo de confirmação expirado');
      await conn.commit();
      conn.release();
      return res.status(409).json({ error: 'Prazo de confirmação expirado' });
    }

    // Validação da taxa da Karrega e carteira pré-paga
    const config = await getOperationalConfig(conn);
    const taxaPct = Number(config.taxa_karrega_percentual || 0);
    const tripPrice = Number(viagem.preco_final || viagem.valor || 0);
    const requiredFee = Math.round((tripPrice * (taxaPct / 100)) * 100) / 100;

    if (taxaPct > 0 && requiredFee > 0) {
      const [wrows] = await conn.query('SELECT saldo FROM carteira_cargueiro WHERE transportador_id = ? FOR UPDATE', [viagem.transportador_id]);
      const currentBalance = wrows.length ? Number(wrows[0].saldo) : 0;
      if (currentBalance < requiredFee) {
        await conn.rollback();
        conn.release();
        return res.status(400).json({
          error: `Saldo insuficiente na carteira. É necessário ter pelo menos ${requiredFee.toLocaleString('pt-AO')} Kz (taxa de ${taxaPct}%) para confirmar esta viagem de ${tripPrice.toLocaleString('pt-AO')} Kz. Saldo atual: ${currentBalance.toLocaleString('pt-AO')} Kz.`,
          requiredFee,
          currentBalance,
          needTopup: true
        });
      }
    }

    // Gerar PIN de segurança de 4 dígitos para confirmação de entrega pelo cliente
    const pinEntrega = Math.floor(1000 + Math.random() * 9000).toString();

    await conn.query("UPDATE viagens SET status = ?, pin_entrega = ? WHERE id = ?", [TRIP_STATUS.CONFIRMED, pinEntrega, tripId]);
    await addTripHistory(conn, tripId, viagem.status, TRIP_STATUS.CONFIRMED, 'transportador', viagem.transportador_id, 'Disponibilidade confirmada');
    const [rows] = await conn.query('SELECT id, status, transportador_id, valor, preco_final, pin_entrega FROM viagens WHERE id = ? LIMIT 1', [tripId]);
    await conn.commit();
    conn.release();
    await notifyTripParticipants(tripId, 'viagem_confirmada');
    return res.json({ sucesso: true, trip: rows[0], pin_entrega: pinEntrega });
  } catch (err) {
    try { await conn.rollback(); } catch (_) {}
    conn.release();
    console.error('Erro confirmar viagem:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Driver accepts a pending 'na hora' request
app.post('/api/trips/:id/accept', async (req, res) => {
  try {
    const tripId = req.params.id;
    const { transportador_id } = req.body;
    if (!transportador_id) return res.status(400).json({ error: 'transportador_id é obrigatório' });

    // Bloqueia kargueiros não verificados ou inativos
    const [trows] = await pool.query('SELECT estado, verificado FROM transportadores WHERE id = ? LIMIT 1', [transportador_id]);
    if (!trows.length) return res.status(404).json({ error: 'Transportador não encontrado' });
    if (trows[0].estado !== 'ativo') return res.status(403).json({ error: 'Conta inativa ou suspensa. Contacte o suporte Karrega.' });
    if (trows[0].verificado !== 'verificado') {
      return res.status(403).json({ error: 'Conta em análise. Os seus documentos ainda estão a ser validados pela equipa Karrega.' });
    }

    const [vrows] = await pool.query('SELECT valor FROM viagens WHERE id = ? LIMIT 1', [tripId]);
    if (!vrows.length) return res.status(404).json({ error: 'Viagem não encontrada' });

    const [existing] = await pool.query('SELECT id FROM propostas WHERE viagem_id = ? AND transportador_id = ? LIMIT 1', [tripId, transportador_id]);
    if (existing.length) {
      await pool.query("UPDATE propostas SET valor_proposto = ?, status = 'pendente', created_at = NOW() WHERE id = ?", [vrows[0].valor || 0, existing[0].id]);
    } else {
      await pool.query("INSERT INTO propostas (viagem_id, transportador_id, valor_proposto, status, created_at) VALUES (?, ?, ?, 'pendente', NOW())", [tripId, transportador_id, vrows[0].valor || 0]);
    }
    await pool.query(`UPDATE viagens SET status = ? WHERE id = ? AND status IN ('pendente', ?, ?)`, [TRIP_STATUS.NEGOTIATING, tripId, TRIP_STATUS.WAITING_PROPOSALS, TRIP_STATUS.NEGOTIATING]);

    return res.json({ sucesso: true, message: 'Preço aceite. Aguarde seleção do cliente.' });
  } catch (err) { console.error('Erro accept trip:', err); return res.status(500).json({ error: 'Erro no servidor' }); }
});

// Driver cancels (rescind) an accepted assignment or client cancels
app.post('/api/trips/:id/cancel', async (req, res) => {
  try {
    const tripId = req.params.id;
    const { motivo } = req.body || {};

    const [tripRows] = await pool.query('SELECT * FROM viagens WHERE id = ? LIMIT 1', [tripId]);
    if (!tripRows.length) return res.status(404).json({ error: 'Viagem não encontrada' });
    const tripToCancel = tripRows[0];

    const [result] = await pool.query("UPDATE viagens SET status = 'cancelado', cancel_reason = ? WHERE id = ? AND status <> 'feito'", [motivo || null, tripId]);
    if (result.affectedRows === 0) return res.status(400).json({ error: 'Não foi possível cancelar a viagem' });
    // CORRIGIDO: 'cancelado' não existe no ENUM de propostas; usa 'recusado'
    await pool.query("UPDATE propostas SET status = 'recusado' WHERE viagem_id = ? AND status IN ('pendente', 'aceito')", [tripId]);
    await notifyTripParticipants(tripId, 'viagem_cancelada');

    // Estorno automático da taxa da Karrega se a viagem já havia sido confirmada
    try {
      if (tripToCancel.transportador_id && ['confirmada', 'a_caminho_recolha', 'carga_recolhida', 'em_transito'].includes(tripToCancel.status)) {
        const config = await getOperationalConfig(pool);
        const taxaPct = Number(config.taxa_karrega_percentual || 0);
        const tripPrice = Number(tripToCancel.preco_final || tripToCancel.valor || 0);
        const feeToRefund = Math.round((tripPrice * (taxaPct / 100)) * 100) / 100;
        if (taxaPct > 0 && feeToRefund > 0) {
          await pool.query('UPDATE carteira_cargueiro SET saldo = saldo + ? WHERE transportador_id = ?', [feeToRefund, tripToCancel.transportador_id]);
          await addTripHistory(pool, tripId, tripToCancel.status, 'cancelado', 'sistema', null, `Taxa de ${feeToRefund.toLocaleString('pt-AO')} Kz estornada para a carteira`);
        }
      }
    } catch (refundErr) {
      console.warn('Aviso no estorno de taxa:', refundErr.message);
    }

    return res.json({ sucesso: true });
  } catch (err) { console.error('Erro cancelar viagem:', err); return res.status(500).json({ error: 'Erro no servidor' }); }
});

// Start the trip: set started_at and move to 'em_transito'
app.post('/api/trips/:id/start', async (req, res) => {
  try {
    const tripId = req.params.id;
    const { transportador_id } = req.body;

    const [vrows] = await pool.query('SELECT status, transportador_id FROM viagens WHERE id = ? LIMIT 1', [tripId]);
    if (!vrows || vrows.length === 0) return res.status(404).json({ error: 'Viagem não encontrada' });
    const viagem = vrows[0];
    if (![TRIP_STATUS.CONFIRMED, 'aceito'].includes(viagem.status)) return res.status(400).json({ error: 'Viagem deve estar confirmada para iniciar' });
    if (transportador_id && viagem.transportador_id !== parseInt(transportador_id, 10)) return res.status(403).json({ error: 'Transportador não autorizado a iniciar esta viagem' });

    const now = new Date();
    try {
      await pool.query("UPDATE viagens SET status = ?, started_at = ? WHERE id = ?", [TRIP_STATUS.IN_TRANSIT, now, tripId]);
    } catch (sqlErr) {
      console.error('Erro ao atualizar status started_at:', sqlErr);
      try {
        await pool.query("UPDATE viagens SET started_at = ? WHERE id = ?", [now, tripId]);
        const [rows] = await pool.query('SELECT * FROM viagens WHERE id = ? LIMIT 1', [tripId]);
        return res.json({ sucesso: true, partial: true, message: 'started_at gravado, mas não foi possível alterar status (ver logs)', started_at: now, trip: rows[0] });
      } catch (e2) {
        console.error('Erro ao gravar started_at fallback:', e2);
        return res.status(500).json({ error: 'Erro ao iniciar viagem', details: e2.message || e2 });
      }
    }

    const [updated] = await pool.query(
      `SELECT v.*, c.nome AS cliente_nome, c.numero AS cliente_numero, t.nome AS transportador_nome, t.numero AS transportador_numero
       FROM viagens v
       LEFT JOIN clientes c ON v.cliente_id = c.id
       LEFT JOIN transportadores t ON v.transportador_id = t.id
       WHERE v.id = ? LIMIT 1`,
      [tripId]
    );
    return res.json({ sucesso: true, started_at: now, trip: updated[0] });
  } catch (err) { console.error('Erro iniciar viagem:', err); return res.status(500).json({ error: 'Erro no servidor' }); }
});

// Requests endpoint: list realtime 'na hora' pending requests (for drivers), filtered by vehicle type
app.get('/api/requests', async (req, res) => {
  try {
    const { tipo_de_transporte } = req.query;
    let sql = `SELECT v.id, v.tipo, v.status, v.origin, v.destination, v.tamanho_carga, v.descricao_da_carga, v.tipo_de_transporte, v.created_at, v.valor, 
           c.id AS cliente_id, c.nome AS cliente_nome, c.numero AS cliente_numero
           FROM viagens v
           LEFT JOIN clientes c ON v.cliente_id = c.id
           WHERE v.tipo = 'na hora' AND v.status IN ('pendente', 'aguardando_propostas', 'em_negociacao') AND v.transportador_id IS NULL AND v.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)`;
    const params = [];

    if (tipo_de_transporte) {
      const aliases = transportTypeAliases(tipo_de_transporte);
      sql += ` AND LOWER(v.tipo_de_transporte) IN (${aliases.map(() => '?').join(', ')})`;
      params.push(...aliases);
    }

    sql += ' ORDER BY v.created_at DESC';
    const [rows] = await pool.query(sql, params);
    return res.json({ sucesso: true, total: rows.length, requests: rows });
  } catch (err) {
    console.error('Erro listar requests:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Endpoint para criar requests (sincronização offline e compatibilidade)
app.post('/api/requests', async (req, res) => {
  try {
    let { title, origin, destination, created_at, cliente_id, cliente_numero } = req.body;

    if (!cliente_id && cliente_numero) {
      const [crows] = await pool.query('SELECT id FROM clientes WHERE numero = ? LIMIT 1', [cliente_numero]);
      if (crows && crows.length > 0) {
        cliente_id = crows[0].id;
      }
    }

    if (!cliente_id) {
      const [firstClient] = await pool.query('SELECT id FROM clientes LIMIT 1');
      if (firstClient && firstClient.length > 0) {
        cliente_id = firstClient[0].id;
      } else {
        return res.status(400).json({ error: 'cliente_id ou cliente_numero é obrigatório' });
      }
    }

    const [result] = await pool.query(
      "INSERT INTO viagens (cliente_id, status, tipo, origin, destination, descricao_da_carga, created_at) VALUES (?, ?, 'na hora', ?, ?, ?, ?)",
      [cliente_id, TRIP_STATUS.WAITING_PROPOSALS, origin || null, destination || null, title || null, created_at || new Date()]
    );
    emitNewTripEvent('ligeiro', {
      id: result.insertId,
      origin,
      destination,
      descricao_da_carga: title,
      tipo: 'na hora',
      created_at: new Date(),
    });
    return res.status(201).json({ ok: true, id: result.insertId });
  } catch (err) {
    console.error('Erro ao criar request via POST:', err);
    return res.status(500).json({ error: err.message });
  }
});

// Agendamentos - endpoint dedicado
app.post('/api/agendamentos', async (req, res) => {
  try {
    let { cliente_id, cliente_numero, transportador_id, scheduled_at, origin, destination, tipo_de_transporte } = req.body;

    if (!cliente_id && cliente_numero) {
      const [crows] = await pool.query('SELECT id FROM clientes WHERE numero = ? LIMIT 1', [cliente_numero]);
      if (!crows || crows.length === 0) return res.status(400).json({ error: 'Cliente não encontrado para o número informado' });
      cliente_id = crows[0].id;
    }

    if (!cliente_id) return res.status(400).json({ error: 'cliente_id ou cliente_numero obrigatório' });

    if (!scheduled_at) return res.status(400).json({ error: 'scheduled_at é obrigatório para agendamentos' });
    const dt = new Date(scheduled_at);
    if (isNaN(dt.getTime())) return res.status(400).json({ error: 'scheduled_at inválido (formato esperado: ISO 8601)' });
    const now = new Date();
    if (dt.getTime() <= now.getTime()) return res.status(400).json({ error: 'scheduled_at deve ser uma data/hora futura' });

    const tipo = 'agendado';
    const config = await getOperationalConfig();
    const minDate = new Date(Date.now() + Number(config.antecedencia_minima_horas) * 3600e3);
    if (dt.getTime() < minDate.getTime()) {
      return res.status(400).json({ error: `Agendamentos devem ter pelo menos ${config.antecedencia_minima_horas} horas de antecedência` });
    }
    const status = TRIP_STATUS.WAITING_PROPOSALS;

    const { descricao_da_carga, tamanho_carga } = req.body;
    const scheduledSql = dt.toISOString().slice(0, 19).replace('T', ' ');
    const [result] = await pool.query(
      'INSERT INTO viagens (cliente_id, transportador_id, status, tipo, scheduled_at, origin, destination, descricao_da_carga, tamanho_carga, tipo_de_transporte) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      [cliente_id, transportador_id || null, status, tipo, scheduledSql, origin || null, destination || null, descricao_da_carga || null, tamanho_carga || null, tipo_de_transporte || 'ligeiro']
    );

    emitNewTripEvent(tipo_de_transporte || 'ligeiro', {
      id: result.insertId,
      origin,
      destination,
      descricao_da_carga,
      tamanho_carga,
      tipo_de_transporte: tipo_de_transporte || 'ligeiro',
      tipo: 'agendado',
      scheduled_at: scheduledSql,
      created_at: new Date(),
    });

    return res.status(201).json({ ok: true, id: result.insertId });
  } catch (err) {
    console.error('Erro agendamentos:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

app.get('/api/agendamentos', async (req, res) => {
  try {
    const { tipo_de_transporte } = req.query;
    let whereClause = "WHERE v.tipo = 'agendado' AND v.status IN ('pendente','aguardando_propostas','em_negociacao') AND v.scheduled_at > NOW()";
    const params = [];
    if (tipo_de_transporte) {
      const aliases = transportTypeAliases(tipo_de_transporte);
      whereClause += ` AND LOWER(v.tipo_de_transporte) IN (${aliases.map(() => '?').join(', ')})`;
      params.push(...aliases);
    }
    const [rows] = await pool.query(`SELECT v.*, c.nome AS cliente_nome, c.numero AS cliente_numero, t.nome AS transportador_nome, t.numero AS transportador_numero FROM viagens v
      LEFT JOIN clientes c ON v.cliente_id = c.id
      LEFT JOIN transportadores t ON v.transportador_id = t.id
      ${whereClause} ORDER BY v.scheduled_at ASC`, params);
    return res.json({ sucesso: true, total: rows.length, agendamentos: rows });
  } catch (err) {
    console.error('Erro listar agendamentos:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Estatísticas agregadas (opcionalmente por transportador)
app.get('/api/stats', async (req, res) => {
  try {
    const { transportador_id } = req.query;
    let where = '';
    const params = [];
    if (transportador_id) {
      where = ' WHERE transportador_id = ?';
      params.push(transportador_id);
    }

    const [totalRows] = await pool.query(`SELECT COUNT(*) AS total FROM viagens${where}`, params);
    let weekSql = 'SELECT COUNT(*) AS weekCount FROM viagens WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)';
    let pendingSql = "SELECT COUNT(*) AS pending FROM viagens WHERE status <> 'feito'";
    const qParams = [];
    if (transportador_id) {
      weekSql = `SELECT COUNT(*) AS weekCount FROM viagens WHERE transportador_id = ? AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)`;
      pendingSql = `SELECT COUNT(*) AS pending FROM viagens WHERE transportador_id = ? AND status <> 'feito'`;
      qParams.push(transportador_id);
    }
    const [weekRows] = await pool.query(weekSql, qParams);
    const [pendingRows] = await pool.query(pendingSql, qParams);

    const total = totalRows[0]?.total || 0;
    const weekCount = weekRows[0]?.weekCount || 0;
    const pending = pendingRows[0]?.pending || 0;

    return res.json({ sucesso: true, total, weekCount, pending });
  } catch (err) {
    console.error('Erro ao calcular estatísticas:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Incomes: criar e listar registros financeiros pós-viagem
app.post('/api/incomes', async (req, res) => {
  try {
    const { viagem_id, taxa, valor_pago_cliente, valor_transportador, valor_app } = req.body;
    if (!viagem_id) return res.status(400).json({ error: 'viagem_id é obrigatório' });

    const [vrows] = await pool.query('SELECT id FROM viagens WHERE id = ? LIMIT 1', [viagem_id]);
    if (!vrows || vrows.length === 0) return res.status(404).json({ error: 'Viagem não encontrada' });

    const [result] = await pool.query(
      'INSERT INTO incomes (viagem_id, taxa, valor_pago_cliente, valor_transportador, valor_app) VALUES (?, ?, ?, ?, ?)',
      [viagem_id, taxa || null, valor_pago_cliente || 0.0, valor_transportador || 0.0, valor_app || 0.0]
    );
    return res.status(201).json({ sucesso: true, id: result.insertId });
  } catch (err) {
    console.error('Erro criar income:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

app.get('/api/incomes', async (req, res) => {
  try {
    const { viagem_id } = req.query;
    let sql = 'SELECT i.*, v.tipo, v.status, v.cliente_id, v.transportador_id FROM incomes i LEFT JOIN viagens v ON i.viagem_id = v.id';
    const params = [];
    if (viagem_id) { sql += ' WHERE i.viagem_id = ?'; params.push(viagem_id); }
    const [rows] = await pool.query(sql, params);
    return res.json({ sucesso: true, total: rows.length, incomes: rows });
  } catch (err) {
    console.error('Erro listar incomes:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Estatísticas financeiras agregadas a partir da tabela incomes
app.get('/api/incomes/stats', async (req, res) => {
  try {
    const { transportador_id } = req.query;
    let sql = `SELECT 
      COUNT(*) AS count, 
      COALESCE(SUM(i.valor_pago_cliente),0) AS total_paid, 
      COALESCE(SUM(i.valor_transportador),0) AS total_transportador, 
      COALESCE(SUM(i.valor_app),0) AS total_app
      FROM incomes i
      LEFT JOIN viagens v ON i.viagem_id = v.id`;
    const params = [];
    if (transportador_id) {
      sql += ' WHERE v.transportador_id = ?';
      params.push(transportador_id);
    }
    const [rows] = await pool.query(sql, params);
    const r = rows[0] || { count: 0, total_paid: 0, total_transportador: 0, total_app: 0 };
    return res.json({ sucesso: true, count: r.count, total_paid: r.total_paid, total_transportador: r.total_transportador, total_app: r.total_app });
  } catch (err) {
    console.error('Erro incomes stats:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Finalizar viagem: cargueiro registra conclusão da entrega (com validação de PIN de 4 dígitos)
app.post('/api/trips/:id/finish', async (req, res) => {
  const conn = await pool.getConnection();
  try {
    const viagemId = req.params.id;
    const { valor_pago_cliente, taxa, pin } = req.body;

    await conn.beginTransaction();
    const [vrows] = await conn.query('SELECT * FROM viagens WHERE id = ? FOR UPDATE', [viagemId]);
    if (!vrows || vrows.length === 0) {
      await conn.rollback();
      conn.release();
      return res.status(404).json({ error: 'Viagem não encontrada' });
    }
    const viagem = vrows[0];

    // Validação do PIN de 4 dígitos fornecido pelo cliente
    if (viagem.pin_entrega && pin) {
      if (String(pin).trim() !== String(viagem.pin_entrega).trim()) {
        await conn.rollback();
        conn.release();
        return res.status(400).json({ error: 'Código PIN de entrega incorreto. Peça o código de 4 dígitos ao cliente após descarregar.' });
      }
    }

    let paid = parseFloat(valor_pago_cliente || viagem.preco_final || viagem.valor || 0) || 0.0;
    const config = await getOperationalConfig(conn);
    const appPct = Number(config.taxa_karrega_percentual || 0) / 100;
    const valor_app = Math.round((paid * appPct) * 100) / 100;
    const valor_transportador = Math.round((paid - valor_app) * 100) / 100;

    await conn.query('UPDATE viagens SET status = ?, entrega_registada_at = ? WHERE id = ?', [TRIP_STATUS.WAITING_DELIVERY_CONFIRMATION, new Date(), viagemId]);

    const [ins] = await conn.query(
      'INSERT INTO incomes (viagem_id, taxa, valor_pago_cliente, valor_transportador, valor_app) VALUES (?, ?, ?, ?, ?)',
      [viagemId, taxa || null, paid, valor_transportador, valor_app]
    );

    // Se houver comissão da Karrega maior que zero, desconta da carteira do cargueiro
    if (valor_app > 0 && viagem.transportador_id) {
      await conn.query(
        'INSERT INTO carteira_cargueiro (transportador_id, saldo) VALUES (?, -?) ON DUPLICATE KEY UPDATE saldo = saldo - ?',
        [viagem.transportador_id, valor_app, valor_app]
      );
    }

    await conn.commit();
    conn.release();
    await notifyTripParticipants(viagemId, 'entrega_registada', ['cliente']);
    return res.json({ sucesso: true, income_id: ins.insertId, status: TRIP_STATUS.WAITING_DELIVERY_CONFIRMATION, valor_pago_cliente: paid, valor_transportador, valor_app });
  } catch (err) {
    try { await conn.rollback(); } catch (e) {}
    conn.release();
    console.error('Erro finalizar viagem:', err);
    return res.status(500).json({ error: 'Erro ao finalizar viagem' });
  }
});

// --- ROTAS DA CARTEIRA PRÉ-PAGA DO CARGUEIRO ---
app.get('/api/wallet/balance', async (req, res) => {
  try {
    const transportador_id = req.query.transportador_id;
    if (!transportador_id) return res.status(400).json({ error: 'transportador_id obrigatório' });
    const [rows] = await pool.query('SELECT saldo FROM carteira_cargueiro WHERE transportador_id = ? LIMIT 1', [transportador_id]);
    const saldo = rows.length ? Number(rows[0].saldo) : 0.00;
    const config = await getOperationalConfig();
    return res.json({ sucesso: true, saldo, taxa_percentual: Number(config.taxa_karrega_percentual) });
  } catch (err) {
    console.error('Erro ao buscar saldo:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

app.post('/api/wallet/topup', async (req, res) => {
  try {
    const { transportador_id, valor, comprovativo_path } = req.body;
    const valorNum = Number(valor);
    if (!transportador_id || isNaN(valorNum) || valorNum <= 0) {
      return res.status(400).json({ error: 'Dados de recarga inválidos' });
    }
    const [result] = await pool.query(
      'INSERT INTO carteira_recargas (transportador_id, valor, comprovativo_path, status) VALUES (?, ?, ?, ?)',
      [transportador_id, valorNum, comprovativo_path || null, 'pendente']
    );
    return res.status(201).json({ sucesso: true, id: result.insertId, message: 'Solicitação de recarga enviada com sucesso para análise' });
  } catch (err) {
    console.error('Erro ao enviar recarga:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

app.get('/api/wallet/transactions', async (req, res) => {
  try {
    const transportador_id = req.query.transportador_id;
    if (!transportador_id) return res.status(400).json({ error: 'transportador_id obrigatório' });
    const [recargas] = await pool.query(
      'SELECT id, valor, status, observacao, created_at FROM carteira_recargas WHERE transportador_id = ? ORDER BY created_at DESC LIMIT 50',
      [transportador_id]
    );
    return res.json({ sucesso: true, recargas });
  } catch (err) {
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

// Cliente confirma a receção e responde aos campos obrigatórios de encerramento
app.post('/api/trips/:id/confirm-delivery', async (req, res) => {
  try {
    const tripId = req.params.id;
    const { atraso_recolha_confirmado, limpeza_transporte } = req.body;
    if (typeof atraso_recolha_confirmado !== 'boolean') {
      return res.status(400).json({ error: 'atraso_recolha_confirmado deve ser true ou false' });
    }
    if (!['limpo', 'moderado', 'sujo'].includes(limpeza_transporte)) {
      return res.status(400).json({ error: 'limpeza_transporte inválida' });
    }

    const [vrows] = await pool.query('SELECT status FROM viagens WHERE id = ? LIMIT 1', [tripId]);
    if (!vrows.length) return res.status(404).json({ error: 'Viagem não encontrada' });
    if (![TRIP_STATUS.WAITING_DELIVERY_CONFIRMATION, 'feito'].includes(vrows[0].status)) {
      return res.status(400).json({ error: 'Entrega ainda não foi registada pelo cargueiro' });
    }

    await pool.query(
      'UPDATE viagens SET status = ?, ended_at = ?, cliente_confirmou_entrega_at = ?, atraso_recolha_confirmado = ?, limpeza_transporte = ? WHERE id = ?',
      [TRIP_STATUS.DONE, new Date(), new Date(), atraso_recolha_confirmado, limpeza_transporte, tripId]
    );
    await addTripHistory(pool, tripId, vrows[0].status, TRIP_STATUS.DONE, 'cliente', null, 'Entrega confirmada pelo cliente');
    await notifyTripParticipants(tripId, 'entrega_confirmada');
    return res.json({ sucesso: true, status: TRIP_STATUS.DONE });
  } catch (err) {
    console.error('Erro confirmar entrega:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});

app.post('/api/trips/:id/ratings', async (req, res) => {
  try {
    const tripId = req.params.id;
    const {
      avaliador_tipo,
      avaliador_id,
      avaliado_id,
      pontualidade,
      comunicacao,
      cuidado_carga,
      limpeza_transporte,
      cumprimento_acordo,
      clareza_informacoes,
      classificacao_geral,
      comentario,
    } = req.body;
    if (!['cliente', 'transportador'].includes(avaliador_tipo)) return res.status(400).json({ error: 'avaliador_tipo inválido' });
    const geral = Number(classificacao_geral);
    if (!avaliador_id || !avaliado_id || !Number.isInteger(geral) || geral < 1 || geral > 5) {
      return res.status(400).json({ error: 'Dados de avaliação inválidos' });
    }
    const [vrows] = await pool.query('SELECT status FROM viagens WHERE id = ? LIMIT 1', [tripId]);
    if (!vrows.length) return res.status(404).json({ error: 'Viagem não encontrada' });
    if (vrows[0].status !== TRIP_STATUS.DONE) return res.status(400).json({ error: 'Só é possível avaliar viagens concluídas' });

    await pool.query(
      `INSERT INTO avaliacoes
       (viagem_id, avaliador_tipo, avaliador_id, avaliado_id, pontualidade, comunicacao, cuidado_carga, limpeza_transporte, cumprimento_acordo, clareza_informacoes, classificacao_geral, comentario)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [tripId, avaliador_tipo, avaliador_id, avaliado_id, pontualidade || null, comunicacao || null, cuidado_carga || null, limpeza_transporte || null, cumprimento_acordo || null, clareza_informacoes || null, geral, comentario || null]
    );
    return res.status(201).json({ sucesso: true });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(409).json({ error: 'Esta viagem já foi avaliada por este utilizador' });
    console.error('Erro gravar avaliação:', err);
    return res.status(500).json({ error: 'Erro no servidor' });
  }
});



// Listar apenas viagens do tipo 'agendado'
app.get('/agendamento', async (req, res) => {
  try {
    const sql = `
      SELECT 
        v.id, v.tipo, v.status, v.scheduled_at, 
        c.nome AS cliente_nome, c.numero AS cliente_contato,
        t.nome AS transportador_nome 
      FROM viagens v
      LEFT JOIN clientes c ON v.cliente_id = c.id
      LEFT JOIN transportadores t ON v.transportador_id = t.id
      WHERE v.tipo = 'agendado'
      ORDER BY v.scheduled_at ASC
    `;
    const [rows] = await pool.query(sql);
    return res.json({ sucesso: true, total: rows.length, agendamentos: rows });
  } catch (err) {
    console.error('Erro ao listar agendamentos:', err.message);
    return res.status(500).json({ error: 'Erro ao buscar agendamentos no servidor' });
  }
});

// --- FINALIZAÇÃO ---

// EXPORTAÇÃO OBRIGATÓRIA PARA VERCEL
export default app;

if (process.env.NODE_ENV !== 'production') {
  server.listen(PORT, () => console.log(`🚀 Servidor com WebSockets rodando em http://localhost:${PORT}`));
}

if (process.env.ENABLE_INTERNAL_CRON !== 'false') {
  const intervalMinutes = Number(process.env.EXPIRE_CONFIRMATIONS_INTERVAL_MINUTES || 5);
  setInterval(() => {
    expirePendingConfirmations().catch((err) => console.error('Job de expiração falhou:', err.message));
  }, Math.max(1, intervalMinutes) * 60 * 1000).unref?.();
}
