// src/admin.js — rotas do painel administrativo
import { Router } from "express";
import crypto from "crypto";
import { enviarWhatsApp, mensagem } from "./whatsapp.js";
import { pgPool } from "./supabase.js";

export default function adminRoutes(pool) {
  const r = Router();
  const secret = process.env.ADMIN_SECRET || "troque-este-segredo";
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
  const mac = (s) => crypto.createHmac("sha256", secret).update(s).digest("base64url");
  const q = async (sql, p = []) => (await pool.query(sql, p))[0];
  const pedidosOperacionais = (where, params = []) => q(`SELECT
      v.id, v.status, v.tipo, v.origin, v.destination, v.valor, v.preco_final,
      v.scheduled_at, v.confirmacao_limite_at, v.entrega_registada_at, v.created_at,
      c.nome cliente, c.numero cliente_numero,
      t.nome cargueiro, t.numero cargueiro_numero
      FROM viagens v
      LEFT JOIN clientes c ON c.id=v.cliente_id
      LEFT JOIN transportadores t ON t.id=v.transportador_id
      WHERE ${where}
      ORDER BY COALESCE(v.confirmacao_limite_at, v.entrega_registada_at, v.scheduled_at, v.created_at) ASC
      LIMIT 100`, params);

  const schemaReady = (async () => {
    try {
      if (pool.isPostgres) return;
      for (const s of [
        "ALTER TABLE clientes ADD COLUMN estado VARCHAR(20) DEFAULT 'ativo'",
        "ALTER TABLE transportadores ADD COLUMN estado VARCHAR(20) DEFAULT 'ativo'",
        "ALTER TABLE transportadores ADD COLUMN verificado VARCHAR(20) DEFAULT 'pendente'",
        "ALTER TABLE transportadores ADD COLUMN foto_perfil_url VARCHAR(500) NULL",
        "ALTER TABLE transportadores ADD COLUMN data_nascimento VARCHAR(50) NULL",
        "ALTER TABLE transportadores ADD COLUMN foto_bi_verso_url VARCHAR(500) NULL",
        "ALTER TABLE transportadores ADD COLUMN numero_carta VARCHAR(100) NULL",
        "ALTER TABLE transportadores ADD COLUMN foto_carta_url VARCHAR(500) NULL",
        "ALTER TABLE transportadores ADD COLUMN foto_veiculo_url VARCHAR(500) NULL",
        "ALTER TABLE transportadores ADD COLUMN veiculo_marca VARCHAR(100) NULL",
        "ALTER TABLE transportadores ADD COLUMN veiculo_modelo VARCHAR(100) NULL",
        "ALTER TABLE transportadores ADD COLUMN veiculo_ano INT NULL",
        "ALTER TABLE transportadores ADD COLUMN veiculo_cor VARCHAR(50) NULL",
        "ALTER TABLE transportadores ADD COLUMN veiculo_placa VARCHAR(50) NULL",
      ]) { try { await pool.query(s); } catch (_) {} }

      try {
        const [feeColumn] = await pool.query(
          `SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
           WHERE TABLE_SCHEMA = DATABASE()
             AND TABLE_NAME = 'configuracoes_operacionais'
             AND COLUMN_NAME = 'taxa_karrega_percentual'
           LIMIT 1`
        );
        if (feeColumn.length === 0) {
          await pool.query(`ALTER TABLE configuracoes_operacionais
            ADD COLUMN taxa_karrega_percentual DECIMAL(5,2) NOT NULL DEFAULT 0.00
            AFTER cancelamento_automatico`);
        }
      } catch (_) {}

      try {
        await pool.query(`ALTER TABLE configuracoes_operacionais
          ADD COLUMN bonus_boas_vindas DECIMAL(12,2) NOT NULL DEFAULT 0.00`);
      } catch (_) {}

      try {
        await pool.query(`CREATE TABLE IF NOT EXISTS carteira_cargueiro (
          transportador_id INT PRIMARY KEY,
          saldo DECIMAL(12,2) NOT NULL DEFAULT 0.00,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          CONSTRAINT fk_carteira_transportador FOREIGN KEY (transportador_id) REFERENCES transportadores(id) ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
      } catch (_) {}

      try {
        await pool.query(`CREATE TABLE IF NOT EXISTS carteira_recargas (
          id INT AUTO_INCREMENT PRIMARY KEY,
          transportador_id INT NOT NULL,
          valor DECIMAL(12,2) NOT NULL,
          comprovativo_path MEDIUMTEXT NULL,
          status ENUM('pendente', 'aprovada', 'recusada') NOT NULL DEFAULT 'pendente',
          observacao VARCHAR(255) NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX idx_recargas_transportador (transportador_id),
          INDEX idx_recargas_status (status),
          CONSTRAINT fk_recargas_transportador FOREIGN KEY (transportador_id) REFERENCES transportadores(id) ON DELETE CASCADE ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
      } catch (_) {}
    } catch (err) {
      console.warn("Aviso ao preparar schema admin:", err.message);
    }
  })();

  r.use((req, res, next) => {
    schemaReady.then(() => next()).catch((error) => {
      console.error("Erro ao preparar configurações do banco:", error.message);
      res.status(503).json({ error: "Banco de dados ainda não está preparado" });
    });
  });

  r.post("/login", (req, res) => {
    const { email, password } = req.body;
    if (!process.env.ADMIN_EMAIL || email !== process.env.ADMIN_EMAIL || password !== process.env.ADMIN_PASSWORD)
      return res.status(401).json({ error: "Credenciais inválidas" });
    const body = b64({ email, exp: Date.now() + 12 * 3600e3 });
    res.json({ ok: true, token: body + "." + mac(body) });
  });

  r.use((req, res, next) => {
    try {
      const [body, sig] = (req.headers.authorization || "").replace("Bearer ", "").split(".");
      if (!body || sig !== mac(body) || JSON.parse(Buffer.from(body, "base64url")).exp < Date.now()) throw 0;
      next();
    } catch { res.status(401).json({ error: "Não autorizado" }); }
  });

  r.get("/stats", async (_, res) => {
    try {
      const [c] = await q("SELECT COUNT(*) n FROM clientes");
      const [t] = await q("SELECT COUNT(*) n FROM transportadores");
      const status = await q("SELECT status, COUNT(*) n FROM viagens GROUP BY status");
      const [rev] = await q("SELECT COALESCE(SUM(valor_pago_cliente),0) total, COALESCE(SUM(valor_app),0) app FROM incomes");
      const rotas = await q("SELECT origin, destination, COUNT(*) n FROM viagens WHERE origin IS NOT NULL GROUP BY origin, destination ORDER BY n DESC LIMIT 5");
      res.json({ clientes: c.n, cargueiros: t.n, status, receita: rev, rotas });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/configuracoes", async (_, res) => {
    try {
      const [cfg] = await q("SELECT * FROM configuracoes_operacionais WHERE id=1 LIMIT 1");
      res.json(cfg || {
        id: 1,
        antecedencia_minima_horas: 72,
        prazo_confirmacao_horas: 24,
        cancelamento_automatico: 1,
        taxa_karrega_percentual: 0.00,
        bonus_boas_vindas: 0.00,
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/operacoes", async (_, res) => {
    try {
      const aExpirar = await pedidosOperacionais(
        "v.status='aguardando_confirmacao' AND v.confirmacao_limite_at IS NOT NULL AND v.confirmacao_limite_at <= DATE_ADD(NOW(), INTERVAL 12 HOUR)"
      );
      const emDisputa = await pedidosOperacionais("v.status='em_disputa'");
      const entregaPendente = await pedidosOperacionais("v.status='aguardando_confirmacao_entrega'");
      res.json({
        a_expirar: aExpirar,
        em_disputa: emDisputa,
        entrega_pendente: entregaPendente,
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.post("/configuracoes", async (req, res) => {
    try {
      const antecedencia = Number(req.body.antecedencia_minima_horas);
      const confirmacao = Number(req.body.prazo_confirmacao_horas);
      const taxaKarrega = Number(req.body.taxa_karrega_percentual);
      const bonusBoasVindas = Number(req.body.bonus_boas_vindas ?? 0);
      const cancelamento = req.body.cancelamento_automatico ? 1 : 0;
      if (!Number.isInteger(antecedencia) || antecedencia < 0 || antecedencia > 720) {
        return res.status(400).json({ error: "Antecedência mínima inválida" });
      }
      if (!Number.isInteger(confirmacao) || confirmacao < 0 || confirmacao > 168) {
        return res.status(400).json({ error: "Prazo de confirmação inválido" });
      }
      if (!Number.isFinite(taxaKarrega) || taxaKarrega < 0 || taxaKarrega > 100) {
        return res.status(400).json({ error: "Percentagem da taxa Karrega inválida" });
      }
      if (!Number.isFinite(bonusBoasVindas) || bonusBoasVindas < 0) {
        return res.status(400).json({ error: "Bónus de boas-vindas inválido" });
      }
      await q(`INSERT INTO configuracoes_operacionais
        (id, antecedencia_minima_horas, prazo_confirmacao_horas, cancelamento_automatico, taxa_karrega_percentual, bonus_boas_vindas)
        VALUES (1, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          antecedencia_minima_horas=VALUES(antecedencia_minima_horas),
          prazo_confirmacao_horas=VALUES(prazo_confirmacao_horas),
          cancelamento_automatico=VALUES(cancelamento_automatico),
          taxa_karrega_percentual=VALUES(taxa_karrega_percentual),
          bonus_boas_vindas=VALUES(bonus_boas_vindas)`,
        [antecedencia, confirmacao, cancelamento, taxaKarrega, bonusBoasVindas]);
      const [cfg] = await q("SELECT * FROM configuracoes_operacionais WHERE id=1 LIMIT 1");
      res.json({ ok: true, configuracoes: cfg });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Gestão de recargas de carteira de cargueiros
  r.get("/recargas", async (_, res) => {
    try {
      const recargas = await q(`SELECT r.*, t.nome transportador, t.numero transportador_numero, t.tipo_transporte
        FROM carteira_recargas r
        JOIN transportadores t ON t.id = r.transportador_id
        ORDER BY FIELD(r.status, 'pendente', 'aprovada', 'recusada'), r.created_at DESC LIMIT 200`);
      res.json(recargas);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.post("/recargas/:id/aprovar", async (req, res) => {
    const conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      const [rows] = await conn.query("SELECT * FROM carteira_recargas WHERE id = ? FOR UPDATE", [req.params.id]);
      if (!rows.length) {
        await conn.rollback(); conn.release();
        return res.status(404).json({ error: "Recarga não encontrada" });
      }
      const recarga = rows[0];
      if (recarga.status !== 'pendente') {
        await conn.rollback(); conn.release();
        return res.status(400).json({ error: `Recarga já está com status '${recarga.status}'` });
      }

      await conn.query("UPDATE carteira_recargas SET status = 'aprovada', updated_at = NOW() WHERE id = ?", [recarga.id]);
      await conn.query(
        "INSERT INTO carteira_cargueiro (transportador_id, saldo) VALUES (?, ?) ON DUPLICATE KEY UPDATE saldo = saldo + ?",
        [recarga.transportador_id, recarga.valor, recarga.valor]
      );
      await conn.commit();
      conn.release();

      const [driver] = await q("SELECT nome, numero FROM transportadores WHERE id = ?", [recarga.transportador_id]);
      if (driver && driver.numero) {
        enviarWhatsApp(driver.numero, `Olá ${driver.nome}! A sua recarga de ${Number(recarga.valor).toLocaleString('pt-AO')} Kz na Karrega foi aprovada. O seu saldo já está disponível na carteira.`);
      }

      res.json({ ok: true, message: "Recarga aprovada com sucesso" });
    } catch (e) {
      try { await conn.rollback(); } catch (_) {}
      conn.release();
      res.status(500).json({ error: e.message });
    }
  });

  r.post("/recargas/:id/recusar", async (req, res) => {
    try {
      const { motivo } = req.body || {};
      const [recarga] = await q("SELECT * FROM carteira_recargas WHERE id = ?", [req.params.id]);
      if (!recarga) return res.status(404).json({ error: "Recarga não encontrada" });

      await q("UPDATE carteira_recargas SET status = 'recusada', observacao = ?, updated_at = NOW() WHERE id = ?", [motivo || 'Comprovativo não validado', req.params.id]);

      const [driver] = await q("SELECT nome, numero FROM transportadores WHERE id = ?", [recarga.transportador_id]);
      if (driver && driver.numero) {
        enviarWhatsApp(driver.numero, `Olá ${driver.nome}. O seu comprovativo de recarga de ${Number(recarga.valor).toLocaleString('pt-AO')} Kz não foi validado: ${motivo || 'Comprovativo ilegível ou não localizado'}.`);
      }

      res.json({ ok: true, message: "Recarga recusada" });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Série temporal: g = dia | semana | mes | ano
  const GRUPOS = {
    dia: ["DATE_FORMAT(created_at,'%Y-%m-%d')", "30 DAY"],
    semana: ["DATE_FORMAT(DATE_SUB(created_at, INTERVAL WEEKDAY(created_at) DAY),'%Y-%m-%d')", "84 DAY"],
    mes: ["DATE_FORMAT(created_at,'%Y-%m')", "12 MONTH"],
    ano: ["DATE_FORMAT(created_at,'%Y')", "5 YEAR"],
  };
  r.get("/series", async (req, res) => {
    try {
      const [k, intv] = GRUPOS[req.query.g] || GRUPOS.dia;
      const a = await q(`SELECT ${k} k, COUNT(*) pedidos, COUNT(DISTINCT cliente_id) demanda FROM viagens
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL ${intv}) GROUP BY k ORDER BY k`);
      const b = await q(`SELECT ${k} k, SUM(valor_pago_cliente) movimentado, SUM(valor_app) receita FROM incomes
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL ${intv}) GROUP BY k ORDER BY k`);
      const map = {};
      a.forEach((x) => (map[x.k] = { k: x.k, pedidos: x.pedidos, demanda: x.demanda, movimentado: 0, receita: 0 }));
      b.forEach((x) => { map[x.k] = { k: x.k, pedidos: 0, demanda: 0, ...map[x.k], movimentado: Number(x.movimentado), receita: Number(x.receita) }; });
      res.json(Object.values(map).sort((x, y) => x.k.localeCompare(y.k)));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/clientes", async (_, res) => {
    try {
      res.json(await q(`SELECT c.id, c.nome, c.numero, c.created_at, COALESCE(c.estado,'ativo') estado,
        COUNT(v.id) pedidos, MAX(v.created_at) ultimo_pedido
        FROM clientes c LEFT JOIN viagens v ON v.cliente_id=c.id GROUP BY c.id ORDER BY c.nome`));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/clientes/:id", async (req, res) => {
    try {
      const [c] = await q("SELECT id, nome, numero, numero_bi, created_at, COALESCE(estado,'ativo') estado FROM clientes WHERE id=?", [req.params.id]);
      if (!c) return res.status(404).json({ error: "Cliente não encontrado" });
      const pedidos = await q(`SELECT v.id, v.status, v.origin, v.destination, v.valor, v.created_at
        FROM viagens v WHERE v.cliente_id=? ORDER BY v.created_at DESC`, [c.id]);
      const [s] = await q("SELECT COALESCE(SUM(i.valor_pago_cliente),0) mov FROM incomes i JOIN viagens v ON v.id=i.viagem_id WHERE v.cliente_id=?", [c.id]);
      res.json({ ...c, pedidos, movimentado: Number(s.mov) });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/cargueiros", async (_, res) => {
    try {
      res.json(await q(`SELECT t.id, t.nome, t.numero, t.tipo_transporte, t.created_at,
        COALESCE(t.estado,'ativo') estado, COALESCE(t.verificado,'pendente') verificado,
        COUNT(v.id) pedidos, SUM(v.status='feito') concluidos, SUM(v.status='cancelado') cancelados
        FROM transportadores t LEFT JOIN viagens v ON v.transportador_id=t.id GROUP BY t.id ORDER BY t.nome`));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/cargueiros/:id", async (req, res) => {
    try {
      const [t] = await q(`SELECT id, nome, numero, numero_bi, tipo_transporte, foto_bi_path, created_at,
        foto_perfil_url, data_nascimento, foto_bi_verso_url, numero_carta, foto_carta_url,
        foto_veiculo_url, veiculo_marca, veiculo_modelo, veiculo_ano, veiculo_cor, veiculo_placa,
        COALESCE(estado,'ativo') estado, COALESCE(verificado,'pendente') verificado FROM transportadores WHERE id=?`, [req.params.id]);
      if (!t) return res.status(404).json({ error: "Cargueiro não encontrado" });

      let docs = {
        foto_perfil_url: t.foto_perfil_url || null,
        data_nascimento: t.data_nascimento || null,
        foto_bi_frente_url: t.foto_bi_path || null,
        foto_bi_verso_url: t.foto_bi_verso_url || null,
        numero_carta: t.numero_carta || null,
        foto_carta_url: t.foto_carta_url || null,
      };

      let veiculo = {
        tipo: t.tipo_transporte,
        marca: t.veiculo_marca || null,
        modelo: t.veiculo_modelo || null,
        ano_fabricacao: t.veiculo_ano || null,
        cor: t.veiculo_cor || null,
        numero_placa: t.veiculo_placa || null,
        foto_veiculo_url: t.foto_veiculo_url || null,
      };

      if (pgPool) {
        try {
          const pgRes = await pgPool.query("SELECT * FROM cargueiros WHERE id = $1 OR numero = $2 LIMIT 1", [t.id, t.numero]);
          if (pgRes.rows.length) {
            const pgc = pgRes.rows[0];
            docs.foto_perfil_url = pgc.foto_perfil_url || docs.foto_perfil_url;
            docs.data_nascimento = pgc.data_nascimento || docs.data_nascimento;
            docs.foto_bi_frente_url = pgc.foto_bi_frente_url || docs.foto_bi_frente_url;
            docs.foto_bi_verso_url = pgc.foto_bi_verso_url || docs.foto_bi_verso_url;
            docs.numero_carta = pgc.numero_carta || docs.numero_carta;
            docs.foto_carta_url = pgc.foto_carta_url || docs.foto_carta_url;

            const pgvRes = await pgPool.query("SELECT * FROM veiculos WHERE cargueiro_id = $1 LIMIT 1", [pgc.id]);
            if (pgvRes.rows.length) {
              const pgv = pgvRes.rows[0];
              veiculo = {
                tipo: pgv.tipo || veiculo.tipo,
                marca: pgv.marca || veiculo.marca,
                modelo: pgv.modelo || veiculo.modelo,
                ano_fabricacao: pgv.ano_fabricacao || veiculo.ano_fabricacao,
                cor: pgv.cor || veiculo.cor,
                numero_placa: pgv.numero_placa || veiculo.numero_placa,
                foto_veiculo_url: pgv.foto_veiculo_url || veiculo.foto_veiculo_url,
              };
            }
          }
        } catch (_) {}
      }

      const pedidos = await q(`SELECT v.id, v.status, v.origin, v.destination, v.valor, v.created_at
        FROM viagens v WHERE v.transportador_id=? ORDER BY v.created_at DESC`, [t.id]);
      const [s] = await q("SELECT COALESCE(SUM(i.valor_transportador),0) rend FROM incomes i JOIN viagens v ON v.id=i.viagem_id WHERE v.transportador_id=?", [t.id]);
      const [w] = await q("SELECT COALESCE(saldo, 0.00) saldo FROM carteira_cargueiro WHERE transportador_id=?", [t.id]);
      const avaliacoes = await q(`SELECT a.id, a.pontualidade, a.comunicacao, a.cuidado_carga, a.limpeza_transporte,
        a.classificacao_geral, a.comentario, a.created_at, c.nome cliente_nome
        FROM avaliacoes a
        JOIN viagens v ON v.id = a.viagem_id
        JOIN clientes c ON c.id = a.avaliador_id
        WHERE a.avaliado_id = ? AND a.avaliador_tipo = 'cliente'
        ORDER BY a.created_at DESC LIMIT 50`, [t.id]);
      const [media] = await q("SELECT COALESCE(ROUND(AVG(classificacao_geral), 1), 0) media, COUNT(*) total FROM avaliacoes WHERE avaliado_id = ? AND avaliador_tipo = 'cliente'", [t.id]);
      res.json({
        ...t,
        documentos: docs,
        veiculo,
        pedidos,
        rendimento: Number(s.rend),
        saldo: Number(w?.saldo || 0),
        avaliacoes: avaliacoes || [],
        media_estrelas: Number(media?.media || 0),
        total_avaliacoes: Number(media?.total || 0),
      });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  const ESTADOS = ["ativo", "suspenso", "banido"];
  const tabela = { clientes: "clientes", cargueiros: "transportadores" };
  const notificar = (row, ev) => row && enviarWhatsApp(row.numero, mensagem(ev, row.nome));

  r.post("/:tipo(clientes|cargueiros)/:id/estado", async (req, res) => {
    const { estado } = req.body;
    if (!ESTADOS.includes(estado)) return res.status(400).json({ error: "Estado inválido" });
    const T = tabela[req.params.tipo];
    const [row] = await q(`SELECT nome, numero FROM ${T} WHERE id=?`, [req.params.id]);
    await q(`UPDATE ${T} SET estado=? WHERE id=?`, [estado, req.params.id]);
    await notificar(row, estado);
    res.json({ ok: true });
  });

  r.delete("/:tipo(clientes|cargueiros)/:id", async (req, res) => {
    try {
      const T = tabela[req.params.tipo];
      const [row] = await q(`SELECT nome, numero FROM ${T} WHERE id=?`, [req.params.id]);
      await q(`DELETE FROM ${T} WHERE id=?`, [req.params.id]);
      await notificar(row, "eliminado");
      res.json({ ok: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.post("/cargueiros/:id/verificar", async (req, res) => {
    try {
      const [row] = await q("SELECT nome, numero FROM transportadores WHERE id=?", [req.params.id]);
      const statusVerif = req.body.aprovar ? "verificado" : "rejeitado";
      await q("UPDATE transportadores SET verificado=? WHERE id=?", [statusVerif, req.params.id]);
      if (pgPool) {
        try {
          await pgPool.query("UPDATE cargueiros SET verificado = $1 WHERE id = $2 OR numero = $3", [statusVerif, req.params.id, row?.numero]);
        } catch (_) {}
      }
      await notificar(row, req.body.aprovar ? "aprovado" : "rejeitado");
      res.json({ ok: true, status: statusVerif });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  r.get("/pedidos", async (_, res) => {
    try {
      res.json(await q(`SELECT v.id, v.status, v.tipo, v.origin, v.destination, v.valor, v.created_at,
        c.nome cliente, t.nome cargueiro FROM viagens v
        LEFT JOIN clientes c ON c.id=v.cliente_id LEFT JOIN transportadores t ON t.id=v.transportador_id
        ORDER BY v.created_at DESC LIMIT 500`));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/pedidos/:id", async (req, res) => {
    try {
      const [p] = await q(`SELECT v.*, c.nome cliente, c.numero cliente_numero, t.nome cargueiro, t.numero cargueiro_numero,
        t.tipo_transporte, i.valor_pago_cliente, i.valor_app, i.valor_transportador
        FROM viagens v LEFT JOIN clientes c ON c.id=v.cliente_id LEFT JOIN transportadores t ON t.id=v.transportador_id
        LEFT JOIN incomes i ON i.viagem_id=v.id WHERE v.id=? LIMIT 1`, [req.params.id]);
      if (!p) return res.status(404).json({ error: "Pedido não encontrado" });
      const historico = await q(`SELECT status_anterior, status_novo, actor_tipo, actor_id, observacao, created_at
        FROM viagem_historico WHERE viagem_id=? ORDER BY created_at DESC LIMIT 100`, [req.params.id]);
      const avaliacoes = await q(`SELECT avaliador_tipo, avaliador_id, avaliado_id, pontualidade, comunicacao,
        cuidado_carga, limpeza_transporte, cumprimento_acordo, clareza_informacoes, classificacao_geral,
        comentario, created_at FROM avaliacoes WHERE viagem_id=? ORDER BY created_at DESC`, [req.params.id]);
      res.json({ ...p, historico, avaliacoes });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  r.get("/financeiro", async (_, res) => {
    try {
      res.json(await q(`SELECT i.id, i.viagem_id, i.valor_pago_cliente, i.valor_app, i.valor_transportador, i.created_at,
        c.nome cliente, t.nome cargueiro FROM incomes i
        LEFT JOIN viagens v ON v.id=i.viagem_id LEFT JOIN clientes c ON c.id=v.cliente_id
        LEFT JOIN transportadores t ON t.id=v.transportador_id ORDER BY i.created_at DESC LIMIT 500`));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  return r;
}
