ALTER TABLE viagens
  MODIFY COLUMN status ENUM(
    'rascunho',
    'aguardando_propostas',
    'em_negociacao',
    'aguardando_confirmacao',
    'confirmada',
    'a_caminho_recolha',
    'carga_recolhida',
    'em_transito',
    'aguardando_confirmacao_entrega',
    'feito',
    'cancelado',
    'expirada',
    'em_disputa',
    'pendente',
    'aceito'
  ) NOT NULL DEFAULT 'aguardando_propostas';

ALTER TABLE propostas
  MODIFY COLUMN status ENUM('pendente', 'selecionada', 'aceito', 'recusado') NOT NULL DEFAULT 'pendente';

ALTER TABLE viagens ADD COLUMN preco_final DECIMAL(12,2) NULL;
ALTER TABLE viagens ADD COLUMN confirmacao_limite_at DATETIME NULL;
ALTER TABLE viagens ADD COLUMN entrega_registada_at DATETIME NULL;
ALTER TABLE viagens ADD COLUMN cliente_confirmou_entrega_at DATETIME NULL;
ALTER TABLE viagens ADD COLUMN atraso_recolha_confirmado BOOLEAN NULL;
ALTER TABLE viagens ADD COLUMN limpeza_transporte ENUM('limpo','moderado','sujo') NULL;
ALTER TABLE viagens ADD COLUMN cancel_reason VARCHAR(255) NULL;

CREATE TABLE IF NOT EXISTS configuracoes_operacionais (
  id INT PRIMARY KEY DEFAULT 1,
  antecedencia_minima_horas INT NOT NULL DEFAULT 72,
  prazo_confirmacao_horas INT NOT NULL DEFAULT 24,
  cancelamento_automatico BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO configuracoes_operacionais (id, antecedencia_minima_horas, prazo_confirmacao_horas, cancelamento_automatico)
VALUES (1, 72, 24, TRUE)
ON DUPLICATE KEY UPDATE id = id;

CREATE TABLE IF NOT EXISTS viagem_historico (
  id INT AUTO_INCREMENT PRIMARY KEY,
  viagem_id INT NOT NULL,
  status_anterior VARCHAR(40) NULL,
  status_novo VARCHAR(40) NOT NULL,
  actor_tipo ENUM('cliente','transportador','sistema','admin') NOT NULL DEFAULT 'sistema',
  actor_id INT NULL,
  observacao VARCHAR(255) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_historico_viagem (viagem_id),
  CONSTRAINT fk_historico_viagem FOREIGN KEY (viagem_id) REFERENCES viagens(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS avaliacoes (
  id INT AUTO_INCREMENT PRIMARY KEY,
  viagem_id INT NOT NULL,
  avaliador_tipo ENUM('cliente','transportador') NOT NULL,
  avaliador_id INT NOT NULL,
  avaliado_id INT NOT NULL,
  pontualidade TINYINT NULL,
  comunicacao TINYINT NULL,
  cuidado_carga TINYINT NULL,
  limpeza_transporte ENUM('limpo','moderado','sujo') NULL,
  cumprimento_acordo TINYINT NULL,
  clareza_informacoes TINYINT NULL,
  classificacao_geral TINYINT NOT NULL,
  comentario TEXT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_avaliacao_viagem_tipo (viagem_id, avaliador_tipo),
  CONSTRAINT fk_avaliacoes_viagem FOREIGN KEY (viagem_id) REFERENCES viagens(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
