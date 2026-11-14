-- Inicialização do esquema Karrega
CREATE TABLE IF NOT EXISTS clientes (
	id INT AUTO_INCREMENT PRIMARY KEY,
	nome VARCHAR(200) NOT NULL,
	numero VARCHAR(50) NOT NULL UNIQUE,
	numero_bi VARCHAR(100) NOT NULL,
	password VARCHAR(255) NULL,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transportadores (
	id INT AUTO_INCREMENT PRIMARY KEY,
	nome VARCHAR(200) NOT NULL,
	numero VARCHAR(50) NOT NULL UNIQUE,
	numero_bi VARCHAR(100) NOT NULL,
	password VARCHAR(255) NULL,
	tipo_transporte VARCHAR(32) DEFAULT 'ligeiro',
	foto_bi_path MEDIUMTEXT,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS taxas (
	id INT AUTO_INCREMENT PRIMARY KEY,
	nome VARCHAR(200) NOT NULL,
	valor DECIMAL(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS valor_inicial (
	id INT AUTO_INCREMENT PRIMARY KEY,
	valor DECIMAL(12,2) NOT NULL,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS planos (
	id INT AUTO_INCREMENT PRIMARY KEY,
	nome VARCHAR(200) NOT NULL,
	descricao TEXT,
	valor DECIMAL(12,2) NOT NULL
);

-- Inserir taxas e planos iniciais (opcionais)
INSERT INTO taxas (nome, valor) VALUES
	('Taxa padrão', 5.00)
ON DUPLICATE KEY UPDATE nome=nome;

INSERT INTO planos (nome, descricao, valor) VALUES
	('Básico', 'Pacote básico para iniciar', 0.00),
	('Profissional', 'Pacote com prioridade e mais visibilidade', 29.90)
ON DUPLICATE KEY UPDATE nome=nome;

INSERT INTO valor_inicial (id, valor) VALUES (1, 5000.00)
ON DUPLICATE KEY UPDATE valor = VALUES(valor);

-- Adicionar coluna `password` para clientes e transportadores (se não existir)
-- ALTER TABLE clientes ADD COLUMN IF NOT EXISTS `password` VARCHAR(255) NULL;
-- ALTER TABLE transportadores ADD COLUMN IF NOT EXISTS `password` VARCHAR(255) NULL;

CREATE TABLE IF NOT EXISTS viagens (
	id INT AUTO_INCREMENT PRIMARY KEY,
	cliente_id INT NOT NULL,
	transportador_id INT NULL,
	status ENUM('rascunho','aguardando_propostas','em_negociacao','aguardando_confirmacao','confirmada','a_caminho_recolha','carga_recolhida','em_transito','aguardando_confirmacao_entrega','feito','cancelado','expirada','em_disputa','pendente','aceito') NOT NULL DEFAULT 'aguardando_propostas',
	tipo ENUM('agendado','na hora') NOT NULL,
	scheduled_at DATETIME NULL,
	started_at DATETIME NULL,
	ended_at DATETIME NULL,
	preco_final DECIMAL(12,2) NULL,
	confirmacao_limite_at DATETIME NULL,
	entrega_registada_at DATETIME NULL,
	cliente_confirmou_entrega_at DATETIME NULL,
	atraso_recolha_confirmado BOOLEAN NULL,
	limpeza_transporte ENUM('limpo','moderado','sujo') NULL,
	cancel_reason VARCHAR(255) NULL,
	pin_entrega VARCHAR(4) NULL,
	origin VARCHAR(255),
	destination VARCHAR(255),
	tamanho_carga VARCHAR(64),
	descricao_da_carga TEXT,
	tipo_de_transporte VARCHAR(32) DEFAULT 'ligeiro',
	valor DECIMAL(12,2) DEFAULT 5000.00,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_cliente (cliente_id),
	INDEX idx_transportador (transportador_id),
	CONSTRAINT fk_viagens_cliente FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_viagens_transportador FOREIGN KEY (transportador_id) REFERENCES transportadores(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



-- If older migrations added tipo_carro, drop it here (we use transportadores.tipo_transporte instead)

-- Tabela que registra a distribuição financeira após cada viagem
CREATE TABLE IF NOT EXISTS incomes (
	id INT AUTO_INCREMENT PRIMARY KEY,
	viagem_id INT NOT NULL,
	taxa VARCHAR(200) DEFAULT NULL,
	valor_pago_cliente DECIMAL(12,2) NOT NULL DEFAULT 0.00,
	valor_transportador DECIMAL(12,2) NOT NULL DEFAULT 0.00,
	valor_app DECIMAL(12,2) NOT NULL DEFAULT 0.00,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_viagem (viagem_id),
	CONSTRAINT fk_income_viagem FOREIGN KEY (viagem_id) REFERENCES viagens(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela que registra propostas de preço feitas por transportadores
CREATE TABLE IF NOT EXISTS propostas (
	id INT AUTO_INCREMENT PRIMARY KEY,
	viagem_id INT NOT NULL,
	transportador_id INT NOT NULL,
	valor_proposto DECIMAL(12,2) NOT NULL,
	status ENUM('pendente', 'selecionada', 'aceito', 'recusado') NOT NULL DEFAULT 'pendente',
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	INDEX idx_viagem_proposta (viagem_id),
	CONSTRAINT fk_propostas_viagem FOREIGN KEY (viagem_id) REFERENCES viagens(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT fk_propostas_transportador FOREIGN KEY (transportador_id) REFERENCES transportadores(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS configuracoes_operacionais (
	id INT PRIMARY KEY DEFAULT 1,
	antecedencia_minima_horas INT NOT NULL DEFAULT 72,
	prazo_confirmacao_horas INT NOT NULL DEFAULT 24,
	cancelamento_automatico BOOLEAN NOT NULL DEFAULT TRUE,
	taxa_karrega_percentual DECIMAL(5,2) NOT NULL DEFAULT 0.00,
	bonus_boas_vindas DECIMAL(12,2) NOT NULL DEFAULT 0.00,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO configuracoes_operacionais (id, antecedencia_minima_horas, prazo_confirmacao_horas, cancelamento_automatico, taxa_karrega_percentual, bonus_boas_vindas)
VALUES (1, 72, 24, TRUE, 0.00, 0.00)
ON DUPLICATE KEY UPDATE id = id;

-- Carteira pré-paga para transportadores
CREATE TABLE IF NOT EXISTS carteira_cargueiro (
	transportador_id INT PRIMARY KEY,
	saldo DECIMAL(12,2) NOT NULL DEFAULT 0.00,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	CONSTRAINT fk_carteira_transportador FOREIGN KEY (transportador_id) REFERENCES transportadores(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Solicitações de recarga de carteira (transferência bancária manual com comprovativo)
CREATE TABLE IF NOT EXISTS carteira_recargas (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
