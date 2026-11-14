ALTER TABLE configuracoes_operacionais
  ADD COLUMN taxa_karrega_percentual DECIMAL(5,2) NOT NULL DEFAULT 20.00
  AFTER cancelamento_automatico;