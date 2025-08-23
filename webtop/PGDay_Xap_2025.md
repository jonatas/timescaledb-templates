# PGDay Xap 2025

## Análise de Tráfego Web em Tempo Real com TimescaleDB

**Jônatas Davi Paganini** - https://ideia.me

---

## 1. O Problema: 500GB de Dados Diários

### Cenário
- **500GB diários** de logs de tráfego web
- **Milhões de requisições** por minuto
- **Custos de armazenamento** crescem exponencialmente
- **Degradação de performance** ao longo do tempo

### Objetivo
Todo o sistema deve caber em **10GB**.

### Solução
Pipeline TimescaleDB que:
- **Agrega dados** em tempo real
- **Mantém apenas estatísticas** ao invés de logs brutos
- **Identifica top websites** automaticamente
- **Escala infinitamente** com armazenamento controlado

---

## 2. Como o TimescaleDB Resolve

### Extensões Necessárias
```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;
```

### Estratégia de Armazenamento
- **Nível 1:** Logs brutos (1 hora) → **Descartados**
- **Nível 2:** Agregados (7-90 dias) → **Comprimidos e Mantidos**
- **Nível 3:** Top websites (permanente) → **Armazenamento Permanente**

### Resultado
Sistema que processa dados ilimitados mantendo custos de armazenamento constantes.

---

## 3. DDL: Estrutura do Sistema

### Tabela Base de Logs
```sql
CREATE TABLE IF NOT EXISTS "logs" (
    "time" timestamp with time zone NOT NULL,
    "domain" text NOT NULL
);

SELECT create_hypertable('logs', 'time', 
    chunk_time_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

SELECT add_retention_policy('logs', INTERVAL '1 hour', 
    schedule_interval => INTERVAL '20 minutes');
```

### Agregados Contínuos Hierárquicos
```sql
-- 1-minuto (nível base)
CREATE MATERIALIZED VIEW website_stats_1m
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
    SELECT 
        time_bucket('1 minute', time) as bucket,
        domain,
        count(*) as total
    FROM logs
    GROUP BY 1,2
WITH NO DATA;

-- 1-hora
CREATE MATERIALIZED VIEW website_stats_1h
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
    SELECT 
        time_bucket('1 hour', bucket) as bucket,
        domain,
        sum(total) as total,
        stats_agg(total) as stats_agg,
        percentile_agg(total) as percentile_agg
    FROM website_stats_1m
    GROUP BY 1,2
WITH NO DATA;

-- 1-dia
CREATE MATERIALIZED VIEW website_stats_1d
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
    SELECT 
        time_bucket('1 day', bucket) as bucket,
        domain,
        sum(total) as total,
        stats_agg(total) as stats_agg,
        percentile_agg(total) as percentile_agg
    FROM website_stats_1h
    GROUP BY 1,2
WITH NO DATA;
```

### Políticas de Refresh
```sql
-- 1-minuto: refresh a cada 30 segundos
SELECT add_continuous_aggregate_policy('website_stats_1m',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds');

-- 1-hora: refresh a cada 5 minutos
SELECT add_continuous_aggregate_policy('website_stats_1h',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

-- 1-dia: refresh a cada 1 hora
SELECT add_continuous_aggregate_policy('website_stats_1d',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

---

## 4. Sistema de Eleição: Configuração

### Tabela de Configuração
```sql
CREATE TABLE IF NOT EXISTS "top_websites_config" (
    "id" serial PRIMARY KEY,
    "min_hits_threshold" integer NOT NULL DEFAULT 100,
    "election_window" interval NOT NULL DEFAULT '2 hours',
    "longterm_retention_period" interval NOT NULL DEFAULT '90 days',
    "candidates_retention_period" interval NOT NULL DEFAULT '7 days',
    "max_longterm_entries" integer NOT NULL DEFAULT 1000,
    "election_schedule_interval" interval NOT NULL DEFAULT '1 hour',
    "created_at" timestamp with time zone NOT NULL DEFAULT now(),
    "updated_at" timestamp with time zone NOT NULL DEFAULT now()
);
```

### Configurando Limites

```sql
INSERT INTO "top_websites_config" 
    ("min_hits_threshold",
     "election_window",
     "longterm_retention_period", 
     "candidates_retention_period",
      "max_longterm_entries",
      "election_schedule_interval")
SELECT 100, '2 hours', '90 days', '7 days', 1000, '1 hour'
WHERE NOT EXISTS (SELECT 1 FROM top_websites_config);
```

### Tabela de Longo Prazo

```sql
CREATE TABLE IF NOT EXISTS "top_websites_longterm" (
    "domain" text NOT NULL,
    "first_seen" timestamp with time zone NOT NULL,
    "last_seen" timestamp with time zone NOT NULL,
    "total_hits" bigint NOT NULL DEFAULT 0,
    "last_election_hits" bigint NOT NULL DEFAULT 0,
    "times_elected" integer NOT NULL DEFAULT 1,
    PRIMARY KEY ("domain")
) WITH (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.025,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_threshold = 50
);
```

### Tabela de candidatos

```sql
CREATE TABLE IF NOT EXISTS "top_websites_candidates" (
    "domain" text NOT NULL,
    "time" timestamp with time zone NOT NULL,
    "hits" bigint NOT NULL,
    "created_at" timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY ("domain", "time")
);

SELECT create_hypertable('top_websites_candidates', 'time', if_not_exists => TRUE);
```

---

## 5. Query de Eleição: População de Candidatos

### Procedimento de População
```sql
CREATE OR REPLACE PROCEDURE populate_website_candidates_job(job_id INTEGER, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
    min_hits INTEGER;
    v_election_window INTERVAL;
    candidate_count INTEGER;
BEGIN
    -- Obter configuração
    SELECT min_hits_threshold, election_window
    INTO min_hits, v_election_window
    FROM top_websites_config
    LIMIT 1;

    -- Popular candidatos do website_stats_1m
    INSERT INTO top_websites_candidates (domain, time, hits)
    SELECT 
        domain,
        MAX(bucket) AS time,
        SUM(total) AS hits
    FROM website_stats_1m
    WHERE bucket >= NOW() - v_election_window
    GROUP BY domain
    HAVING SUM(total) >= min_hits
    ON CONFLICT (domain, time) DO UPDATE SET hits = EXCLUDED.hits;

    GET DIAGNOSTICS candidate_count = ROW_COUNT;
    RAISE NOTICE 'Populated % website candidates.', candidate_count;
END;
$$;
```

---

## 6. Upsert de Candidatos: Sistema de Eleição

### Procedimento de Eleição com Upsert
```sql
CREATE OR REPLACE PROCEDURE elect_top_websites(job_id INTEGER, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
    max_entries INTEGER;
    retention_period INTERVAL;
BEGIN
    -- Obter configuração
    SELECT max_longterm_entries, longterm_retention_period 
    INTO max_entries, retention_period
    FROM top_websites_config 
    LIMIT 1;
    
    -- Upsert usando ON CONFLICT
    INSERT INTO top_websites_longterm (
        domain,
        first_seen,
        last_seen,
        total_hits,
        last_election_hits,
        times_elected
    )
    SELECT 
        domain,
        time AS first_seen,
        time AS last_seen,
        hits AS total_hits,
        hits AS last_election_hits,
        1 AS times_elected
    FROM top_websites_candidates tc
    WHERE tc.time = (SELECT MAX(time) FROM top_websites_candidates WHERE domain = tc.domain)
    ON CONFLICT (domain) DO UPDATE SET
        last_seen = EXCLUDED.last_seen,
        total_hits = top_websites_longterm.total_hits + EXCLUDED.total_hits,
        last_election_hits = EXCLUDED.last_election_hits,
        times_elected = top_websites_longterm.times_elected + 1;

    -- limite de entradas
    DELETE FROM top_websites_longterm
    WHERE domain IN (
        SELECT domain
        FROM top_websites_longterm
        ORDER BY last_seen ASC
        OFFSET max_entries
    );

    -- Limpa candidatos antigos
    DELETE FROM top_websites_candidates
    WHERE time < now() - retention_period;

    RAISE NOTICE 'Election complete. % websites in long-term storage.', 
        (SELECT COUNT(*) FROM top_websites_longterm);
END;
$$;
```

---

## 7. Inserção de Longo Prazo: View de Relatório

### View de Relatório
```sql
CREATE OR REPLACE VIEW top_websites_report AS
SELECT 
    domain,
    total_hits,
    last_election_hits,
    times_elected,
    first_seen,
    last_seen,
    CASE 
        WHEN last_seen = first_seen THEN 0
        ELSE ROUND((total_hits::numeric / EXTRACT(EPOCH FROM (last_seen - first_seen)) * 3600), 2)
    END AS avg_hourly_requests
FROM top_websites_longterm
ORDER BY total_hits DESC;
```

---

## 8. Jobs em Background: Automação

### Agendamento de Jobs
```sql
CREATE OR REPLACE PROCEDURE schedule_candidates_election()
LANGUAGE plpgsql AS $$
DECLARE
    election_schedule INTERVAL;
    config_record RECORD;
BEGIN
    -- Obter configuração para agendamento
    SELECT * INTO config_record FROM top_websites_config LIMIT 1;

    -- Job de população de candidatos (a cada 6 minutos)
    PERFORM add_job('populate_website_candidates_job',
        (config_record.election_schedule_interval / 10)::interval,
        initial_start => NOW() + INTERVAL '10 seconds'
    );

    -- Job de eleição (a cada hora)
    PERFORM add_job('elect_top_websites',
        config_record.election_schedule_interval,
        initial_start => NOW() + INTERVAL '1 minute'
    );
END;
$$;
```

### Execução dos Jobs
```sql
-- Inicializar jobs
CALL schedule_candidates_election();

-- Verificar status dos jobs
SELECT job_id, proc_name, schedule_interval, next_start 
FROM timescaledb_information.jobs 
WHERE proc_name IN ('populate_website_candidates_job', 'elect_top_websites')
ORDER BY job_id;
```

---

## 9. Resultados: Sistema em Ação

### Verificação do Sistema
```sql
-- Status atual
SELECT COUNT(*) as total_websites FROM top_websites_longterm;

-- Top performers
SELECT domain, total_hits, times_elected, avg_hourly_requests 
FROM top_websites_report 
ORDER BY total_hits DESC 
LIMIT 10;

-- Candidatos atuais
SELECT COUNT(*) as candidates FROM top_websites_candidates;
```

### Performance do Sistema
- **Logs brutos:** 1GB/hora → 24GB/dia
- **Dados agregados:** 10MB/hora → 240MB/dia
- **Taxa de compressão:** 99.6% de redução
- **Queries em tempo real:** <5ms para agregados

---

## 10. Monitoramento e Troubleshooting

### Verificação de Jobs
```sql
SELECT proc_name, schedule_interval, next_start - now() as next_start, scheduled
FROM timescaledb_information.jobs
WHERE proc_name IN ('populate_website_candidates_job', 'elect_top_websites');
```

### Verificação de Erros
```sql
-- Erros recentes
SELECT 
    job_id,
    proc_name,
    start_time,
    sqlerrcode,
    err_message
FROM timescaledb_information.job_errors
WHERE start_time > NOW() - INTERVAL '1 hour'
ORDER BY start_time DESC;
```

### Verificação de Agregados
```sql
-- Lag dos agregados contínuos
SELECT 
    view_name,
    last_run_started_at,
    last_successful_finish,
    EXTRACT(EPOCH FROM (now() - last_successful_finish)) as lag_seconds
FROM timescaledb_information.jobs
WHERE proc_name LIKE '%continuous_aggregate%';
```

---

## Conclusão

### Benefícios Alcançados
1. **Redução de 99.6%** no armazenamento
2. **Análise em tempo real** com latência <5ms
3. **Identificação automática** de top websites
4. **Escalabilidade infinita** com custos controlados
5. **Sistema totalmente automatizado** com jobs em background

### Tecnologias Utilizadas
- **TimescaleDB:** Agregados contínuos e hypertables
- **PostgreSQL:** ACID compliance e SQL completo
- **Background Jobs:** Automação de processos
- **Upsert:** Operações atômicas sem race conditions

### Resultado Final
Sistema que processa **500GB diários** mantendo apenas **10GB** de dados essenciais, com identificação automática dos websites mais relevantes.

---

## Perguntas?

**Contato:** https://ideia.me  
**Código:** https://github.com/jonatas/timescaledb-webtop

---

## 5. Investigação: Política de Retenção e Crescimento de Dados

### O Problema Identificado

**Cenário:** Sistema atingiu 11GB apesar da política de retenção de 1 hora configurada.

**Causa Raiz:** 16 jobs simultâneos gerando dados massivos:
- **2.6 milhões de registros por minuto**
- **Chunks de 1 minuto com 216MB cada**
- **60 chunks = 13GB de dados**

### Jobs Identificados

```sql
-- Jobs gerando dados excessivos
SELECT job_id, proc_name, schedule_interval, config 
FROM timescaledb_information.jobs 
WHERE proc_name = 'generate_log_data_job';

-- Resultado: 16 jobs ativos
-- - 5 jobs: ~5k registros a cada 10s
-- - 1 job: ~11k registros a cada 60s  
-- - 1 job: ~10k registros a cada 30s
-- - 1 job: ~12k registros a cada 15s
-- - 5 jobs: ~55k registros a cada 10s
-- - 1 job: ~110k registros a cada 60s
-- - 1 job: ~100k registros a cada 30s
-- - 1 job: ~101k registros a cada 15s
```

### Verificação da Política de Retenção

```sql
-- Status da política de retenção
SELECT * FROM timescaledb_information.jobs 
WHERE proc_name = 'policy_retention';

-- Resultado: Ativa e funcionando
-- job_id: 1014
-- schedule_interval: 20 minutos
-- drop_after: 1 hora
```

### Análise dos Chunks

```sql
-- Chunks existentes
SELECT COUNT(*) as total_chunks, 
       MIN(range_start) as oldest_chunk, 
       MAX(range_end) as newest_chunk 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'logs';

-- Resultado: 60 chunks (17:59 - 19:00)
-- Dentro da janela de 1 hora configurada
```

### Solução Implementada

**1. Parada dos Jobs Excessivos:**
```sql
-- Remoção de todos os jobs de geração de dados
SELECT delete_job(job_id) 
FROM timescaledb_information.jobs 
WHERE proc_name = 'generate_log_data_job';
```

**2. Verificação da Política:**
```sql
-- Execução manual da política de retenção
CALL run_job(1014);

-- Resultado: Chunks antigos removidos corretamente
-- De 67 chunks → 60 chunks (exatamente 1 hora)
```

### Conclusões

✅ **Política de Retenção Funcionando:**
- Mantém exatamente 1 hora de dados
- Remove chunks automaticamente a cada 20 minutos
- Configuração correta: `INTERVAL '1 hour'`

✅ **Sistema Escalável:**
- 60 chunks × 216MB = ~13GB para 1 hora de dados
- Crescimento controlado e previsível
- Retenção automática funcionando

❌ **Problema: Geração Excessiva de Dados:**
- Jobs duplicados e mal configurados
- 2.6M registros/minuto vs. expectativa de ~3k/minuto
- Necessário controle de jobs ativos

### Recomendações

1. **Monitoramento de Jobs:** Verificar jobs ativos regularmente
2. **Controle de Taxa:** Limitar geração de dados por job
3. **Alertas:** Configurar alertas para crescimento inesperado
4. **Documentação:** Manter registro de jobs de teste vs. produção

---

## 6. Monitoramento e Alertas

### Query de Monitoramento

```sql
-- Monitoramento de crescimento
SELECT 
    pg_size_pretty(pg_database_size('pipeline')) as db_size,
    COUNT(*) as total_chunks,
    MIN(range_start) as oldest_data,
    MAX(range_end) as newest_data,
    NOW() - MIN(range_start) as data_age
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'logs';
```

### Alertas Recomendados

- **Tamanho > 15GB:** Possível vazamento de dados
- **Chunks > 70:** Verificar política de retenção
- **Idade > 2 horas:** Falha na política de retenção
- **Jobs > 5:** Possível duplicação de jobs
