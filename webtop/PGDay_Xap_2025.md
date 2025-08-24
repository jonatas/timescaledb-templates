# PGDay Xap 2025

Análise de Tráfego Web em Tempo Real com TimescaleDB

**Jônatas Davi Paganini** - https://ideia.me

> 1. O Problema: 500GB de Dados Diários

## Cenário

- **500GB diários** de logs de tráfego web
- **Milhões de requisições** por minuto
- **Custos de armazenamento** crescem exponencialmente
- **Degradação de performance** ao longo do tempo

Objetivo

> Todo o sistema deve caber em **20GB**.

## Solução

Pipeline TimescaleDB que:

- **Agrega dados** em tempo real
- **Mantém apenas estatísticas** ao invés de logs brutos
- **Identifica top websites** automaticamente
- **Escala infinitamente** com armazenamento controlado

# Extensões utilizadas

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;
```

> Toolkit apenas estatísticas avançadas.

## Estratégia de Armazenamento

- **Nível 1:** Logs brutos (20 minutos) → **Descartados**
- **Nível 2:** Agregados (7-90 dias) → **Mantidos**
- **Nível 3:** Top websites (permanente) → **Armazenamento Permanente**

## Vantagens

* Processamento de dados "limitado" apenas pelo IO.
* Custos de armazenamento praticamente constantes.
* Reciclagem constannte de dados brutos

# Tabela Gigante

```sql
CREATE TABLE IF NOT EXISTS "logs" (
    "time" timestamp with time zone NOT NULL,
    "domain" text NOT NULL
);
```

## Hypertable

```sql
SELECT create_hypertable('logs', 'time', 
    chunk_time_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);
```

## Retenção

```sql
SELECT add_retention_policy('logs', INTERVAL '5 minutes'
    schedule_interval => INTERVAL '1 minute');
```

# Agregados Contínuos

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
```

## Agregados Contínuos Hierárquicos

De minuto para hora
 
```sql
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
```

## De hora para dia

```sql
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

# Políticas de Agregação Contínua

```sql
SELECT add_continuous_aggregate_policy('website_stats_1m',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds');
```

## 1 hora: refresh a cada 5 minutos

```sql
SELECT add_continuous_aggregate_policy('website_stats_1h',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');
```

## 1 dia: refresh a cada 1 hora

```sql
SELECT add_continuous_aggregate_policy('website_stats_1d',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

# Sistema de Eleição

```sql
CREATE TABLE IF NOT EXISTS "top_websites_config" (
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

## Limitando processamento e alcance

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

## Estatísticas de Longo Prazo

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

# Configurações

```sql
...
) WITH (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.025,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_threshold = 50
);
```

## `fillfactor = 70`

Deixa 30% de espaço livre em cada página para atualizações
1. reduz fragmentação
2. melhorando performance de UPDATE

## `autovacuum_vacuum_scale_factor = 0.05`

> Dispara VACUUM quando 5% das linhas estão mortas, mantendo a tabela limpa para performance otimizada

## `autovacuum_analyze_scale_factor = 0.025`

> Atualiza estatísticas quando 2,5% das linhas mudam
> Garantindo que o planejador de consultas tenha distribuição atualizada.

## `autovacuum_vacuum_threshold = 50`

Mínimo de linhas mortas antes de disparar VACUUM

> evita overhead excessivo de manutenção.

## `autovacuum_analyze_threshold = 50`

Mínimo de mudanças de linhas antes de disparar ANALYZE, equilibrando precisão vs. performance.

# Tabela de candidatos

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

## Query de Eleição

```sql
SELECT 
    domain,
    MAX(bucket) AS time,
    SUM(total) AS hits
FROM website_stats_1m
WHERE bucket >= NOW() - INTERVAL '5 minutes'
```

## Procedimento de Candidatura

1. Obter configuração
2. Query eleição
3. Insert the novos candidatos

## Background Job

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

# Elegendo Candidatos

```sql
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
```

##  Limita tabela de Longo Termo

```sql
DELETE FROM top_websites_longterm
WHERE domain IN (
    SELECT domain
    FROM top_websites_longterm
    ORDER BY last_seen ASC
    OFFSET max_entries
);
```
##  Limpa candidatos antigos

```sql
DELETE FROM top_websites_candidates
WHERE time < now() - retention_period;
```

## Encapsula tudo em um job

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

# Inspecionando

## View de Relatório

```sql
CREATE OR REPLACE VIEW top_websites_report AS
SELECT 
    domain,
    total_hits,
    last_election_hits,
    times_elected,
    first_seen,
    last_seen
FROM top_websites_longterm
ORDER BY total_hits DESC;
```

# Tarefas em Background

```sql
PERFORM add_job(func_name, time_interval, *config)
```
## Inserir candidatos

```sql
PERFORM add_job('populate_website_candidates_job',
    (config_record.election_schedule_interval / 10)::interval,
    initial_start => NOW() + INTERVAL '10 seconds'
);
```

## Job de eleição (a cada hora)

```sql
PERFORM add_job('elect_top_websites',
  config_record.election_schedule_interval,
  initial_start => NOW() + INTERVAL '1 minute'
);
```

## Verificar status dos jobs

```sql
SELECT job_id, proc_name, schedule_interval, next_start 
FROM timescaledb_information.jobs 
WHERE proc_name IN ('populate_website_candidates_job', 'elect_top_websites')
ORDER BY job_id;
```

## Verificação do Sistema

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

## Performance do Sistema

- **Logs brutos:** 1GB/hora → 24GB/dia - 2.4 Bilhões de registros processados por hora
- **Dados agregados:** 10MB/hora → 240MB/dia
- **Taxa de compressão:** 99.6% de redução
- **Queries em tempo real:** <5ms para agregados

# Monitoramento

```sql
SELECT proc_name, schedule_interval, next_start - now() as next_start, scheduled
FROM timescaledb_information.jobs
WHERE proc_name IN ('populate_website_candidates_job', 'elect_top_websites');
```

## Verificação de Erros
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

## Verificação de Agregados
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

# Conclusão

1. **Redução de 99.6%** no armazenamento
2. **Análise em tempo real** com latência <5ms
3. **Identificação automática** de top websites
4. **Escalabilidade** com custos controlados
5. **Sistema totalmente automatizado** com jobs em background


## Resultado Final
Sistema que processa **500GB diários** mantendo apenas **20GB** de dados essenciais, com identificação automática dos websites mais relevantes.

# Perguntas?

**Contato:** https://ideia.me  
**Linkedin:**  https://www.linkedin.com/in/jonatasdp/
**Código:** https://github.com/jonatas/timescaledb-templates - Pasta `webtop`


## Análise dos Chunks

```sql
-- Chunks existentes
SELECT COUNT(*) as total_chunks, 
       MIN(range_start) as oldest_chunk, 
       MAX(range_end) as newest_chunk 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'logs';
```

## Tamanho da tabela

```sql
SELECT pg_size_pretty(sum(total_bytes))
FROM chunks_detailed_size( 'logs');
```

## Detalhes das partições

```sql
select COUNT(*) as total_chunks,
    MIN(range_start) as oldest_data,
    MAX(range_end) as newest_data,
    NOW() - MIN(range_start) as data_age
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'logs';
```

## Configuração dos jobs

```sql
table timescaledb_information.jobs;
table timescaledb_information.job_stats;
table timescaledb_information.job_errors;
```

## Funções da API

```sql
select add_job(func_name, *config); -- returns job_id
select delete_job(job_id);
select alter_job(job_id, *config);
```

## Em caso de emergência

```sql
SELECT _timescaledb_internal.stop_background_workers();
SELECT _timescaledb_internal.start_background_workers();
```
