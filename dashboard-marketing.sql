------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--- visitas novas

WITH lead_base_consolidation AS (
    SELECT date_trunc( {{agrupamento_de_data}}, DAY) AS date, INTERNAL, sum(accoutancy_visitors) AS visitantes_novos
FROM tofu.navigation_base_full e
GROUP BY 1, 2)
SELECT
  date,
  CASE WHEN internal IN ('others', 'mobile')
    THEN 'branded'
  ELSE internal END     AS internal,
  sum(visitantes_novos) AS visitantes_novos
FROM lead_base_consolidation
WHERE
  date BETWEEN {{data_de_inicio}}::date AND {{data_de_fim}}::date [[AND INTERNAL = lower({{canal_mkt}})]] GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

WITH lead_base_consolidation AS (
    SELECT
      date_trunc(WEEK, DAY)    AS date,
      INTERNAL,
      sum(accoutancy_visitors) AS visitantes_novos
    FROM tofu.navigation_base_full e
    GROUP BY 1, 2)
SELECT
  date,
  CASE WHEN internal IN ('others', 'mobile')
    THEN 'branded'
  ELSE internal END     AS internal,
  sum(visitantes_novos) AS visitantes_novos
FROM lead_base_consolidation
WHERE
  date BETWEEN '2017-07-01' AND '2017-07-31' AND INTERNAL = lower('paid')
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
---leads novos

WITH lead_base_consolidation AS (
    SELECT
      date_trunc({{agrupamento_de_data}}, lead.lead_date)     AS DATA,
      lead.lead_internal,
      COUNT(DISTINCT lead.email)
        FILTER (WHERE lead.is_accountancy) AS leads_unicos
    FROM tofu.lead_first_last_full AS lead
    GROUP BY 1, 2
)
SELECT
  data,
  CASE WHEN lead_internal IN ('others', 'mobile')
    THEN 'branded'
  ELSE lead_internal END AS internal,
  sum(leads_unicos)      AS leads_novos
FROM lead_base_consolidation
WHERE data BETWEEN {{data_de_inicio}} AND {{data_de_fim}}
[[AND lead_internal = lower({{canal_mkt}})]]
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

WITH lead_base_consolidation AS (
    SELECT
      date_trunc(WEEK, lead.lead_date)     AS DATA,
      lead.lead_internal,
      COUNT(DISTINCT lead.email)
        FILTER (WHERE lead.is_accountancy) AS leads_unicos
    FROM tofu.lead_first_last_full AS lead
    GROUP BY 1, 2
)
SELECT
  data,
  CASE WHEN lead_internal IN ('others', 'mobile')
    THEN 'branded'
  ELSE lead_internal END AS internal,
  sum(leads_unicos)      AS leads_novos
FROM lead_base_consolidation
WHERE data BETWEEN '2017-07-01' AND '2017-07-31'
[[AND lead_internal = lower('paid')]]
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--- leads por visitas

SELECT
  v.data,
  v.internal,
  CASE WHEN sum(v.visitantes_novos) > 0
    THEN sum(l.leads_novos) / sum(v.visitantes_novos)
  ELSE 0 END AS visitors_to_leads
FROM (
       WITH lead_base_consolidation AS (
           SELECT
             date_trunc({{agrupamento_de_data}}, navigation_date) AS data,
             internal,
             count(DISTINCT e.email)
               FILTER (WHERE e.is_accountancy)   AS leads_unicos
           FROM tofu.lead_base_full e
           GROUP BY 1, 2)
       SELECT
         data,
         CASE WHEN internal IN ('others', 'mobile')
           THEN 'branded'
         ELSE internal END AS internal,
         sum(leads_unicos) AS leads_novos
       FROM lead_base_consolidation
       WHERE data BETWEEN {{data_de_inicio}}::date AND {{data_de_fim}}::date [[AND INTERNAL = lower({{canal_mkt}})]]
       GROUP BY 1, 2
     ) l
  JOIN
  (
    WITH visit_base_consolidation AS (
        SELECT
          date_trunc({{agrupamento_de_data}}, day)           AS data,
          internal,
          sum(visitantes_unicos)
            FILTER (WHERE e.is_accountancy) AS visitantes_novos
        FROM tofu.navigation_base_full e
        GROUP BY 1, 2)
    SELECT
      data,
      CASE WHEN internal IN ('others', 'mobile')
        THEN 'branded'
      ELSE internal END     AS internal,
      sum(visitantes_novos) AS visitantes_novos
    FROM visit_base_consolidation
    WHERE data BETWEEN {{data_de_inicio}}::date AND {{data_de_fim}}::date [[AND INTERNAL = lower({{canal_mkt}})]]
    GROUP BY 1, 2
  ) v ON v.data = l.data AND v.internal = l.internal
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

SELECT
  v.data,
  v.internal,
  CASE WHEN sum(v.visitantes_novos) > 0
    THEN sum(l.leads_novos) / sum(v.visitantes_novos)
  ELSE 0 END AS visitors_to_leads
FROM (
       WITH lead_base_consolidation AS (
           SELECT
             date_trunc('week', navigation_date) AS data,
             internal,
             count(DISTINCT e.email)
               FILTER (WHERE e.is_accountancy)   AS leads_unicos
           FROM tofu.lead_base_full e
           GROUP BY 1, 2)
       SELECT
         data,
         CASE WHEN internal IN ('others', 'mobile')
           THEN 'branded'
         ELSE internal END AS internal,
         sum(leads_unicos) AS leads_novos
       FROM lead_base_consolidation
       WHERE data BETWEEN '2017-07-01' AND '2017-07-05' AND INTERNAL = lower('paid')
       GROUP BY 1, 2
     ) l
  JOIN
  (
    WITH visit_base_consolidation AS (
        SELECT
          date_trunc('week', day)           AS data,
          internal,
          sum(visitantes_unicos)
            FILTER (WHERE e.is_accountancy) AS visitantes_novos
        FROM tofu.navigation_base_full e
        GROUP BY 1, 2)
    SELECT
      data,
      CASE WHEN internal IN ('others', 'mobile')
        THEN 'branded'
      ELSE internal END     AS internal,
      sum(visitantes_novos) AS visitantes_novos
    FROM visit_base_consolidation
    WHERE data BETWEEN '2017-07-01' AND '2017-07-031' AND internal = lower('paid')
    GROUP BY 1, 2
  ) v ON v.data = l.data AND v.internal = l.internal
GROUP BY 1, 2;


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--- Novos Parceiros

SELECT
  date_trunc({{agrupamento_de_data}}, became_partner_date) :: DATE AS Dia,
  COUNT(DISTINCT id)
    FILTER (WHERE became_partner)                AS Parceiros
FROM public.accountancy
WHERE became_partner_date BETWEEN  {{data_de_inicio}}::date AND {{data_de_fim}}::date
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------

SELECT
  date_trunc('day', became_partner_date) :: DATE AS Dia,
  COUNT(DISTINCT id)
    FILTER (WHERE became_partner)                AS Parceiros
FROM public.accountancy
WHERE became_partner_date BETWEEN '2017-01-01' AND '2017-07-05'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--- Leads / parceiros

SELECT
  v.data,
  CASE WHEN sum(v.parceiros) > 0
    THEN sum(l.leads_novos) / sum(v.parceiros)
  ELSE 0 END AS visitors_to_leads
FROM (
       WITH lead_base_consolidation AS (
           SELECT
             date_trunc({{agrupamento_de_data}}, navigation_date) AS data,
             internal,
             count(DISTINCT e.email)
               FILTER (WHERE e.is_accountancy)   AS leads_unicos
           FROM tofu.lead_base_full e
           GROUP BY 1, 2)
       SELECT
         data,
         sum(leads_unicos) AS leads_novos
       FROM lead_base_consolidation
       WHERE data BETWEEN {{data_de_inicio}}::date AND {{data_de_fim}}::date
       GROUP BY 1
     ) l
  JOIN (
         SELECT
           date_trunc({{agrupamento_de_data}}, acc.became_partner_date) :: DATE AS data,
           COUNT(DISTINCT id)
             FILTER (WHERE acc.became_partner)                AS parceiros
         FROM public.accountancy acc
         WHERE acc.became_partner_date BETWEEN {{data_de_inicio}}::date AND {{data_de_fim}}::date
         GROUP BY 1
       ) v ON v.data = l.data
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------

SELECT
  v.data,
  CASE WHEN sum(v.parceiros) > 0
    THEN sum(l.leads_novos) / sum(v.parceiros)
  ELSE 0 END AS visitors_to_leads
FROM (
       WITH lead_base_consolidation AS (
           SELECT
             date_trunc('week', navigation_date) AS data,
             internal,
             count(DISTINCT e.email)
               FILTER (WHERE e.is_accountancy)   AS leads_unicos
           FROM tofu.lead_base_full e
           GROUP BY 1, 2)
       SELECT
         data,
         sum(leads_unicos) AS leads_novos
       FROM lead_base_consolidation
       WHERE data BETWEEN '2017-07-01' AND '2017-07-31'
       GROUP BY 1
     ) l
  JOIN (
         SELECT
           date_trunc('day', acc.became_partner_date) :: DATE AS data,
           COUNT(DISTINCT id)
             FILTER (WHERE acc.became_partner)                AS parceiros
         FROM public.accountancy acc
         WHERE acc.became_partner_date BETWEEN '2017-01-01' AND '2017-07-05'
         GROUP BY 1
       ) v ON v.data = l.data
GROUP BY 1
