WITH 
cte_data_prestation_category AS (
    -- cte de sélection de la colonne data_prestation_category: Les abonnements et les offres.
    SELECT
        p.prestation_id,
        CASE
            WHEN (LOWER(p.name_standard) LIKE 'abonnement :%') THEN 'Abonnements'
            WHEN (LOWER(p.name_standard) LIKE 'avoir sur facture') THEN 'Avoirs Sur Factures'
            WHEN (LOWER(p.name_standard) LIKE 'offre commerciale :%') THEN 'Offres Commerciales'
            WHEN (LOWER(p.name_standard) LIKE 'offre de parrainage:%') THEN 'Offres De Parrainage'
            WHEN (LOWER(p.name_standard) LIKE 'partenariat :%') THEN 'Offres De Partenariat'
            WHEN (LOWER(p.name_standard) LIKE 'remise commerciale%') THEN 'Remises Commerciales'
            WHEN (p.department = 'sales') THEN 'Autres Offres Commerciales'
            WHEN (LOWER(p.name_standard) LIKE 'rattrapage%') THEN 'Rattrapages'
            WHEN (LOWER(p.name_standard) LIKE 'fiches de paie') THEN 'Fiches de paie'
        ELSE 'Ponctuel'
        END AS data_prestation_category
    FROM {{ ref('stg_prestations') }} p
)

SELECT 
  p.prestation_id,
  p.company_id,
  p.name_standard,
  p.name,
  p.department,
  CASE 
    WHEN cdpc.data_prestation_category IN ('Abonnements', 'Fiches de paie') THEN TRUE
        ELSE FALSE
    END AS is_recurring,
  p.category,
  p.discount_reason,
  p.discount_product,
  cdpc.data_prestation_category,
  p.amount_excl_tax,
  p.invoice_issued_at,
  p.invoice_paid_at,
  CASE
    WHEN p.invoice_paid_at IS NOT NULL THEN TRUE
    ELSE FALSE
    END AS prestation_is_paid,
  p.created_at,
  p.billing_invoice_id,
  p.credited_billing_invoice_id,
  bii.quantity AS billing_invoice_item_quantity,
  p.date_last_refresh
FROM {{ ref('stg_prestations') }} p
LEFT JOIN cte_data_prestation_category cdpc
    ON p.prestation_id = cdpc.prestation_id
LEFT JOIN {{ ref('stg_billing_invoice_items') }} bii
    ON p.prestation_id = bii.billing_invoice_item_id
