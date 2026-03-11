# Project Context

## Business Description
[Esta es una compañia que se dedica a la venta de zapatillas, en gran parte a traves de distribuidores
Sabemos que tiene algunos productos estables y algunos que han tenido "boom" y despues han desaparecido, 
como es de esperar de productos de moda. Queremos entender mejor el negocio y los riesgos de estas modas ]

## Business Questions We Want to Answer
[Queremos tener un mejor entendimiento del crecimiento del negocio, los drivers y potenciales riesgos]

## Dataset Description
[Nombre Modelo: nos dice el producto especifico, hay algunos estables y creemos que otros más de moda
Canal: nos dice el tipo de distribución.
Venta Total: venta sin IVA.
Ventas Tot: nuesta columna en € con IVA.
Línea: los tipos de producto, creemos que hay 3 principales -> CITY, TRIBE y DISTRICT (confirmar)-
Año fiscal: nos gusta ver números y crecimientos (CAGR por ejemplo) en base a los años fiscales.
]

## Analysis Catalog
The agent should choose analyses from this list. It may combine or extend them. The focus is business understanding.

- Descriptive statistics and data profiling (row counts, nulls, data types, ranges)
- Revenue distribution by client, product, category, region
- Top N concentration (Top 10, Top 20, Top 50 clients or products by revenue share)
- Like-for-like (LFL) vs non-LFL client analysis (clients active in both periods vs new/lost)
- Time trends (monthly, quarterly, annual revenue and volume evolution)
- Seasonality patterns (which months or quarters are strongest/weakest)
- Price × Quantity decomposition (PxQ: how much of revenue change comes from price vs volume)
- Customer cohort behavior (when did clients first buy, are they still active)
- Business concentration risk (Herfindahl-style, top client dependency)
- Product mix evolution (how the category/product mix shifts over time)
- New vs returning client revenue split per period
- Average ticket size evolution (revenue per transaction or per client per period)
- Geographic or segment breakdown if columns exist
