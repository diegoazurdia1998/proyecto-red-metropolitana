# Fase 1.2 — Costo de la unidad (Persona vs Tarjeta)

_Generado: 2026-10-30T03:59:49_

## Justificación de la unidad: Persona (`sk_usuario`)

La unidad de análisis es **Persona**, no Tarjeta. Esta decisión se tomó y justificó en el Proyecto 1: Transmetro, Transurbano y MetroRiel comparten un identificador entero subyacente que se normaliza con `REGEXP_REPLACE(tarjeta, '\D', '', 'g')::bigint::text`, mientras que Aerómetro usa `user_hash` (hex de 12 chars) y no puede correlacionarse determinísticamente con los otros sistemas. El P2 no redefine la unidad; **mide el costo empírico** de haber elegido Persona.

## Las 3 métricas del costo

| # | Métrica | Numerador | Denominador | Valor |
|---|---|---|---|---|
| 1_costo_identidad | Fracción de usuarios de la población con >= 2 tarjetas asociadas | 27,778 | 58,600 | 47.4% |
| 2_cobertura_identidad | Fracción de user_hash de Aerómetro vinculables a un sk_usuario de tarjeta | 0 | 16,930 | 0.0% |
| 3_falsos_positivos | Tarjetas 'abandonadas' de personas con etiqueta_persona=0 y >= 2 tarjetas | 7,347 | 92,622 | 7.93% |

### Interpretación de negocio

**1_costo_identidad**

El 47.4% de la población usa ≥2 modos (y por eso tiene ≥2 tarjetas asociadas). Esto **crea la posibilidad** de falsos positivos si se trabaja a nivel tarjeta (una tarjeta puede 'abandonar' mientras la persona sigue viajando con otra), pero **el daño real se mide en la métrica 3**.

**2_cobertura_identidad**

El 0.0% de los identificadores crudos se mapean a un `sk_usuario` canónico. La cobertura **no es 100%** porque Aerómetro no puede correlacionarse con los otros sistemas: cada `user_hash` es un identificador aislado. La decisión Persona **hereda esta limitación** y la documenta como gobernanza.

**3_falsos_positivos**

El 7.93% de las tarjetas de la población serían **falsos positivos** si se trabajara a nivel tarjeta (tarjeta sin abordajes en 30d, pero persona con al menos un abordaje en otra tarjeta). Este es el **costo principal** de la unidad: contactos de retención desperdiciados en usuarios que en realidad siguen viajando.

## Limitación declarada: Aerómetro

Aerómetro usa `user_hash` de 12 chars. Cada hash es un identificador **aislado**: no se puede mapear a un `sk_usuario` de Transmetro/Transurbano/MetroRiel, ni a otro `user_hash` del mismo usuario en Aerómetro. Para el cálculo del costo, Aerómetro cuenta como **1 tarjeta** en el denominador, lo cual es conservador: si un usuario usa Aerómetro y Transmetro, aparecerá como 2 identificadores no vinculados, aunque en realidad sea 1 persona.
## Relación entre las 3 métricas y costo monetario

| Métrica | Rol en el análisis |
|---|---|
| **1_costo_identidad** | Magnitud del fenómeno multi-tarjeta (población en riesgo). |
| **2_cobertura_identidad** | Limitación de gobernanza: Aerómetro no se puede vincular. |
| **3_falsos_positivos** | **Costo materializado**: tarjetas que se marcarían como abandonadas sin serlo. |

**Costo monetario de la decisión Persona vs Tarjeta:**

- Falsos positivos por corte: **7,347 tarjetas**.
- Costo de contacto: Q5.00 × 7,347 = **Q36,735.00 por corte**.
- Proyectado a 12 cortes/año: **Q440,820.00/año**.

**Conclusión:** la decisión Persona (vs Tarjeta) evita estos contactos desperdiciados. Es el ahorro que justifica la complejidad de la resolución de identidad.
