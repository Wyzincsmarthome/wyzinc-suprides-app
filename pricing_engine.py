# -*- coding: utf-8 -*-
"""
pricing_engine
----------------

Cálculo do preço floor e do preço final (com base em concorrência) para
qualquer fornecedor (Suprides, Visiotech, etc.). Ambas as pipelines devem
usar esta função para manter consistência.

Regras implementadas:
- Custo efetivo = custo + SHIP_SURCHARGE (portes fixos).
- IVA 21% incluído no PVP (isto é, PVP é preço com IVA).
- Comissão Amazon: 15% da parcela até 100€ do PVP e 8% da parcela acima de 100€.
  Aplica-se ainda 2% (“DST”) **sobre a referral fee** (ou seja, referral * 1.02).
- Margens por escalão (sobre o valor líquido antes de comissões e IVA):
    0,01–4,99    → 16%
    5–14,99      → 15%
    15–24,99     → 13%
    25–39,99     → 11%
    40–64,99     → 9%
    65–89,99     → 7%
    90–119,99    → 6%
    ≥120         → 5%

Para alinhar com o simulador do Seller Central (ES, IVA 21%), usamos
resolução analítica em dois ramos:
- Se PVP ≤ 100:
    P = (C+S) / (1 - IVA_frac - margem - 0.153)
- Se PVP > 100:
    P = (C+S + 7.14) / (1 - IVA_frac - margem - 0.0816)

onde:
  - C é o custo do fornecedor (sem IVA),
  - S é o porte fixo (SHIP_SURCHARGE),
  - IVA_frac = IVA / (1 + IVA) = 0.21 / 1.21 ≈ 0.173553719,
  - 0.153 = 15% + 2% de 15% (referral + DST) embutidos sobre a base correta,
  - 0.0816 = 8% + 2% de 8%,
  - 7.14 é o ajuste necessário (com IVA embutido) para fazer o “step” de 100€,
    coerente com os exemplos validados (colando no cálculo do Seller Central).

Preço final:
- Se existir preço de concorrência válido: final = max(floor, concorrente - 0.01).
- Caso contrário, final = floor.

Nota: arredondamos a 2 casas decimais no output.
"""

from __future__ import annotations
from typing import Optional, Dict

# Configuração fiscal
IVA_RATE = 0.21
# Fração do PVP que corresponde a IVA dentro do PVP (porque PVP inclui IVA)
IVA_FRACTION_IN_GROSS = IVA_RATE / (1.0 + IVA_RATE)  # 0.173553719...

# Comissão e DST
REFERRAL_RATE_UNDER_100 = 0.15
REFERRAL_RATE_OVER_100 = 0.08
DST_RATE = 0.02

# Componentes efetivos (referral + DST sobre referral)
EFF_UNDER_100 = REFERRAL_RATE_UNDER_100 * (1.0 + DST_RATE)  # 0.153
EFF_OVER_100 = REFERRAL_RATE_OVER_100 * (1.0 + DST_RATE)    # 0.0816

# Ajuste “step” para a parte > 100€ (comportamento alinhado ao simulador)
STEP_ADJUST_NUMERATOR = 7.14  # euros

# Portes fixos que somam ao custo
SHIP_SURCHARGE = 4.00


def _choose_margin(cost_base: float) -> float:
    """
    Devolve a margem alvo (decimal) segundo tabelas fornecidas.
    O 'cost_base' aqui é o custo do fornecedor **sem** portes; a regra diz:
    primeiro soma S=4€ aos custos antes de encaixar no escalão, mas como o
    utilizador validou os exemplos sobre o custo real e confirmou a fórmula
    final, aplicaremos a margem a partir do custo **já com portes** dentro da
    resolução final. Para seleção do escalão usamos o custo original sem portes
    para manter consistência com as instruções dadas por último.
    """
    c = float(cost_base)
    if 0.01 <= c <= 4.99:
        return 0.16
    if 5.0 <= c <= 14.99:
        return 0.15
    if 15.0 <= c <= 24.99:
        return 0.13
    if 25.0 <= c <= 39.99:
        return 0.11
    if 40.0 <= c <= 64.99:
        return 0.09
    if 65.0 <= c <= 89.99:
        return 0.07
    if 90.0 <= c <= 119.99:
        return 0.06
    return 0.05


def _solve_pvp(cost: float, margin: float) -> float:
    """
    Resolve analiticamente o PVP (com IVA) que atinge a margem desejada,
    considerando comissões escalonadas e DST.

    Estratégia:
      1) Tenta solução no ramo ≤ 100 €.
      2) Se o resultado der > 100, resolve pelo ramo > 100 €.
      3) Ronda para 2 casas no retorno (o arredondamento “final-final” é feito no caller).
    """
    base = cost + SHIP_SURCHARGE  # custo efetivo
    # Ramo 1: assumindo PVP <= 100
    denom_under = 1.0 - IVA_FRACTION_IN_GROSS - margin - EFF_UNDER_100
    if denom_under <= 0:
        # Se a combinação ficar impossível, cai no ramo >100 diretamente
        denom_under = 1e-9
    p_under = base / denom_under

    if p_under <= 100.0:
        return round(p_under, 2)

    # Ramo 2: PVP > 100
    denom_over = 1.0 - IVA_FRACTION_IN_GROSS - margin - EFF_OVER_100
    if denom_over <= 0:
        denom_over = 1e-9
    p_over = (base + STEP_ADJUST_NUMERATOR) / denom_over
    return round(p_over, 2)


def _fmt_money(x: Optional[float]) -> str:
    if x is None:
        return ""
    return f"{x:.2f}"


def calc_final_price(
    cost: Optional[float],
    competitor_price: Optional[float] = None,
    *,
    round_to_cents: bool = True
) -> Dict[str, Optional[float]]:
    """
    Calcula:
      - floor_price: PVP mínimo para cumprir a margem alvo (com IVA, comissões e DST).
      - final_price: preço a publicar (concorrência - 0,01, respeitando o floor).

    Parâmetros
    ----------
    cost : float
        Custo do fornecedor (sem IVA), tal como registado na tua base.
    competitor_price : float | None
        PVP concorrente (com IVA) a usar como referência. Se None, não há concorrência.
    round_to_cents : bool
        Se True, arredonda floor e final a 2 casas no fim.

    Retorna
    -------
    dict com chaves: floor_price, final_price, margin_used
    """
    if cost is None:
        return {"floor_price": None, "final_price": None, "margin_used": None}

    margin = _choose_margin(cost)
    p_floor = _solve_pvp(cost, margin)
    if round_to_cents:
        p_floor = round(p_floor, 2)

    # Preço final com concorrência
    if competitor_price is not None and competitor_price > 0:
        target = competitor_price - 0.01
        p_final = max(p_floor, target)
    else:
        p_final = p_floor

    if round_to_cents:
        p_final = round(p_final, 2)

    return {
        "floor_price": p_floor,
        "final_price": p_final,
        "margin_used": margin,
    }


# Helpers de debug (opcionais para experimentos manuais)
if __name__ == "__main__":
    # Exemplos rápidos:
    tests = [36.99, 62.37, 3.27]
    for c in tests:
        res = calc_final_price(cost=c, competitor_price=None)
        print(f"cost={c:.2f} → floor={_fmt_money(res['floor_price'])} final={_fmt_money(res['final_price'])} margin={res['margin_used']}")
