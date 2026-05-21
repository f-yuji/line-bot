from __future__ import annotations

import html
from typing import Sequence


BG = "#0a0c10"
BORDER = "#1f242e"
GRID = "#1f242e"
AXIS_TEXT = "#6c7280"

CLOSE = "#e6e9ef"
CURRENT = "#d4a44a"
TEXT_TITLE = "#e6e9ef"
TEXT_SUB = "#9aa0aa"

BOX_FILL = "rgba(212,164,74,0.05)"
BOX_EDGE = "rgba(212,164,74,0.45)"

ENTRY_FILL = "rgba(94,230,168,0.10)"
ENTRY_EDGE = "rgba(94,230,168,0.55)"
ENTRY_TEXT = "#5ee6a8"

STOP_EDGE = "rgba(239,68,68,0.65)"
STOP_TEXT = "#ef4444"
TP_EDGE = "rgba(96,165,250,0.65)"
TP_TEXT = "#60a5fa"

MA5 = ("#56b7ff", 1.1, 0.38)
MA25 = ("#e6c860", 1.1, 0.32)
MA75 = ("#7d8597", 1.0, 0.28)


def _esc(value: object) -> str:
    return html.escape(str(value if value is not None else ""))


def _as_float(value: object) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _fmt(value: object, digits: int = 1, suffix: str = "") -> str:
    number = _as_float(value)
    if number is None:
        return "-"
    return f"{number:.{digits}f}{suffix}"


def _fmt_signed(value: object, digits: int = 1, suffix: str = "%") -> str:
    number = _as_float(value)
    if number is None:
        return "-"
    sign = "+" if number > 0 else ""
    return f"{sign}{number:.{digits}f}{suffix}"


def _yscale(values: Sequence[float], pad_top: int, plot_h: int):
    clean = [float(v) for v in values if v is not None]
    if not clean:
        clean = [0.0, 1.0]
    mn, mx = min(clean), max(clean)
    pad = (mx - mn) * 0.06 or 1.0
    y_min, y_max = mn - pad, mx + pad
    y_range = y_max - y_min or 1.0

    def y_of(v: float) -> float:
        return pad_top + plot_h - ((float(v) - y_min) / y_range) * plot_h

    return y_of, y_min, y_max


def _xscale(n: int, pad_left: int, plot_w: int):
    def x_of(i: int) -> float:
        return pad_left + (i / (n - 1 or 1)) * plot_w

    return x_of


def _poly(arr: Sequence[float], x_of, y_of) -> str:
    return " ".join(f"{x_of(i):.1f},{y_of(v):.1f}" for i, v in enumerate(arr))


def _line_with_label(
    parts: list[str],
    *,
    y: float,
    width: int,
    pad_l: int,
    pad_r: int,
    color: str,
    text_color: str,
    label: str,
    y_shift: float = 0.0,
    dash: str = "4 4",
) -> None:
    label_x = width - pad_r - 4
    parts.append(
        f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}" '
        f'stroke="{color}" stroke-width="1.1" stroke-dasharray="{dash}"/>'
    )
    parts.append(
        f'<rect x="{label_x-88}" y="{y-8+y_shift:.1f}" width="88" height="15" '
        f'rx="3" fill="{BG}" stroke="{color}"/>'
    )
    parts.append(
        f'<text x="{label_x-4}" y="{y+3+y_shift:.1f}" fill="{text_color}" '
        f'font-family="system-ui, sans-serif" font-size="10" text-anchor="end">{_esc(label)}</text>'
    )


def render_chart(
    *,
    code: str,
    name: str,
    trade_date: Sequence[str],
    close: Sequence[float],
    ma5: Sequence[float],
    ma25: Sequence[float],
    ma75: Sequence[float],
    box_high: float,
    box_low: float,
    entry_min: float,
    entry_max: float,
    current_price: float,
    box_position_pct: float | None = None,
    bounce_count: int | None = None,
    bounce_points: list[dict] | None = None,
    rsi14: float | None = None,
    margin_ratio: float | None = None,
    box_score: float | None = None,
    stop_loss_price: float | None = None,
    take_profit_price: float | None = None,
    atr_pct: float | None = None,
    ma5_gap_pct: float | None = None,
    ma25_gap_pct: float | None = None,
    ma75_gap_pct: float | None = None,
    width: int = 720,
    height: int = 260,
) -> str:
    pad_l, pad_r, pad_t, pad_b = 54, 18, 24, 28
    pw, ph = width - pad_l - pad_r, height - pad_t - pad_b

    n = min(len(trade_date), len(close), len(ma5), len(ma25), len(ma75))
    if n <= 0:
        return _empty_svg(width, height, "chart data unavailable")
    trade_date = list(trade_date)[-n:]
    close = [float(v) for v in close[-n:]]
    ma5 = [float(v) for v in ma5[-n:]]
    ma25 = [float(v) for v in ma25[-n:]]
    ma75 = [float(v) for v in ma75[-n:]]

    extra_lines = [
        v
        for v in (box_high, box_low, entry_min, entry_max, current_price, stop_loss_price, take_profit_price)
        if v is not None
    ]
    all_y = list(close) + list(ma5) + list(ma25) + list(ma75) + [float(v) for v in extra_lines]
    y_of, y_min, y_max = _yscale(all_y, pad_t, ph)
    x_of = _xscale(len(close), pad_l, pw)
    y_range = y_max - y_min

    parts: list[str] = []
    parts.append(f'<rect width="{width}" height="{height}" fill="{BG}"/>')
    parts.append(
        f'<rect x="0.5" y="0.5" width="{width-1}" height="{height-1}" '
        f'rx="10" fill="none" stroke="{BORDER}"/>'
    )

    for i in range(5):
        v = y_max - (i / 4) * y_range
        y = y_of(v)
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}" '
            f'stroke="{GRID}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{pad_l-8}" y="{y+4:.1f}" fill="{AXIS_TEXT}" '
            f'font-family="ui-monospace, monospace" font-size="10" text-anchor="end">{v:,.0f}</text>'
        )

    y_bh, y_bl = y_of(box_high), y_of(box_low)
    parts.append(
        f'<rect x="{pad_l}" y="{y_bh:.1f}" width="{pw}" height="{max(0, y_bl-y_bh):.1f}" fill="{BOX_FILL}"/>'
    )
    _line_with_label(parts, y=y_bh, width=width, pad_l=pad_l, pad_r=pad_r, color=BOX_EDGE, text_color=CURRENT, label=f"box上 {box_high:,.0f}", y_shift=-4)
    _line_with_label(parts, y=y_bl, width=width, pad_l=pad_l, pad_r=pad_r, color=BOX_EDGE, text_color=CURRENT, label=f"box下 {box_low:,.0f}", y_shift=8)

    if take_profit_price is not None:
        _line_with_label(
            parts,
            y=y_of(take_profit_price),
            width=width,
            pad_l=pad_l,
            pad_r=pad_r,
            color=TP_EDGE,
            text_color=TP_TEXT,
            label=f"利確 {take_profit_price:,.0f}",
            y_shift=-18 if abs(y_of(take_profit_price) - y_bh) < 12 else -3,
        )
    if stop_loss_price is not None:
        _line_with_label(
            parts,
            y=y_of(stop_loss_price),
            width=width,
            pad_l=pad_l,
            pad_r=pad_r,
            color=STOP_EDGE,
            text_color=STOP_TEXT,
            label=f"損切 {stop_loss_price:,.0f}",
            y_shift=20 if abs(y_of(stop_loss_price) - y_bl) < 12 else 3,
        )

    y_eh, y_el = y_of(entry_max), y_of(entry_min)
    parts.append(
        f'<rect x="{pad_l}" y="{y_eh:.1f}" width="{pw}" height="{max(1, y_el-y_eh):.1f}" fill="{ENTRY_FILL}"/>'
    )
    for y in (y_eh, y_el):
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width-pad_r}" y2="{y:.1f}" '
            f'stroke="{ENTRY_EDGE}" stroke-width="1"/>'
        )

    for arr, (color, sw, op) in ((ma75, MA75), (ma25, MA25), (ma5, MA5)):
        parts.append(
            f'<polyline points="{_poly(arr, x_of, y_of)}" fill="none" stroke="{color}" '
            f'stroke-width="{sw}" stroke-linejoin="round" stroke-linecap="round" opacity="{op}"/>'
        )

    parts.append(
        f'<polyline points="{_poly(close, x_of, y_of)}" fill="none" stroke="{CLOSE}" '
        f'stroke-width="2.0" stroke-linejoin="round" stroke-linecap="round"/>'
    )

    date_to_index = {str(d): i for i, d in enumerate(trade_date)}
    for idx, point in enumerate((bounce_points or [])[-10:], start=1):
        date = str(point.get("date") or "")
        price = _as_float(point.get("price"))
        if date not in date_to_index or price is None:
            continue
        x = x_of(date_to_index[date])
        y = y_of(price)
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{ENTRY_TEXT}" stroke="{BG}" stroke-width="1.4"/>')
        parts.append(
            f'<text x="{x:.1f}" y="{y-7:.1f}" fill="{ENTRY_TEXT}" font-family="ui-monospace, monospace" '
            f'font-size="9" text-anchor="middle">{idx}</text>'
        )

    y_cur = y_of(current_price)
    x_last = x_of(len(close) - 1)
    parts.append(
        f'<line x1="{pad_l}" y1="{y_cur:.1f}" x2="{width-pad_r}" y2="{y_cur:.1f}" '
        f'stroke="{CURRENT}" stroke-width="1" stroke-dasharray="2 3" opacity="0.6"/>'
    )

    entry_mid_y = (y_eh + y_el) / 2
    parts.append(
        f'<rect x="{pad_l+4}" y="{entry_mid_y-7:.1f}" width="128" height="14" fill="rgba(94,230,168,0.12)" stroke="{ENTRY_EDGE}"/>'
    )
    parts.append(
        f'<text x="{pad_l+8}" y="{entry_mid_y+3:.1f}" fill="{ENTRY_TEXT}" '
        f'font-family="ui-monospace, monospace" font-size="10">entry {entry_min:,.0f}-{entry_max:,.0f}</text>'
    )

    pill_w = 64
    pill_x = min(x_last + 8, width - pad_r - pill_w - 2)
    parts.append(f'<line x1="{x_last:.1f}" y1="{y_cur:.1f}" x2="{pill_x:.1f}" y2="{y_cur:.1f}" stroke="{CURRENT}" stroke-width="1" opacity="0.7"/>')
    parts.append(f'<circle cx="{x_last:.1f}" cy="{y_cur:.1f}" r="3.4" fill="{CURRENT}" stroke="{BG}" stroke-width="1.5"/>')
    parts.append(f'<rect x="{pill_x:.1f}" y="{y_cur-9:.1f}" width="{pill_w}" height="18" rx="3" fill="{CURRENT}"/>')
    parts.append(
        f'<text x="{pill_x+pill_w/2:.1f}" y="{y_cur+4:.1f}" fill="{BG}" font-family="ui-monospace, monospace" '
        f'font-size="11" font-weight="700" text-anchor="middle">{current_price:,.0f}</text>'
    )

    parts.append(
        f'<text x="{pad_l}" y="16" fill="{TEXT_TITLE}" font-family="system-ui, sans-serif" '
        f'font-size="13" font-weight="700">{_esc(code)} 日足</text>'
    )
    parts.append(f'<text x="{pad_l+64}" y="16" fill="{TEXT_SUB}" font-family="system-ui, sans-serif" font-size="11">{_esc(name)}</text>')

    _info_box(parts, width, box_score, box_position_pct, bounce_count, rsi14, margin_ratio, atr_pct, ma25_gap_pct)

    parts.append(f'<text x="{pad_l}" y="{height-10}" fill="{AXIS_TEXT}" font-family="ui-monospace, monospace" font-size="10">{_esc(trade_date[0])}</text>')
    parts.append(
        f'<text x="{width-pad_r}" y="{height-10}" fill="{AXIS_TEXT}" font-family="ui-monospace, monospace" '
        f'font-size="10" text-anchor="end">{_esc(trade_date[-1])}</text>'
    )

    legend = [("終値", CLOSE), ("MA5", MA5[0]), ("MA25", MA25[0]), ("MA75", MA75[0]), ("現値", CURRENT)]
    cx = width / 2 - 110
    for label, color in legend:
        parts.append(f'<rect x="{cx-12}" y="{height-17}" width="8" height="2" fill="{color}"/>')
        parts.append(f'<text x="{cx}" y="{height-10}" fill="{TEXT_SUB}" font-family="system-ui, sans-serif" font-size="10">{_esc(label)}</text>')
        cx += 50

    body = "".join(parts)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{_esc(code)} {_esc(name)} chart">{body}</svg>'
    )


def _info_box(
    parts: list[str],
    width: int,
    box_score: float | None,
    box_position_pct: float | None,
    bounce_count: int | None,
    rsi14: float | None,
    margin_ratio: float | None,
    atr_pct: float | None,
    ma25_gap_pct: float | None,
) -> None:
    x, y, w, h = width - 154, 30, 132, 110
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="rgba(10,12,16,0.78)" stroke="{BORDER}"/>')
    rows = [
        ("Score", _fmt(box_score, 0, "")),
        ("下限距離", _fmt(box_position_pct, 0, "%")),
        ("反発", "-" if bounce_count is None else f"{bounce_count}回"),
        ("RSI", _fmt(rsi14, 1, "")),
        ("信用", _fmt(margin_ratio, 2, "倍")),
        ("ATR", _fmt(atr_pct, 1, "%")),
        ("MA25乖離", _fmt_signed(ma25_gap_pct, 1, "%")),
    ]
    for i, (label, value) in enumerate(rows):
        yy = y + 17 + i * 13
        parts.append(f'<text x="{x+10}" y="{yy}" fill="{TEXT_SUB}" font-family="system-ui, sans-serif" font-size="10">{_esc(label)}</text>')
        parts.append(f'<text x="{x+w-10}" y="{yy}" fill="{TEXT_TITLE}" font-family="ui-monospace, monospace" font-size="10" text-anchor="end">{_esc(value)}</text>')


def _empty_svg(width: int, height: int, message: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">'
        f'<rect width="{width}" height="{height}" fill="{BG}"/>'
        f'<rect x="1" y="1" width="{width-2}" height="{height-2}" rx="10" fill="none" stroke="{BORDER}"/>'
        f'<text x="{width/2:.1f}" y="{height/2:.1f}" fill="{AXIS_TEXT}" font-family="system-ui, sans-serif" '
        f'font-size="14" text-anchor="middle">{_esc(message)}</text></svg>'
    )


def render_card_chart(
    *,
    close: Sequence[float],
    box_high: float,
    box_low: float,
    entry_min: float,
    entry_max: float,
    current_price: float,
    width: int = 220,
    height: int = 82,
) -> str:
    pad = 6
    pw, ph = width - pad * 2, height - pad * 2
    close = [float(v) for v in close if v is not None]
    if not close:
        return _empty_svg(width, height, "no data")

    all_y = list(close) + [box_high, box_low, entry_min, entry_max, current_price]
    y_of, _, _ = _yscale(all_y, pad, ph)
    x_of = _xscale(len(close), pad, pw)

    parts: list[str] = []
    parts.append(f'<rect width="{width}" height="{height}" fill="{BG}"/>')
    parts.append(f'<rect x="0.5" y="0.5" width="{width-1}" height="{height-1}" rx="6" fill="none" stroke="{BORDER}"/>')

    y_bh, y_bl = y_of(box_high), y_of(box_low)
    parts.append(f'<rect x="{pad}" y="{y_bh:.1f}" width="{pw}" height="{max(0, y_bl-y_bh):.1f}" fill="{BOX_FILL}"/>')
    for y in (y_bh, y_bl):
        parts.append(f'<line x1="{pad}" y1="{y:.1f}" x2="{width-pad}" y2="{y:.1f}" stroke="{BOX_EDGE}" stroke-width="0.9" stroke-dasharray="3 3"/>')

    y_eh, y_el = y_of(entry_max), y_of(entry_min)
    parts.append(f'<rect x="{pad}" y="{y_eh:.1f}" width="{pw}" height="{max(1, y_el-y_eh):.1f}" fill="{ENTRY_FILL}"/>')
    for y in (y_eh, y_el):
        parts.append(f'<line x1="{pad}" y1="{y:.1f}" x2="{width-pad}" y2="{y:.1f}" stroke="{ENTRY_EDGE}" stroke-width="0.7"/>')

    parts.append(
        f'<polyline points="{_poly(close, x_of, y_of)}" fill="none" stroke="{CLOSE}" '
        f'stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"/>'
    )
    x_last = x_of(len(close) - 1)
    y_cur = y_of(current_price)
    parts.append(f'<circle cx="{x_last:.1f}" cy="{y_cur:.1f}" r="2.4" fill="{CURRENT}" stroke="{BG}" stroke-width="1"/>')

    body = "".join(parts)
    return f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" style="display:block">{body}</svg>'
