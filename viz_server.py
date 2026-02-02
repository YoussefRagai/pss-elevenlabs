from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import base64
import io

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from mplsoccer import Pitch, VerticalPitch, Radar, PyPizza, Bumpy
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib import font_manager
from matplotlib import image as mpimg
from pathlib import Path


app = FastAPI()

ASSETS_DIR = Path(__file__).resolve().parent / "assets" / "brand"
LOGO_NAVY = ASSETS_DIR / "logos" / "PSS_Logo_Navy.png"
ARCHIVO_REGULAR = ASSETS_DIR / "fonts" / "Archivo-Regular.ttf"
ARCHIVO_BOLD = ASSETS_DIR / "fonts" / "Archivo-Bold.ttf"

def register_fonts():
    for font_path in (ARCHIVO_REGULAR, ARCHIVO_BOLD):
        if font_path.exists():
            font_manager.fontManager.addfont(str(font_path))

register_fonts()


@app.get("/health")
def health():
    return {"status": "ok"}

PSS_COLORS = ["#2E7D6D", "#003C71", "#FFD000", "#1F2E3D"]
SHOT_SHAPES = {
    "penalty": "s",
    "shoot": "o",
    "shoot location": "o",
    "shot": "o",
    "header": "^",
    "free kick": "D",
}

def pick_series_color(idx, current):
    if current and current not in {"#6ae0c3", "#f5b861"}:
        return current
    return PSS_COLORS[idx % len(PSS_COLORS)]

def get_shot_type(row):
    for key in ("shot_type", "event_type", "event_name"):
        value = row.get(key)
        if value:
            return str(value).strip().lower()
    return "shot"

def get_marker_override(rules, target):
    if not rules:
        return None
    for rule in rules:
        if str(rule.get("target", "")).lower() == target:
            return rule.get("marker")
    return None

def get_highlight_color(rules, rule_type):
    if not rules:
        return None
    for rule in rules:
        if str(rule.get("type", "")).lower() == rule_type:
            return rule.get("color")
    return None

def in_penalty_area(x_val, y_val):
    if x_val is None or y_val is None:
        return False
    depth = 17.0
    width = 64.7
    y_min = (100 - width) / 2
    y_max = y_min + width
    return (x_val <= depth or x_val >= 100 - depth) and (y_val >= y_min and y_val <= y_max)


class RenderRequest(BaseModel):
    chart_type: str
    title: Optional[str] = None
    subtitle: Optional[str] = None
    x_field: Optional[str] = "x"
    y_field: Optional[str] = "y"
    end_x_field: Optional[str] = "end_x"
    end_y_field: Optional[str] = "end_y"
    data: List[dict]
    orientation: Optional[str] = "horizontal"
    half: Optional[bool] = False
    metrics: Optional[List[str]] = None
    values: Optional[List[float]] = None
    values_compare: Optional[List[float]] = None
    series: Optional[List[dict]] = None
    series_label: Optional[str] = None
    marker_rules: Optional[List[dict]] = None
    highlight_rules: Optional[List[dict]] = None


def get_series_list(payload: RenderRequest):
    if payload.series and isinstance(payload.series, list):
        return payload.series
    return None


def fig_to_base64(fig):
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=180, bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    return base64.b64encode(buffer.read()).decode("utf-8")

def add_brand_stamp(fig):
    if LOGO_NAVY.exists():
        logo = mpimg.imread(str(LOGO_NAVY))
        ax_logo = fig.add_axes([0.04, 0.005, 0.34, 0.16])
        ax_logo.imshow(logo)
        ax_logo.axis("off")
    else:
        fig.text(
            0.02,
            0.08,
            "PSS",
            ha="left",
            va="top",
            fontsize=12,
            fontweight="bold",
            color="#003C71",
            fontfamily="Archivo",
        )
        fig.text(
            0.02,
            0.05,
            "Smarter football decisions",
            ha="left",
            va="top",
            fontsize=8.5,
            color="#1F2E3D",
            fontfamily="Archivo",
        )

def place_bottom_legend(fig, handles, ncol=1):
    if not handles:
        return
    fig.legend(
        handles=handles,
        loc="lower right",
        bbox_to_anchor=(0.98, 0.005),
        frameon=False,
        ncol=ncol,
        labelcolor="#1F2E3D",
    )

@app.post("/render")
def render_chart(payload: RenderRequest):
    if payload.chart_type.lower() not in {"radar", "pizza", "bumpy"}:
        if not payload.data and not payload.series:
            raise HTTPException(status_code=400, detail="No data provided.")

    chart_type = payload.chart_type.lower()
    orientation = (payload.orientation or "horizontal").lower()
    half = bool(payload.half)

    x_field = payload.x_field or "x"
    y_field = payload.y_field or "y"
    end_x_field = payload.end_x_field or "end_x"
    end_y_field = payload.end_y_field or "end_y"

    if chart_type in {"shot_map", "pass_map", "heatmap", "pitch_plot", "pass_network"}:
        pitch_kwargs = {
            "pitch_type": "custom",
            "pitch_length": 100,
            "pitch_width": 100,
            "line_color": "#FFFFFF",
            "line_zorder": 2,
            "half": half,
        }
        pitch = VerticalPitch(**pitch_kwargs) if orientation == "vertical" else Pitch(**pitch_kwargs)
        fig, ax = pitch.draw(figsize=(8.4, 5.8))
        fig.set_facecolor("#ECECEC")
        ax.set_facecolor("#ECECEC")

        series_list = get_series_list(payload)
        if not series_list:
            series_list = [
                {
                    "label": "Series",
                    "color": PSS_COLORS[0],
                    "data": payload.data,
                }
            ]
        for idx, series in enumerate(series_list):
            series["color"] = pick_series_color(idx, series.get("color"))
        shot_marker_override = get_marker_override(payload.marker_rules, "shot")
        pass_marker_override = get_marker_override(payload.marker_rules, "pass")
        penalty_highlight = get_highlight_color(payload.highlight_rules, "penalty_area")

        if chart_type == "heatmap":
            for idx, series in enumerate(series_list):
                data = series.get("data", [])
                xs = [row.get(x_field) for row in data if row.get(x_field) is not None]
                ys = [row.get(y_field) for row in data if row.get(y_field) is not None]
                if not xs or not ys:
                    continue
                color = series.get("color", PSS_COLORS[idx % len(PSS_COLORS)])
                cmap = LinearSegmentedColormap.from_list(
                    f"series_{idx}", [(0, 0, 0, 0), color]
                )
                stats = pitch.bin_statistic(xs, ys, statistic="count", bins=(24, 16))
                pitch.heatmap(stats, ax=ax, cmap=cmap, alpha=0.65)
        elif chart_type == "pass_network":
            for idx, series in enumerate(series_list):
                data = series.get("data", [])
                from_x = [row.get("from_x") for row in data if row.get("from_x") is not None]
                from_y = [row.get("from_y") for row in data if row.get("from_y") is not None]
                to_x = [row.get("to_x") for row in data if row.get("to_x") is not None]
                to_y = [row.get("to_y") for row in data if row.get("to_y") is not None]
                weights = [row.get("count", 1) for row in data if row.get("from_x") is not None]
                if not (from_x and from_y and to_x and to_y):
                    continue
                widths = np.clip(np.array(weights), 1, None) / max(np.max(weights), 1) * 6
                color = series.get("color", PSS_COLORS[idx % len(PSS_COLORS)])
                for i in range(len(from_x)):
                    pitch.lines(
                        from_x[i],
                        from_y[i],
                        to_x[i],
                        to_y[i],
                        ax=ax,
                        color=color,
                        linewidth=widths[i],
                        alpha=0.7,
                    )
                pitch.scatter(from_x, from_y, ax=ax, color=color, s=60, alpha=0.9)
        else:
            for idx, series in enumerate(series_list):
                data = series.get("data", [])
                xs = [row.get(x_field) for row in data if row.get(x_field) is not None]
                ys = [row.get(y_field) for row in data if row.get(y_field) is not None]
                if not xs or not ys:
                    continue
                color = series.get("color", PSS_COLORS[idx % len(PSS_COLORS)])
                if chart_type == "pass_map":
                    exs = [row.get(end_x_field) for row in data if row.get(end_x_field) is not None]
                    eys = [row.get(end_y_field) for row in data if row.get(end_y_field) is not None]
                    if xs and ys and exs and eys:
                        pitch.arrows(xs, ys, exs, eys, ax=ax, color=color, alpha=0.7, width=2)
                    marker = pass_marker_override or "o"
                    if penalty_highlight:
                        in_x, in_y, out_x, out_y = [], [], [], []
                        for row in data:
                            xv = row.get(x_field)
                            yv = row.get(y_field)
                            if xv is None or yv is None:
                                continue
                            if in_penalty_area(xv, yv):
                                in_x.append(xv)
                                in_y.append(yv)
                            else:
                                out_x.append(xv)
                                out_y.append(yv)
                        if out_x:
                            pitch.scatter(out_x, out_y, ax=ax, color=color, s=30, alpha=0.7, marker=marker)
                        if in_x:
                            pitch.scatter(in_x, in_y, ax=ax, color=penalty_highlight, s=30, alpha=0.9, marker=marker)
                    else:
                        pitch.scatter(xs, ys, ax=ax, color=color, s=30, alpha=0.7, marker=marker)
                elif chart_type == "pitch_plot":
                    exs = [row.get(end_x_field) for row in data if row.get(end_x_field) is not None]
                    eys = [row.get(end_y_field) for row in data if row.get(end_y_field) is not None]
                    marker = pass_marker_override or "o"
                    if penalty_highlight:
                        in_x, in_y, out_x, out_y = [], [], [], []
                        for row in data:
                            xv = row.get(x_field)
                            yv = row.get(y_field)
                            if xv is None or yv is None:
                                continue
                            if in_penalty_area(xv, yv):
                                in_x.append(xv)
                                in_y.append(yv)
                            else:
                                out_x.append(xv)
                                out_y.append(yv)
                        if out_x:
                            pitch.scatter(out_x, out_y, ax=ax, color=color, s=30, alpha=0.8, marker=marker)
                        if in_x:
                            pitch.scatter(in_x, in_y, ax=ax, color=penalty_highlight, s=30, alpha=0.9, marker=marker)
                    else:
                        pitch.scatter(xs, ys, ax=ax, color=color, s=30, alpha=0.8, marker=marker)
                    if exs and eys:
                        pitch.arrows(xs, ys, exs, eys, ax=ax, color=color, alpha=0.7, width=2)
                else:  # shot_map
                    grouped = {}
                    for row in data:
                        shot_key = get_shot_type(row)
                        grouped.setdefault(shot_key, []).append(row)
                    for shot_key, rows in grouped.items():
                        sxs = [row.get(x_field) for row in rows if row.get(x_field) is not None]
                        sys = [row.get(y_field) for row in rows if row.get(y_field) is not None]
                        if not sxs or not sys:
                            continue
                        marker = shot_marker_override or SHOT_SHAPES.get(shot_key, "o")
                        if penalty_highlight:
                            in_x, in_y, out_x, out_y = [], [], [], []
                            for row in rows:
                                xv = row.get(x_field)
                                yv = row.get(y_field)
                                if xv is None or yv is None:
                                    continue
                                if in_penalty_area(xv, yv):
                                    in_x.append(xv)
                                    in_y.append(yv)
                                else:
                                    out_x.append(xv)
                                    out_y.append(yv)
                            if out_x:
                                pitch.scatter(
                                    out_x,
                                    out_y,
                                    ax=ax,
                                    color=color,
                                    s=40,
                                    alpha=0.85,
                                    edgecolors="black",
                                    marker=marker,
                                )
                            if in_x:
                                pitch.scatter(
                                    in_x,
                                    in_y,
                                    ax=ax,
                                    color=penalty_highlight,
                                    s=40,
                                    alpha=0.9,
                                    edgecolors="black",
                                    marker=marker,
                                )
                        else:
                            pitch.scatter(
                                sxs,
                                sys,
                                ax=ax,
                                color=color,
                                s=40,
                                alpha=0.85,
                                edgecolors="black",
                                marker=marker,
                            )
        if series_list:
            legend_handles = []
            for idx, series in enumerate(series_list):
                label = series.get("label", "Series")
                color = series.get("color", PSS_COLORS[idx % len(PSS_COLORS)])
                if chart_type == "shot_map":
                    if shot_marker_override:
                        legend_handles.append(
                            Line2D(
                                [0],
                                [0],
                                marker=shot_marker_override,
                                color="none",
                                markerfacecolor=color,
                                markeredgecolor="black",
                                markersize=8,
                                label=f"{label} • Shots",
                            )
                        )
                    else:
                        data = series.get("data", [])
                        types = []
                        for row in data:
                            shot_key = get_shot_type(row)
                            if shot_key not in types:
                                types.append(shot_key)
                        if not types:
                            legend_handles.append(Patch(color=color, label=label))
                        else:
                            for shot_key in types:
                                marker = SHOT_SHAPES.get(shot_key, "o")
                                legend_handles.append(
                                    Line2D(
                                        [0],
                                        [0],
                                        marker=marker,
                                        color="none",
                                        markerfacecolor=color,
                                        markeredgecolor="black",
                                        markersize=8,
                                        label=f"{label} • {shot_key.title()}",
                                    )
                                )
                else:
                    if chart_type in {"pass_map", "pitch_plot"} and pass_marker_override:
                        legend_handles.append(
                            Line2D(
                                [0],
                                [0],
                                marker=pass_marker_override,
                                color="none",
                                markerfacecolor=color,
                                markeredgecolor="black",
                                markersize=8,
                                label=f"{label} • Passes",
                            )
                        )
                    else:
                        legend_handles.append(Patch(color=color, label=label))
            if penalty_highlight:
                legend_handles.append(Patch(color=penalty_highlight, label="Penalty area"))
            fig.subplots_adjust(bottom=0.28)
            place_bottom_legend(fig, legend_handles, ncol=1)
    elif chart_type == "radar":
        if not payload.metrics or not payload.values:
            raise HTTPException(status_code=400, detail="Radar requires metrics and values.")
        radar = Radar(
            params=payload.metrics,
            min_range=[0] * len(payload.metrics),
            max_range=[max(payload.values) or 1] * len(payload.metrics),
        )
        fig, ax = radar.setup_axis()
        fig.set_facecolor("#FFFFFF")
        radar.draw_circles(ax=ax, facecolor="#FFFFFF", edgecolor="#ECECEC")
        radar.draw_radar(payload.values, ax=ax, kwargs_radar={"color": PSS_COLORS[1]})
        if payload.values_compare:
            radar.draw_radar(
                payload.values_compare, ax=ax, kwargs_radar={"color": PSS_COLORS[2], "alpha": 0.6}
            )
            fig.subplots_adjust(bottom=0.28)
            place_bottom_legend(
                fig,
                [
                    Patch(color=PSS_COLORS[1], label="Series A"),
                    Patch(color=PSS_COLORS[2], label="Series B"),
                ],
                ncol=1,
            )
        radar.draw_range_labels(ax=ax, fontsize=8, color="#1F2E3D")
        radar.draw_param_labels(ax=ax, fontsize=9, color="#003C71")
    elif chart_type == "pizza":
        if not payload.metrics or not payload.values:
            raise HTTPException(status_code=400, detail="Pizza requires metrics and values.")
        pizza = PyPizza(
            params=payload.metrics,
            background_color="#FFFFFF",
            straight_line_color="#ECECEC",
            last_circle_color="#ECECEC",
            other_circle_color="#ECECEC",
        )
        color_list = ["#FFFFFF"] * len(payload.values)
        fig, ax = pizza.make_pizza(
            payload.values,
            figsize=(6.8, 6.8),
            color_blank_space=color_list,
            slice_colors=["#2E7D6D"] * len(payload.values),
            value_colors=["#1F2E3D"] * len(payload.values),
            param_location=110,
        )
        fig.set_facecolor("#FFFFFF")
    elif chart_type == "bumpy":
        if not payload.metrics or not payload.series:
            raise HTTPException(status_code=400, detail="Bumpy requires metrics and series.")
        bumpy = Bumpy(
            background_color="#FFFFFF",
            scatter_color=PSS_COLORS[2],
            label_color="#1F2E3D",
        )
        series_values = {
            s.get("label", "Series"): s.get("values", []) for s in payload.series
        }
        max_rank = 0
        for values in series_values.values():
            for value in values:
                if value is not None and value > max_rank:
                    max_rank = value
        fill_value = max_rank + 1 if max_rank else 1
        for label, values in series_values.items():
            series_values[label] = [fill_value if v is None else v for v in values]
        y_list = list(range(1, fill_value + 1))
        fig, ax = bumpy.plot(
            x_list=payload.metrics,
            y_list=y_list,
            values=series_values,
            highlight_dict={},
            x_label="",
            y_label="",
        )
        fig.set_facecolor("#FFFFFF")
        if payload.series:
            legend_handles = [
                Patch(color=PSS_COLORS[i % len(PSS_COLORS)], label=s.get("label", f"Series {i+1}"))
                for i, s in enumerate(payload.series)
            ]
            fig.subplots_adjust(bottom=0.28)
            place_bottom_legend(fig, legend_handles, ncol=1)
    else:
        raise HTTPException(status_code=400, detail="Unsupported chart type.")

    if payload.title:
        fig.suptitle(payload.title, color="#003C71", fontsize=14, fontweight="bold", fontfamily="Archivo")
    if payload.subtitle:
        ax.set_title(payload.subtitle, color="#1F2E3D", fontsize=10, pad=8, fontfamily="Archivo")
    fig.subplots_adjust(bottom=0.28)
    add_brand_stamp(fig)

    img_base64 = fig_to_base64(fig)
    return {"image_base64": img_base64, "mime": "image/png"}
