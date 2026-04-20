"""
app.py -- SAR Pair Viewer

    uvicorn app:app --port 7861 --reload
"""

import io, base64, textwrap
import numpy as np
import matplotlib.pyplot as plt
import folium
from PIL import Image
from shapely.ops import unary_union
from shapely import contains_xy
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from typing import Optional

from db import get_pairs, PairRecord
from geo_utils import (
    get_gcps_for_folder, build_tps_transformer,
    image_footprint, compute_overlap, warp_to_geo_grid,
)

RESOLUTION = 2048
app = FastAPI()

# Pairs loaded at startup with no filter applied
ALL_PAIRS = get_pairs()
PAIR_MAP  = {p.label: p for p in ALL_PAIRS}


# ---------------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------------

def _to_png_b64(rgba: np.ndarray) -> str:
    buf = io.BytesIO()
    Image.fromarray(rgba, mode="RGBA").save(buf, format="PNG")
    buf.seek(0)
    return "data:image/png;base64," + base64.b64encode(buf.read()).decode()


def grid_to_png_b64(grid: np.ndarray, alpha=0.85) -> str:
    cm = plt.get_cmap("gray")
    rgba = (cm(np.nan_to_num(grid, nan=0.0)) * 255).astype(np.uint8)
    nan_mask = np.isnan(grid)
    rgba[nan_mask] = 0
    rgba[~nan_mask, 3] = int(alpha * 255)
    return _to_png_b64(rgba)


def add_overlay(m, img_b64, lat_min, lat_max, lon_min, lon_max):
    folium.raster_layers.ImageOverlay(
        image=img_b64,
        bounds=[[lat_min, lon_min], [lat_max, lon_max]],
        opacity=1.0,
        zindex=1,
    ).add_to(m)


def base_map(lat, lon, zoom=6):
    return folium.Map(location=[lat, lon], zoom_start=zoom, tiles="CartoDB positron")


def mask_to_polygon(grid, lon_vec, lat_vec, polygon):
    lon_grid, lat_grid = np.meshgrid(lon_vec, lat_vec)
    inside = contains_xy(polygon, lon_grid.ravel(), lat_grid.ravel()).reshape(grid.shape)
    out = grid.copy()
    out[~inside] = np.nan
    return out


# ---------------------------------------------------------------------------
# Two-map page
# ---------------------------------------------------------------------------

TWO_MAP_CSS = textwrap.dedent("""\
    <style>
      * { box-sizing: border-box; margin: 0; padding: 0; }
      html, body { height: 100%; overflow: hidden; font-family: sans-serif;
                   background: #111; display: flex; flex-direction: column; }
      #toolbar { display: flex; align-items: center; gap: 10px; padding: 6px 12px;
                 background: #1f2937; flex-shrink: 0; border-bottom: 1px solid #374151; }
      #sync-btn { background: #374151; color: #f9fafb; border: none; border-radius: 6px;
                  padding: 5px 14px; cursor: pointer; font-size: 13px; }
      #sync-btn.on { background: #2563eb; }
      #labels { display: flex; flex-shrink: 0; }
      .lbl { flex: 1; text-align: center; padding: 5px 10px; font-size: 12px;
             background: #1f2937; color: #9ca3af; overflow: hidden; text-overflow: ellipsis; }
      #maps { display: flex; gap: 4px; flex: 1; min-height: 0; }
      .folium-map { width: 100% !important; height: 100% !important; }
    </style>
""")


def two_map_sync_js(id_a, id_b):
    # Build JS using string concatenation -- no f-strings near JS braces
    return (
        "<script>\n"
        "(function() {\n"
        "  function init(retries) {\n"
        "    var ma  = window['" + id_a + "'];\n"
        "    var mb  = window['" + id_b + "'];\n"
        "    var btn = document.getElementById('sync-btn');\n"
        "    if (!ma || !mb || !btn) {\n"
        "      if (retries > 0) setTimeout(function() { init(retries - 1); }, 100);\n"
        "      else console.error('sync init failed');\n"
        "      return;\n"
        "    }\n"
        "    setTimeout(function() { ma.invalidateSize(); mb.invalidateSize(); }, 100);\n"
        "    var synced = false;\n"
        "    var busy   = false;\n"
        "    function onMoveA() {\n"
        "      if (busy) return; busy = true;\n"
        "      mb.setView(ma.getCenter(), ma.getZoom(), {animate: false});\n"
        "      busy = false;\n"
        "    }\n"
        "    function onMoveB() {\n"
        "      if (busy) return; busy = true;\n"
        "      ma.setView(mb.getCenter(), mb.getZoom(), {animate: false});\n"
        "      busy = false;\n"
        "    }\n"
        "    btn.onclick = function() {\n"
        "      synced = !synced;\n"
        "      btn.textContent = synced ? '\U0001f512 Synced' : '\U0001f513 Sync maps';\n"
        "      btn.classList.toggle('on', synced);\n"
        "      if (synced) {\n"
        "        mb.setView(ma.getCenter(), ma.getZoom(), {animate: false});\n"
        "        ma.on('moveend', onMoveA);\n"
        "        mb.on('moveend', onMoveB);\n"
        "      } else {\n"
        "        ma.off('moveend', onMoveA);\n"
        "        mb.off('moveend', onMoveB);\n"
        "      }\n"
        "    };\n"
        "  }\n"
        "  window.addEventListener('load', function() { init(50); });\n"
        "})();\n"
        "</script>\n"
    )


def two_map_page(m_a, m_b, label_a, label_b):
    id_a = m_a.get_name()
    id_b = m_b.get_name()

    fig = folium.Figure()
    fig.add_child(m_a)
    fig.add_child(m_b)
    doc = fig.render()

    toolbar = (
        '<div id="toolbar">'
        '<button id="sync-btn">\U0001f513 Sync maps</button>'
        '<span style="color:#9ca3af;font-size:12px;">'
        'When synced, releasing a pan/zoom updates the other map'
        '</span></div>'
        '<div id="labels">'
        '<div class="lbl">' + label_a + '</div>'
        '<div class="lbl">' + label_b + '</div>'
        '</div>'
        '<div id="maps">'
    )

    move_js = (
        "<script>\n"
        "window.addEventListener('load', function() {\n"
        "  var c = document.getElementById('maps');\n"
        "  document.querySelectorAll('.folium-map').forEach(function(el) {\n"
        "    c.appendChild(el);\n"
        "  });\n"
        "});\n"
        "</script>\n"
    )

    doc = doc.replace('</head>', TWO_MAP_CSS + '</head>')
    doc = doc.replace('<body>', '<body>' + toolbar)
    doc = doc.replace('</body>', move_js + two_map_sync_js(id_a, id_b) + '</div></body>')
    return doc


# ---------------------------------------------------------------------------
# Map builders
# ---------------------------------------------------------------------------

def build_overlaid(pair):
    path_a, gcps_a = get_gcps_for_folder(pair.folder_before)
    path_b, gcps_b = get_gcps_for_folder(pair.folder_after)
    tf_a = build_tps_transformer(gcps_a)
    tf_b = build_tps_transformer(gcps_b)
    union = unary_union([image_footprint(gcps_a), image_footprint(gcps_b)])

    grid_a, lon_vec, lat_vec = warp_to_geo_grid(path_a, tf_a, union, RESOLUTION)
    grid_b, _, _ = warp_to_geo_grid(path_b, tf_b, union, RESOLUTION)

    a = np.nan_to_num(grid_a, nan=0.0)
    b = np.nan_to_num(grid_b, nan=0.0)
    rgb   = np.stack([a, b, b], axis=-1)
    alpha = np.where(np.isnan(grid_a) & np.isnan(grid_b), 0, int(0.85 * 255)).astype(np.uint8)
    rgba  = np.dstack([(rgb * 255).astype(np.uint8), alpha])

    lon_min, lon_max = float(lon_vec[0]), float(lon_vec[-1])
    lat_min, lat_max = float(lat_vec[-1]), float(lat_vec[0])
    m = base_map((lat_min + lat_max) / 2, (lon_min + lon_max) / 2)
    add_overlay(m, _to_png_b64(rgba), lat_min, lat_max, lon_min, lon_max)
    return m.get_root().render()


def build_overlap(pair):
    path_a, gcps_a = get_gcps_for_folder(pair.folder_before)
    path_b, gcps_b = get_gcps_for_folder(pair.folder_after)
    tf_a = build_tps_transformer(gcps_a)
    tf_b = build_tps_transformer(gcps_b)
    overlap = compute_overlap(image_footprint(gcps_a), image_footprint(gcps_b))

    grid_a, lon_vec, lat_vec = warp_to_geo_grid(path_a, tf_a, overlap, RESOLUTION)
    grid_b, _, _ = warp_to_geo_grid(path_b, tf_b, overlap, RESOLUTION)
    grid_a = mask_to_polygon(grid_a, lon_vec, lat_vec, overlap)
    grid_b = mask_to_polygon(grid_b, lon_vec, lat_vec, overlap)

    lon_min, lon_max = float(lon_vec[0]), float(lon_vec[-1])
    lat_min, lat_max = float(lat_vec[-1]), float(lat_vec[0])
    cx, cy = (lon_min + lon_max) / 2, (lat_min + lat_max) / 2

    m_a, m_b = base_map(cy, cx), base_map(cy, cx)
    add_overlay(m_a, grid_to_png_b64(grid_a), lat_min, lat_max, lon_min, lon_max)
    add_overlay(m_b, grid_to_png_b64(grid_b), lat_min, lat_max, lon_min, lon_max)
    return two_map_page(m_a, m_b,
                        "Before \u2014 " + pair.id_before[:12] + "\u2026",
                        "After \u2014 "  + pair.id_after[:12]  + "\u2026")


def build_side_by_side(pair):
    path_a, gcps_a = get_gcps_for_folder(pair.folder_before)
    path_b, gcps_b = get_gcps_for_folder(pair.folder_after)
    tf_a = build_tps_transformer(gcps_a)
    tf_b = build_tps_transformer(gcps_b)
    fp_a = image_footprint(gcps_a)
    fp_b = image_footprint(gcps_b)
    cx, cy = unary_union([fp_a, fp_b]).centroid.x, unary_union([fp_a, fp_b]).centroid.y

    grid_a, lon_a, lat_a = warp_to_geo_grid(path_a, tf_a, fp_a, RESOLUTION)
    grid_b, lon_b, lat_b = warp_to_geo_grid(path_b, tf_b, fp_b, RESOLUTION)

    m_a, m_b = base_map(cy, cx), base_map(cy, cx)
    add_overlay(m_a, grid_to_png_b64(grid_a),
                float(lat_a[-1]), float(lat_a[0]), float(lon_a[0]), float(lon_a[-1]))
    add_overlay(m_b, grid_to_png_b64(grid_b),
                float(lat_b[-1]), float(lat_b[0]), float(lon_b[0]), float(lon_b[-1]))
    return two_map_page(m_a, m_b,
                        "Before \u2014 " + pair.id_before[:12] + "\u2026",
                        "After \u2014 "  + pair.id_after[:12]  + "\u2026")


BUILDERS = {
    "overlaid":     build_overlaid,
    "overlap":      build_overlap,
    "side_by_side": build_side_by_side,
}


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

def build_pair_options(pairs):
    if not pairs:
        return '<option value="" disabled>No pairs match filter</option>'
    return (
        '<option value="" disabled selected>Select a pair\u2026</option>\n' +
        "\n".join(
            '<option value="' + p.label + '">' + p.label + '</option>'
            for p in pairs
        )
    )


SIDEBAR_HTML = """\
<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>SAR Pair Viewer</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:      #0d1117;
    --panel:   #161b22;
    --border:  #30363d;
    --muted:   #8b949e;
    --text:    #e6edf3;
    --accent:  #58a6ff;
    --accentH: #79b8ff;
    --danger:  #f85149;
    --mono:    'IBM Plex Mono', monospace;
    --sans:    'IBM Plex Sans', sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { display: flex; height: 100vh; overflow: hidden;
         background: var(--bg); color: var(--text);
         font-family: var(--sans); font-size: 13px; }

  #sidebar { width: 240px; min-width: 240px; flex-shrink: 0;
             background: var(--panel); border-right: 1px solid var(--border);
             display: flex; flex-direction: column; overflow: hidden;
             transition: width 0.2s ease, min-width 0.2s ease; }
  #sidebar.collapsed { width: 0; min-width: 0; }
  #sidebar-inner { width: 240px; display: flex; flex-direction: column;
                   overflow-y: auto; height: 100%; }

  .sb-header { padding: 14px 14px 10px; border-bottom: 1px solid var(--border);
               font-family: var(--mono); font-size: 11px; font-weight: 500;
               color: var(--muted); letter-spacing: 0.1em; text-transform: uppercase; }
  .sb-section { padding: 12px 14px; border-bottom: 1px solid var(--border);
                display: flex; flex-direction: column; gap: 8px; }
  .sb-label { font-family: var(--mono); font-size: 10px; font-weight: 500;
              color: var(--muted); letter-spacing: 0.08em; text-transform: uppercase;
              display: flex; align-items: center; justify-content: space-between; }
  .sb-toggle { background: none; border: none; color: var(--muted);
               cursor: pointer; font-size: 10px; font-family: var(--mono);
               padding: 0; letter-spacing: 0.05em; }
  .sb-toggle:hover { color: var(--text); }
  .sb-collapsible { display: flex; flex-direction: column; gap: 8px; }
  .sb-collapsible.hidden { display: none; }

  .coord-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
  .coord-field { display: flex; flex-direction: column; gap: 3px; }
  .coord-field label { font-family: var(--mono); font-size: 9px; color: var(--muted);
                       text-transform: uppercase; letter-spacing: 0.06em; }
  .coord-field input { background: var(--bg); color: var(--text);
                       border: 1px solid var(--border); border-radius: 4px;
                       padding: 5px 7px; font-family: var(--mono); font-size: 12px;
                       width: 100%; }
  .coord-field input:focus { outline: none; border-color: var(--accent); }
  .coord-field input::placeholder { color: var(--muted); }

  .filter-actions { display: flex; gap: 6px; }
  .filter-btn { flex: 1; background: var(--border); color: var(--text);
                border: none; border-radius: 4px; padding: 6px;
                font-family: var(--mono); font-size: 11px; cursor: pointer; }
  .filter-btn:hover { background: var(--muted); color: var(--bg); }
  .filter-btn.primary { background: var(--accent); color: var(--bg); }
  .filter-btn.primary:hover { background: var(--accentH); }

  #pair-count { font-family: var(--mono); font-size: 10px; color: var(--muted); }

  #pair-select { width: 100%; background: var(--bg); color: var(--text);
                 border: 1px solid var(--border); border-radius: 4px;
                 padding: 7px 28px 7px 10px; font-family: var(--mono); font-size: 12px;
                 appearance: none; cursor: pointer;
                 background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%238b949e' stroke-width='2'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
                 background-repeat: no-repeat; background-position: right 8px center; }
  #pair-select:focus { outline: none; border-color: var(--accent); }

  .mode-opt { display: flex; align-items: center; gap: 8px;
              padding: 7px 10px; border-radius: 4px; cursor: pointer;
              border: 1px solid transparent;
              transition: border-color 0.15s, background 0.15s;
              color: var(--muted); line-height: 1.3; }
  .mode-opt:hover { background: rgba(88,166,255,0.06); color: var(--text); }
  .mode-opt input[type=radio] { accent-color: var(--accent); flex-shrink: 0; margin: 0; }
  .mode-opt input[type=radio]:checked + span { color: var(--accent); }
  .mode-opt:has(input:checked) { border-color: rgba(88,166,255,0.3);
                                  background: rgba(88,166,255,0.06); }

  .sb-actions { padding: 12px 14px; display: flex; flex-direction: column; gap: 8px; }
  #render-btn { padding: 9px; background: var(--accent); color: #0d1117;
                border: none; border-radius: 4px; font-family: var(--mono);
                font-size: 12px; font-weight: 500; letter-spacing: 0.05em;
                cursor: pointer; transition: background 0.15s; text-transform: uppercase; }
  #render-btn:hover { background: var(--accentH); }
  #render-btn:disabled { background: var(--border); color: var(--muted); cursor: not-allowed; }
  #render-all-btn { padding: 9px; background: transparent; color: var(--muted);
                    border: 1px solid var(--border); border-radius: 4px;
                    font-family: var(--mono); font-size: 12px; letter-spacing: 0.05em;
                    cursor: pointer; transition: all 0.15s; text-transform: uppercase; }
  #render-all-btn:hover { border-color: var(--accent); color: var(--accent); }

  #toggle { width: 22px; flex-shrink: 0; background: var(--panel);
            border: none; border-right: 1px solid var(--border);
            color: var(--muted); cursor: pointer; font-size: 10px;
            transition: background 0.15s, color 0.15s; writing-mode: vertical-rl; }
  #toggle:hover { background: var(--border); color: var(--text); }

  #map-wrap { flex: 1; position: relative; overflow: hidden; }
  #map-frame { width: 100%; height: 100%; border: none; display: block; }

  #loading-overlay { display: flex; position: absolute; inset: 0;
                     background: var(--bg); z-index: 10;
                     flex-direction: column; align-items: center;
                     justify-content: center; gap: 16px; }
  #loading-overlay.hidden { display: none; }
  .spinner { width: 36px; height: 36px; border: 3px solid var(--border);
             border-top-color: var(--accent); border-radius: 50%;
             animation: spin 0.8s linear infinite; display: none; }
  #loading-overlay.spinning .spinner { display: block; }
  .loading-text { font-family: var(--mono); font-size: 12px;
                  color: var(--muted); letter-spacing: 0.1em; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head><body>

<div id="sidebar">
  <div id="sidebar-inner">
    <div class="sb-header">SAR Pair Viewer</div>

    <!-- Coordinate filter -->
    <div class="sb-section">
      <div class="sb-label">
        <span>Coordinate Filter</span>
        <button class="sb-toggle" id="filter-toggle">SHOW</button>
      </div>
      <div class="sb-collapsible hidden" id="filter-body">
        <div class="coord-grid">
          <div class="coord-field">
            <label>Lon min</label>
            <input type="number" id="f-lon-min" placeholder="-180" step="any">
          </div>
          <div class="coord-field">
            <label>Lon max</label>
            <input type="number" id="f-lon-max" placeholder="180" step="any">
          </div>
          <div class="coord-field">
            <label>Lat min</label>
            <input type="number" id="f-lat-min" placeholder="-90" step="any">
          </div>
          <div class="coord-field">
            <label>Lat max</label>
            <input type="number" id="f-lat-max" placeholder="90" step="any">
          </div>
        </div>
        <div class="filter-actions">
          <button class="filter-btn primary" id="filter-apply-btn">Apply</button>
          <button class="filter-btn" id="filter-clear-btn">Clear</button>
        </div>
      </div>
    </div>

    <!-- Pair selector -->
    <div class="sb-section">
      <div class="sb-label">
        <span>Pair</span>
        <span id="pair-count">PAIR_COUNT available</span>
      </div>
      <select id="pair-select">
        PAIR_OPTIONS
      </select>
    </div>

    <!-- View mode -->
    <div class="sb-section">
      <div class="sb-label"><span>View mode</span></div>
      MODE_RADIOS
    </div>

    <!-- Actions -->
    <div class="sb-actions">
      <button id="render-btn">Render</button>
      <button id="render-all-btn">Render All Pairs</button>
    </div>
  </div>
</div>

<button id="toggle" title="Toggle sidebar">&#9664;</button>

<div id="map-wrap">
  <iframe id="map-frame" src="about:blank"></iframe>
  <div id="loading-overlay">
    <div class="spinner"></div>
    <div class="loading-text" id="loading-text">Select a pair and press Render</div>
  </div>
</div>

<script>
  var sidebar     = document.getElementById('sidebar');
  var toggle      = document.getElementById('toggle');
  var frame       = document.getElementById('map-frame');
  var overlay     = document.getElementById('loading-overlay');
  var btn         = document.getElementById('render-btn');
  var pairSelect  = document.getElementById('pair-select');
  var loadingText = document.getElementById('loading-text');

  toggle.addEventListener('click', function() {
    sidebar.classList.toggle('collapsed');
    toggle.innerHTML = sidebar.classList.contains('collapsed') ? '&#9654;' : '&#9664;';
  });

  // Filter toggle
  document.getElementById('filter-toggle').addEventListener('click', function() {
    var body = document.getElementById('filter-body');
    var hidden = body.classList.toggle('hidden');
    this.textContent = hidden ? 'SHOW' : 'HIDE';
  });

  function getFilterParams() {
    var p = new URLSearchParams();
    var lonMin = document.getElementById('f-lon-min').value;
    var lonMax = document.getElementById('f-lon-max').value;
    var latMin = document.getElementById('f-lat-min').value;
    var latMax = document.getElementById('f-lat-max').value;
    if (lonMin !== '') p.set('lon_min', lonMin);
    if (lonMax !== '') p.set('lon_max', lonMax);
    if (latMin !== '') p.set('lat_min', latMin);
    if (latMax !== '') p.set('lat_max', latMax);
    return p;
  }

  function applyFilter() {
    var p = getFilterParams();
    fetch('/pairs?' + p.toString())
      .then(function(r) { return r.json(); })
      .then(function(data) {
        document.getElementById('pair-count').textContent = data.pairs.length + ' available';
        var sel = document.getElementById('pair-select');
        sel.innerHTML = '';
        if (data.pairs.length === 0) {
          sel.innerHTML = '<option value="" disabled selected>No pairs match filter</option>';
        } else {
          var placeholder = document.createElement('option');
          placeholder.value = ''; placeholder.disabled = true; placeholder.selected = true;
          placeholder.textContent = 'Select a pair\u2026';
          sel.appendChild(placeholder);
          data.pairs.forEach(function(label) {
            var opt = document.createElement('option');
            opt.value = label; opt.textContent = label;
            sel.appendChild(opt);
          });
        }
      });
  }

  document.getElementById('filter-apply-btn').addEventListener('click', applyFilter);

  document.getElementById('filter-clear-btn').addEventListener('click', function() {
    ['f-lon-min','f-lon-max','f-lat-min','f-lat-max'].forEach(function(id) {
      document.getElementById(id).value = '';
    });
    applyFilter();
  });

  function loadMap(url) {
    overlay.classList.remove('hidden');
    overlay.classList.add('spinning');
    loadingText.style.color = '';
    loadingText.textContent = 'Rendering\u2026';
    btn.disabled = true;
    btn.textContent = 'Rendering\u2026';

    frame.onload = function() {
      if (!frame.src || frame.src === 'about:blank') return;
      overlay.classList.add('hidden');
      overlay.classList.remove('spinning');
      btn.disabled = false;
      btn.textContent = 'Render';
      frame.onload = null;
    };
    frame.src = url;
  }

  pairSelect.addEventListener('change', function() {
    if (this.value) loadingText.textContent = 'Press Render to load';
  });

  btn.addEventListener('click', function() {
    var pair = pairSelect.value;
    if (!pair) {
      loadingText.textContent = 'Please select a pair first';
      loadingText.style.color = '#f85149';
      setTimeout(function() {
        loadingText.style.color = '';
        loadingText.textContent = 'Select a pair and press Render';
      }, 2000);
      return;
    }
    var mode = document.querySelector('input[name="mode"]:checked').value;
    var p = getFilterParams();
    p.set('pair', pair);
    p.set('mode', mode);
    loadMap('/map?' + p.toString());
  });

  document.getElementById('render-all-btn').addEventListener('click', function() {
    var mode = document.querySelector('input[name="mode"]:checked').value;
    var p = getFilterParams();
    p.set('mode', mode);
    loadMap('/map/all?' + p.toString());
  });
</script>
</body></html>
"""


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    mode_labels = [
        ("overlaid",     "Overlaid \u2014 red/cyan composite"),
        ("overlap",      "Overlap only \u2014 side by side"),
        ("side_by_side", "Full images \u2014 side by side"),
    ]
    mode_radios = "\n".join(
        '<label class="mode-opt">'
        '<input type="radio" name="mode" value="' + k + '"' + (' checked' if i == 0 else '') + '>'
        '<span>' + v + '</span></label>'
        for i, (k, v) in enumerate(mode_labels)
    )
    html = (SIDEBAR_HTML
            .replace("PAIR_OPTIONS", build_pair_options(ALL_PAIRS))
            .replace("PAIR_COUNT", str(len(ALL_PAIRS)))
            .replace("MODE_RADIOS", mode_radios))
    return HTMLResponse(html)


@app.get("/pairs")
def list_pairs(lon_min: Optional[float] = None, lon_max: Optional[float] = None,
               lat_min: Optional[float] = None, lat_max: Optional[float] = None):
    """Return filtered pair labels for the sidebar dropdown."""
    pairs = get_pairs(lon_min=lon_min, lon_max=lon_max, lat_min=lat_min, lat_max=lat_max)
    return {"pairs": [p.label for p in pairs]}


@app.get("/map", response_class=HTMLResponse)
def serve_map(pair: str = Query(...), mode: str = Query(...),
              lon_min: Optional[float] = None, lon_max: Optional[float] = None,
              lat_min: Optional[float] = None, lat_max: Optional[float] = None):
    # Re-resolve from current PAIR_MAP (startup load), no need to re-query DB
    if pair not in PAIR_MAP:
        return HTMLResponse("<p>Unknown pair</p>", status_code=404)
    if mode not in BUILDERS:
        return HTMLResponse("<p>Unknown mode</p>", status_code=400)
    return HTMLResponse(BUILDERS[mode](PAIR_MAP[pair]))


@app.get("/map/all", response_class=HTMLResponse)
def serve_all_maps(mode: str = Query(...),
                   lon_min: Optional[float] = None, lon_max: Optional[float] = None,
                   lat_min: Optional[float] = None, lat_max: Optional[float] = None):
    """Render all filtered pairs, each in its own iframe."""
    if mode not in BUILDERS:
        return HTMLResponse("<p>Unknown mode</p>", status_code=400)
    pairs = get_pairs(lon_min=lon_min, lon_max=lon_max, lat_min=lat_min, lat_max=lat_max)
    if not pairs:
        return HTMLResponse(
            "<p style='color:gray;padding:20px;font-family:monospace;'>"
            "No pairs match the current filter.</p>"
        )

    def map_url(p):
        import urllib.parse
        params = "pair=" + urllib.parse.quote(p.label) + "&mode=" + mode
        if lon_min is not None: params += "&lon_min=" + str(lon_min)
        if lon_max is not None: params += "&lon_max=" + str(lon_max)
        if lat_min is not None: params += "&lat_min=" + str(lat_min)
        if lat_max is not None: params += "&lat_max=" + str(lat_max)
        return "/map?" + params

    sections = []
    for p in pairs:
        sections.append(
            '<section style="margin-bottom:16px;">'
            '<div style="background:#1f2937;color:#9ca3af;font-family:monospace;'
            'font-size:11px;padding:6px 12px;border-bottom:1px solid #374151;">'
            + p.label +
            '</div>'
            '<iframe src="' + map_url(p) + '" '
            'style="width:100%;height:600px;border:none;display:block;"></iframe>'
            '</section>'
        )

    return HTMLResponse(
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<style>'
        '* { box-sizing: border-box; margin: 0; padding: 0; }'
        'body { background: #0d1117; padding: 12px; }'
        '</style></head><body>'
        + "".join(sections) +
        '</body></html>'
    )