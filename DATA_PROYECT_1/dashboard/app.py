import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

# --- CONFIGURACIÓN ---
DB_USER = "admin"
DB_PASS = "admin"
# OJO: Asegúrate de que este nombre coincide con tu docker-compose (postgres o postgres_dp1)
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "calidad_aire"
DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Niveles definidos por tu equipo
LIMITE_NO2 = 100
MAX_GREEN_LEVEL = 120
MAX_YELLOW_LEVEL = 200

geolocator = Nominatim(user_agent="valencia_air_app")

# --- FUNCIONES DE DATOS ---

def get_latest_data():
    try:
        engine = create_engine(DATABASE_URI)
        query = """
            SELECT DISTINCT ON (nombre_estacion)
                nombre_estacion, indice_aqi, latitud, longitud, ingestion_time
            FROM clean_pollution
            ORDER BY nombre_estacion, ingestion_time DESC;
        """
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

def get_station_history(nombre_estacion):
    """Recupera histórico y corrige la zona horaria a Madrid"""
    try:
        engine = create_engine(DATABASE_URI)
        query = f"""
            SELECT * FROM (
                SELECT ingestion_time, indice_aqi
                FROM clean_pollution
                WHERE nombre_estacion = '{nombre_estacion}'
                ORDER BY ingestion_time DESC
                LIMIT 100
            ) sub
            ORDER BY ingestion_time ASC;
        """
        df = pd.read_sql(query, engine)

        # --- CORRECCIÓN DE HORA ---
        if not df.empty:
            df['ingestion_time'] = pd.to_datetime(df['ingestion_time'])
            if df['ingestion_time'].dt.tz is None:
                df['ingestion_time'] = df['ingestion_time'].dt.tz_localize('UTC')
            df['ingestion_time'] = df['ingestion_time'].dt.tz_convert('Europe/Madrid')

        return df
    except Exception:
        return pd.DataFrame()

def get_top_polluted():
    df = get_latest_data()
    if df.empty: return df
    return df.sort_values(by='indice_aqi', ascending=False).head(5)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.MINTY])

# --- VISUALES ---

navbar = dbc.NavbarSimple(
    children=[
        html.Div(
            [
                html.Div("Carlos Beltrán", className="m-0"),
                html.Div("Celia Sarrió", className="m-0"),
                html.Div("Iñaki Buj", className="m-0"),
                html.Div("Jorge Martínez", className="m-0"),
            ],
            style={'textAlign': 'right', 'color': 'white', 'fontSize': '0.85rem', 'lineHeight': '1.2'}
        )
    ],
    brand="🌿 Valencia Smart City | Air Control",
    brand_href="#",
    color="primary",
    dark=True,
    className="mb-4 shadow"
)

search_card = dbc.Card(
    [
        dbc.CardHeader("🔎 Inspector de Calles"),
        dbc.CardBody(
            [
                html.P("Verifica la seguridad del aire en tu ubicación exacta.", className="text-muted"),
                dbc.InputGroup(
                    [
                        dbc.Input(id="input-calle", placeholder="Ej: Plaza de la Reina...", type="text"),
                        dbc.Button("Analizar", id="btn-buscar", color="dark", n_clicks=0),
                    ],
                    className="mb-3",
                ),
                html.Div(id="resultado-alerta"),
                dcc.Graph(id='grafico-detalle', style={'display': 'none'})
            ]
        ),
    ],
    className="shadow-sm h-100"
)

map_card = dbc.Card(
    [
        dbc.CardHeader("🗺️ Mapa de Calor en Tiempo Real"),
        dbc.CardBody(
            dcc.Graph(id='mapa-valencia', style={'height': '400px'})
        ),
    ],
    className="shadow-sm h-100"
)

trend_card = dbc.Card(
    [
        dbc.CardHeader(id="titulo-tendencia", children="📊 Ranking de Contaminación (Top 5)"),
        dbc.CardBody(
            dcc.Graph(id='grafico-tendencia', style={'height': '300px'})
        ),
    ],
    className="shadow-sm"
)

# --- LAYOUT VERTICAL ---
app.layout = dbc.Container(
    [
        navbar,
        dcc.Store(id='estacion-seleccionada-store'),
        dcc.Store(id='ubicacion-usuario-store'),

        dbc.Row([dbc.Col(search_card, md=8, className="offset-md-2")], className="mb-4"),
        dbc.Row([dbc.Col(map_card, md=12)], className="mb-4"),
        dbc.Row([dbc.Col(trend_card, md=12)]),

        html.Div("Dashboard de Ingeniería de Datos", className="text-center text-muted my-4"),
        dcc.Interval(id='interval-component', interval=60*1000, n_intervals=0)
    ],
    fluid=True,
    className="bg-light"
)

# --- CALLBACKS ---

# 1. Buscador
@app.callback(
    [Output('resultado-alerta', 'children'),
     Output('grafico-detalle', 'figure'),
     Output('grafico-detalle', 'style'),
     Output('estacion-seleccionada-store', 'data'),
     Output('ubicacion-usuario-store', 'data')],
    Input('btn-buscar', 'n_clicks'),
    State('input-calle', 'value')
)
def check_street_alert(n_clicks, calle):
    empty_style = {'display': 'none'}
    empty_fig = {}

    if n_clicks == 0 or not calle:
        return "", empty_fig, empty_style, None, None

    df = get_latest_data()
    if df.empty: return dbc.Alert("⚠️ Sin datos.", color="warning"), empty_fig, empty_style, None, None

    try:
        location = geolocator.geocode(f"{calle}, Valencia, Spain")
        if not location: return dbc.Alert("❌ Dirección no encontrada.", color="danger"), empty_fig, empty_style, None, None
        user_coords = (location.latitude, location.longitude)
    except:
        return dbc.Alert("Error mapas.", color="danger"), empty_fig, empty_style, None, None

    min_dist = float('inf')
    nearest_station = None

    for _, row in df.iterrows():
        if pd.notnull(row['latitud']):
            dist = geodesic(user_coords, (row['latitud'], row['longitud'])).km
            if dist < min_dist:
                min_dist = dist
                nearest_station = row

    if nearest_station is not None:
        nivel = nearest_station['indice_aqi']
        nombre = nearest_station['nombre_estacion']
        dist_txt = f"{min_dist:.2f} km"

        color_alert = "danger" if nivel > MAX_YELLOW_LEVEL else "success"
        mensaje = "🚨 PELIGRO" if nivel > MAX_YELLOW_LEVEL else "✅ AIRE LIMPIO"
        alert = dbc.Alert([html.H5(f"{mensaje} en {nombre}"), html.P(f"Distancia: {dist_txt}")], color=color_alert)

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=nivel,
            number={'font': {'size': 50}, 'suffix': " µg/m³"},
            title={'text': "Nivel NO2", 'font': {'size': 20, 'color': 'gray'}},
            gauge={
                'axis': {'range': [None, 300], 'tickwidth': 1},
                'bar': {'color': "#2c3e50"},
                'bgcolor': "white",
                'borderwidth': 0,
                'steps': [
                    {'range': [0, MAX_GREEN_LEVEL], 'color': "#abebc6"},
                    {'range': [MAX_GREEN_LEVEL, MAX_YELLOW_LEVEL], 'color': "#dfe21d"},
                    {'range': [MAX_YELLOW_LEVEL, 300], 'color': "#fadbd8"}
                ],
                'threshold': {'line': {'color': "#e74c3c", 'width': 4}, 'thickness': 0.8, 'value': MAX_GREEN_LEVEL}
            }
        ))

        fig.update_layout(
            height=300,
            margin=dict(l=30, r=30, t=50, b=50),
            paper_bgcolor="rgba(0,0,0,0)",
            font={'family': "Arial"}
        )

        return alert, fig, {'display': 'block'}, nombre, user_coords

    return dbc.Alert("Error.", color="warning"), empty_fig, empty_style, None, None

# 2. Mapa
@app.callback(
    Output('mapa-valencia', 'figure'),
    [Input('interval-component', 'n_intervals'),
     Input('estacion-seleccionada-store', 'data'),
     Input('ubicacion-usuario-store', 'data')]
)
def update_map(n, estacion_seleccionada, ubicacion_usuario):
    df_map = get_latest_data()

    lat_center, lon_center = 39.4699, -0.3763
    zoom_level = 9

    if ubicacion_usuario:
        lat_center, lon_center = ubicacion_usuario
        zoom_level = 13

    if df_map.empty:
        return px.scatter_mapbox(
            lat=[lat_center], lon=[lon_center], zoom=zoom_level, mapbox_style="open-street-map"
        )

    def clasificar_estado(x):
        if x > MAX_YELLOW_LEVEL: return 'Peligro'
        return 'Bueno'

    df_map['Estado'] = df_map['indice_aqi'].apply(clasificar_estado)

    if estacion_seleccionada:
        df_map.loc[df_map['nombre_estacion'] == estacion_seleccionada, 'Estado'] = 'Estación Cercana'

    df_map['tamano_visual'] = df_map['indice_aqi'].fillna(0) + 25

    mapa_colores = {
        'Bueno': '#2ecc71',
        'Peligro': '#e74c3c',
        'Estación Cercana': '#0000FF'
    }

    fig_map = px.scatter_mapbox(
        df_map,
        lat="latitud",
        lon="longitud",
        color="Estado",
        size="tamano_visual",
        hover_name="nombre_estacion",
        hover_data={"indice_aqi": True, "tamano_visual": False, "latitud": False, "longitud": False},
        color_discrete_map=mapa_colores,
        zoom=zoom_level,
        center={"lat": lat_center, "lon": lon_center},
        mapbox_style="open-street-map"
    )

    if ubicacion_usuario:
        fig_map.add_trace(go.Scattermapbox(
            lat=[ubicacion_usuario[0]],
            lon=[ubicacion_usuario[1]],
            mode='markers+text',
            marker=go.scattermapbox.Marker(size=25, color='red', opacity=1),
            name="TU POSICIÓN",
            text=["📍 TÚ"],
            textposition="top center",
            textfont=dict(size=14, color='black'),
            hoverinfo='text'
        ))

    fig_map.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        mapbox=dict(center={"lat": lat_center, "lon": lon_center}, zoom=zoom_level, style="open-street-map")
    )

    return fig_map

# 3. Tendencias
@app.callback(
    [Output('grafico-tendencia', 'figure'),
     Output('titulo-tendencia', 'children')],
    [Input('estacion-seleccionada-store', 'data'),
     Input('interval-component', 'n_intervals')]
)
def update_trend_graph(estacion_seleccionada, n):
    if estacion_seleccionada:
        df_hist = get_station_history(estacion_seleccionada)
        if df_hist.empty:
            return px.line(title="Sin datos"), f"Histórico: {estacion_seleccionada}"

        fig = px.area(df_hist, x='ingestion_time', y='indice_aqi', markers=True, title="")
        fig.add_hline(y=MAX_YELLOW_LEVEL, line_dash="dash", line_color="red", annotation_text="Límite Tóxico")
        fig.update_traces(line_color='#3498db', fillcolor="rgba(52, 152, 219, 0.2)")
        fig.update_layout(
            margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
            yaxis_title="Nivel NO2"
        )
        return fig, f"📉 Evolución de contaminación en: {estacion_seleccionada}"

    else:
        df_top = get_top_polluted()
        if df_top.empty:
            return px.bar(title="Cargando..."), "📊 Ranking"

        # --- CAMBIO AQUÍ: Escala gradiente de VERDES ---
        fig = px.bar(
            df_top,
            x='indice_aqi',
            y='nombre_estacion',
            orientation='h',
            text='indice_aqi',
            # Usamos el valor numérico para el color
            color='indice_aqi',
            # Usamos la escala continua de verdes
            color_continuous_scale='Greens'
        )

        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            margin=dict(l=20, r=20, t=20, b=20),
            paper_bgcolor="rgba(0,0,0,0)"
        )

        return fig, "📊 Top 5 Barrios más contaminados ahora mismo"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=True)
