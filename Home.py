import streamlit as st
import snowflake.snowpark.functions as F
from snowflake.snowpark.context import get_active_session
from PIL import Image
import io

# Import des applications spécifiques
from apps.analyst_jeux_olympiques import AnalystJeuxOlympiques
from apps.analyst_st_gobain import AnalystSaintGobain
from apps.analyst_winter_games import AnalystWinterGames
from pages import monitoring, admin

def load_image_from_snowflake(stage_path):
    session = get_active_session()
    try:
        with session.file.get_stream(stage_path) as file_stream:
            image_data = file_stream.read()
            return Image.open(io.BytesIO(image_data))
    except Exception as e:
        st.error(f"Erreur lors du chargement de l'image : {e}")
        return None

def load_css():
    st.markdown("""
    <style>
    .stImage {
        border-radius: 15px;
        margin-bottom: 10px;
        box-shadow: 2px 2px 5px rgba(0, 0, 0, 0.2);
    }
    .stButton>button {
        width: 100%;
        background-color: #1E90FF;
        color: white;
        padding: 10px;
        border-radius: 10px;
        border: none;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        background-color: #4682B4;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        transform: translateY(-2px);
    }
    .app-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

def main():
    st.set_page_config(page_title="Cortex Analyst Apps", layout="wide", page_icon="🏠")
    load_css()
    
    st.title("Cortex Analyst Apps")

    logo_width, logo_height = 200, 200
    session = get_active_session()

    df = session.table("CORTEX_DB.PUBLIC.CORTEX_APPS").filter(F.col("APP_ACTIVE") == True).to_pandas()
    df = df.sort_values(by='APP_ID')

    cols = st.columns(3)

    for i, (_, app) in enumerate(df.iterrows()):
        col = cols[i % 3]
        with col:
            with st.container():
                st.markdown('<div class="app-container">', unsafe_allow_html=True)
                logo_image = load_image_from_snowflake(app["APP_LOGO_URL"])
                if logo_image:
                    resized_image = logo_image.resize((logo_width, logo_height))
                    st.image(resized_image, use_column_width=False)
                if st.button(app['APP_NAME']):
                    st.session_state.selected_page = app["APP_URL"]
                st.markdown('</div>', unsafe_allow_html=True)

    if "selected_page" in st.session_state:
        try:
            page_mapping = {
                "analyst_winter_games": AnalystWinterGames,
                "analyst_st_gobain": AnalystSaintGobain,
                "analyst_jeux_olympiques": AnalystJeuxOlympiques,
                "monitoring": monitoring,
                "admin": admin
            }

            selected = st.session_state.selected_page
            if selected in page_mapping:
                if selected in ["monitoring", "admin"]:
                    # Pour les pages monitoring et admin, on appelle directement leur fonction main
                    page_mapping[selected].main()
                else:
                    # Pour les applications d'analyse, on instancie la classe et on appelle sa méthode run
                    app = page_mapping[selected]()
                    app.run()
            else:
                st.error("Page non trouvée")

        except Exception as e:
            st.error(f"Une erreur s'est produite: {e}")

if __name__ == "__main__":
    main()