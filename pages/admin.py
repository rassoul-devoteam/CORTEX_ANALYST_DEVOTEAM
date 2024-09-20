import streamlit as st
import pandas as pd
import snowflake.snowpark.functions as F
from snowflake.snowpark.context import get_active_session
from datetime import datetime
from PIL import Image
import io

def main():
    # Fonction pour charger une image depuis un stage Snowflake
    def load_image_from_snowflake(stage_path):
        session = get_active_session()
        try:
            with session.file.get_stream(stage_path) as file_stream:
                image_data = file_stream.read()
                return Image.open(io.BytesIO(image_data))
        except Exception as e:
            st.error(f"Erreur lors du chargement de l'image : {e}")
            return None

    # Fonction pour charger les données d'une table
    def load_table_data(table_name):
        session = get_active_session()
        return session.table(f"CORTEX_DB.PUBLIC.{table_name}").to_pandas()

    # Fonction pour insérer un nouveau signet dans la table
    def insert_bookmark(app_id, username, question, lang):
        session = get_active_session()
        query = f"""
        INSERT INTO CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS (APP_ID, BK_USERNAME, BK_CREATED_AT, BK_UPDATED_AT, BK_QUESTION, BK_LANG)
        VALUES ({app_id}, '{username}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{question}', '{lang}')
        """
        session.sql(query).collect()
        st.success(f"Signet ajouté avec succès pour {username} !")

    # Fonction pour mettre à jour une ligne dans une table
    def update_table_row(table_name, primary_key, primary_key_value, column, new_value):
        session = get_active_session()
        table = session.table(f"CORTEX_DB.PUBLIC.{table_name}")
        table.update(
            {column: new_value},
            (F.col(primary_key) == primary_key_value)
        )

    # Fonction pour afficher un message spécifique après modification
    def show_update_message(column, value):
        message = f"{column} a été activé." if value else f"{column} a été désactivé."
        st.info(message)

    # Fonction pour afficher les détails de l'application de manière plus attrayante
    def display_app_details(app, logo_image):
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if logo_image:
                st.image(logo_image, width=100)
            else:
                st.write("Logo non disponible")
        
        with col2:
            st.subheader(app['APP_NAME'])
            st.write(f"**URL:** {app['APP_URL']}")
            st.write(f"**Base de données:** {app['APP_DATABASE']}")
            st.write(f"**Schéma:** {app['APP_SCHEMA']}")
        
        with col3:
            status_color = "green" if app['APP_ACTIVE'] else "red"
            st.markdown(f"**Statut:** <span style='color:{status_color};'>{'Actif' if app['APP_ACTIVE'] else 'Inactif'}</span>", unsafe_allow_html=True)
            st.write(f"**Rôle d'accès:** {app['APP_ACCESS_ROLE']}")

    # Fonction pour ajouter une nouvelle application
    def add_new_app():
        st.subheader("➕ Ajouter une nouvelle application")
        with st.form(key="add_app_form"):
            new_name = st.text_input("Nom de l'application")
            new_logo_url = st.text_input("URL du logo")
            new_app_url = st.text_input("URL de l'application")
            new_active = st.checkbox("Actif", value=True)
            new_access_role = st.text_input("Rôle d'accès")
            new_database = st.text_input("Base de données")
            new_schema = st.text_input("Schéma")
            new_stage = st.text_input("Stage")
            
            if st.form_submit_button("Ajouter l'application"):
                session = get_active_session()
                new_app_id = session.sql("SELECT COALESCE(MAX(APP_ID), 0) + 1 AS new_id FROM CORTEX_DB.PUBLIC.CORTEX_APPS").collect()[0]['NEW_ID']
                
                session.table("CORTEX_DB.PUBLIC.CORTEX_APPS").insert([
                    F.lit(new_app_id), F.lit(new_name), F.lit(new_logo_url), F.lit(new_app_url),
                    F.lit(new_active), F.lit(new_access_role), F.lit(new_database), F.lit(new_schema), F.lit(new_stage)
                ])
                
                st.success(f"Nouvelle application '{new_name}' ajoutée avec succès!")
                st.experimental_rerun()

    # Nouvelles fonctions pour la gestion des signets
    def load_bookmarks(app_id):
        session = get_active_session()
        query = f"""
        SELECT BK_ID, APP_ID, BK_USERNAME, BK_QUESTION, BK_LANG, BK_CREATED_AT, BK_UPDATED_AT
        FROM CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
        WHERE APP_ID = {app_id}
        ORDER BY BK_UPDATED_AT DESC
        """
        return session.sql(query).to_pandas()

    def delete_bookmark(bookmark_id):
        session = get_active_session()
        try:
            query = f"""
            DELETE FROM CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
            WHERE BK_ID = {bookmark_id}
            """
            session.sql(query).collect()
            return True
        except Exception as e:
            st.error(f"Erreur lors de la suppression du signet: {str(e)}")
            return False

    def update_bookmark(bookmark_id, new_question, new_lang):
        session = get_active_session()
        try:
            query = f"""
            UPDATE CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
            SET BK_QUESTION = '{new_question}', BK_LANG = '{new_lang}', BK_UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE BK_ID = {bookmark_id}
            """
            session.sql(query).collect()
            return True
        except Exception as e:
            st.error(f"Erreur lors de la mise à jour du signet: {str(e)}")
            return False

    # Style général avec CSS personnalisé
    st.markdown(
        """
        <style>
        .stTabs > div > div {
            font-size: 24px;
        }
        .stTextInput>div>div>input {
            border: 2px solid #1E90FF;
            border-radius: 10px;
            padding: 5px;
        }
        .stTextInput>div>div>input:focus {
            outline: none;
            border-color: #4682B4;
        }
        .stMarkdown h1, h2, h3 {
            color: #1E90FF;
        }
        .stAlert>div {
            border-radius: 10px;
            padding: 10px;
            background-color: #4682B4;
            color: white;
            font-weight: bold;
        }
        </style>
        """, 
        unsafe_allow_html=True
    )

    # Titre de la page d'administration
    st.title("🎛️ Page d'administration Cortex")

    # Chargement et filtrage des données d'applications
    apps_data = load_table_data("CORTEX_APPS")
    apps_data_filtered = apps_data[~apps_data['APP_ID'].isin([4, 5])]

    # Création des onglets principaux
    tab1, tab2, tab3 = st.tabs(["🤖Liste des applications", "➕Ajouter une application", "🔖 Signets"])

    with tab1:
        st.header("Applications Cortex")
        
        for _, app in apps_data_filtered.iterrows():
            with st.container():
                st.markdown("---")
                logo_image = load_image_from_snowflake(app["APP_LOGO_URL"])
                display_app_details(app, logo_image)
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Détails", key=f"details_button_{app['APP_ID']}"):
                        st.session_state[f"show_details_{app['APP_ID']}"] = True
                with col2:
                    if st.button("Modifier", key=f"modify_button_{app['APP_ID']}"):
                        st.session_state[f"show_modify_{app['APP_ID']}"] = True
                
                if st.session_state.get(f"show_details_{app['APP_ID']}", False):
                    with st.expander("Détails de l'application", expanded=True):
                        st.json(app.to_dict())
                        
                        st.markdown("---")
                        st.subheader("📊 Gestion des modèles")
                        st.markdown("Contrôlez l'état actif de chaque modèle.")

                        models_data = load_table_data("CORTEX_MODELS")
                        app_models = models_data[models_data['APP_ID'] == app['APP_ID']]

                        for index, model in app_models.iterrows():
                            model_row = f"Modèle: {model['CORTEX_YAML_NAME']}"

                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.write(model_row)
                            with col2:
                                new_active = st.radio(f"Statut {model['CORTEX_YAML_NAME']}", options=[True, False], format_func=lambda x: "Actif" if x else "Inactif", key=f"radio_model_{index}_{app['APP_ID']}")
                                if new_active != model['CORTEX_YAML_ACTIVE']:
                                    update_table_row("CORTEX_MODELS", "CORTEX_YAML_NAME", model['CORTEX_YAML_NAME'], "CORTEX_YAML_ACTIVE", new_active)
                                    show_update_message(f"Statut du modèle {model['CORTEX_YAML_NAME']}", new_active)

                if st.session_state.get(f"show_modify_{app['APP_ID']}", False):
                    with st.form(key=f"modify_form_{app['APP_ID']}"):
                        st.subheader("Modifier l'application")
                        new_name = st.text_input("Nom de l'application", app['APP_NAME'])
                        new_logo_url = st.text_input("URL du logo", app['APP_LOGO_URL'])
                        new_app_url = st.text_input("URL de l'application", app['APP_URL'])
                        new_active = st.checkbox("Actif", app['APP_ACTIVE'])
                        new_access_role = st.text_input("Rôle d'accès", app['APP_ACCESS_ROLE'])
                        new_database = st.text_input("Base de données", app['APP_DATABASE'])
                        new_schema = st.text_input("Schéma", app['APP_SCHEMA'])
                        new_stage = st.text_input("Stage", app['APP_STAGE'])
                        
                        if st.form_submit_button("Sauvegarder les modifications"):
                            fields_to_update = {
                                "APP_NAME": new_name,
                                "APP_LOGO_URL": new_logo_url,
                                "APP_URL": new_app_url,
                                "APP_ACTIVE": new_active,
                                "APP_ACCESS_ROLE": new_access_role,
                                "APP_DATABASE": new_database,
                                "APP_SCHEMA": new_schema,
                                "APP_STAGE": new_stage
                            }
                            
                            for field, new_value in fields_to_update.items():
                                if new_value != app[field]:
                                    update_table_row("CORTEX_APPS", "APP_ID", app['APP_ID'], field, new_value)
                            
                            st.success("Modifications sauvegardées avec succès!")
                            st.experimental_rerun()

    with tab2:
        add_new_app()

    with tab3:
        st.subheader("🔖 Signets des applications")
        
        # Sélection de l'application pour afficher les signets
        selected_app_id = st.selectbox("Sélectionnez une application", options=apps_data_filtered['APP_ID'], format_func=lambda x: apps_data_filtered[apps_data_filtered['APP_ID'] == x]['APP_NAME'].iloc[0])
        
        # Ajouter un nouveau signet
        st.markdown("---")
        st.subheader("➕ Ajouter un nouveau signet")
        
        with st.form(key="add_bookmark_form"):
            username = st.text_input("Nom d'utilisateur (BK_USERNAME)")
            question = st.text_input("Question (BK_QUESTION)")
            lang = st.text_input("Langue (BK_LANG)", value="FR")
            
            submit_button = st.form_submit_button(label="Ajouter")

            if submit_button:
                if username and question and lang:
                    insert_bookmark(selected_app_id, username, question, lang)
                    st.experimental_rerun()
                else:
                    st.error("Veuillez remplir tous les champs avant de soumettre.")

        # Afficher et gérer les signets existants
        st.markdown("---")
        st.subheader("Signets existants")
        
        bookmarks = load_bookmarks(selected_app_id)
        if bookmarks.empty:
            st.info(f"Aucun signet trouvé pour l'application sélectionnée (APP_ID: {selected_app_id}).")
        else:
            for _, bookmark in bookmarks.iterrows():
                with st.expander(f"{bookmark['BK_QUESTION']} ({bookmark['BK_USERNAME']})"):
                    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                    with col1:
                        new_question = st.text_input("Question", value=bookmark['BK_QUESTION'], key=f"q_{bookmark['BK_ID']}")
                    with col2:
                        new_lang = st.text_input("Langue", value=bookmark['BK_LANG'], key=f"l_{bookmark['BK_ID']}")
                    with col3:
                        if st.button("Mettre à jour", key=f"update_{bookmark['BK_ID']}"):
                            if update_bookmark(bookmark['BK_ID'], new_question, new_lang):
                                st.success("Signet mis à jour avec succès!")
                                st.experimental_rerun()
                            else:
                                st.error("Erreur lors de la mise à jour du signet.")
                    with col4:
                        if st.button("Supprimer", key=f"delete_{bookmark['BK_ID']}"):
                            if delete_bookmark(bookmark['BK_ID']):
                                st.success("Signet supprimé avec succès!")
                                st.experimental_rerun()
                            else:
                                st.error("Erreur lors de la suppression du signet.")

        # Affichage de toutes les données de signets pour diagnostic (sans filtrage par APP_ID)
        if st.checkbox("Afficher toutes les données de signets"):
            st.write("Données de la table des signets (non filtrées) :")
            st.dataframe(load_table_data("CORTEX_BOOKMARKS"))

# Condition pour exécuter main() si le script est exécuté directement
if __name__ == "__main__":
    main()