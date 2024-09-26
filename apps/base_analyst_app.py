import _snowflake
import json
import streamlit as st
import time
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import pandas as pd
import hashlib
import logging

class BaseAnalystApp:
    def __init__(self, app_id):
        self.APP_ID = app_id
        self.setup_logging()
        self.load_app_config()
        self.FILES = self.fetch_yamls()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def load_app_config(self):
        session = get_active_session()
        query = f"""
            SELECT * 
            FROM CORTEX_DB.PUBLIC.CORTEX_APPS
            WHERE APP_ID={self.APP_ID}
        """
        results = session.sql(query).collect()
        row = results[0]
        self.APP_NAME = row['APP_NAME']  # Store the APP_NAME
        self.APP_TITLE = row['APP_NAME']  # Keep APP_TITLE for backwards compatibility
        self.DATABASE = row['APP_DATABASE']
        self.SCHEMA = row['APP_SCHEMA']
        self.STAGE = row['APP_STAGE']
        self.APP_LOGO_URL = row['APP_LOGO_URL']

    def log_to_snowflake(self, username, input_text, output_json, elapsed_time, resolution_time, yaml_file):
        session = get_active_session()
        session.sql("""
            INSERT INTO CORTEX_DB.PUBLIC.CORTEX_LOGS 
            (DateTime, Username, App_Name, App_ID, Yaml_File, input_text, output_json, elapsed_time, resolution_time)
            VALUES (?, CURRENT_USER(), ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(), 
            self.APP_NAME,  # Use APP_NAME instead of APP_TITLE
            self.APP_ID,
            yaml_file,
            input_text, 
            json.dumps(output_json),
            elapsed_time,
            resolution_time
        )).collect()

    def calculate_resolution_time(self, elapsed_time):
        return elapsed_time * 0.7

    def fetch_popular_questions(self):
        logging.info(f"fetch_popular_questions called in {__class__.__name__}")
        session = get_active_session()
        popular_questions_query = f"""
        SELECT input_text, COUNT(*) as question_count
        FROM CORTEX_DB.PUBLIC.CORTEX_LOGS
        WHERE App_ID = {self.APP_ID}
        GROUP BY input_text
        ORDER BY question_count DESC
        LIMIT 4
        """
        popular_questions = session.sql(popular_questions_query).collect()
        return [row['INPUT_TEXT'] for row in popular_questions]
    
    # @st.cache_data(ttl=3600)
    def fetch_key_questions(self):  # Ajoutez un underscore devant 'self'
        logging.info(f"fetch_key_questions called in {__class__.__name__}")
        session = get_active_session()
        user_bookmarks_query = f"""
        SELECT bk_question 
        FROM CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
        WHERE APP_ID = {self.APP_ID}
        AND BK_USERNAME = 'ALL'
        ORDER BY BK_UPDATED_AT DESC
        LIMIT 6
        """
        user_bookmarks = session.sql(user_bookmarks_query).collect()
        return [row['BK_QUESTION'] for row in user_bookmarks]
    
    def fetch_yamls(self):
        session = get_active_session()
        query = f"""
        SELECT *
        FROM CORTEX_DB.PUBLIC.CORTEX_MODELS
        WHERE APP_ID = {self.APP_ID}
        AND CORTEX_YAML_ACTIVE = 1
        """
        results = session.sql(query).collect()
        files = {}
        for row in results:
            files[row['CORTEX_YAML_NAME']] = row['CORTEX_YAML_FILE']
        return files
    
    def display_key_questions(self):
        logging.info(f"display_key_questions called in {self.__class__.__name__}")
        st.markdown("<h2 style='text-align: center;'>Comment puis-je vous aider aujourd'hui ?</h2>", unsafe_allow_html=True)
        key_questions = self.fetch_key_questions()
        with st.container(border=True):
            col1, col2 = st.columns(2)
            for i, question in enumerate(key_questions):
                if i % 2 == 0:
                    with col1:
                        if st.button(question, key=f"q_{i}_{hash(question)}"):
                            st.session_state.active_suggestion = question
                else:
                    with col2:
                        if st.button(question, key=f"q_{i}_{hash(question)}"):
                            st.session_state.active_suggestion = question

    def load_and_display_image(self):
        session = get_active_session()
        try:
            with session.file.get_stream(self.APP_LOGO_URL) as file_stream:
                image_data = file_stream.read()
                col1, col2, col3 = st.columns([1,2,1])
                with col2:
                    st.image(image_data, width=500)
            return self.APP_LOGO_URL
        except Exception as e:
            st.error(f"Erreur lors du chargement de l'image : {e}")
            return None

    def insert_bookmark_data(self, question, lang):
        logging.info(f"Tentative d'ajout d'un Bookmark : app_id={self.APP_ID}, question={question}, lang={lang}")
        session = get_active_session()
        try:
            query = """
            INSERT INTO CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS 
            (APP_ID, BK_USERNAME, BK_QUESTION, BK_LANG)
            VALUES (?, CURRENT_USER(), ?, ?)
            """
            result = session.sql(query, (self.APP_ID, question, lang)).collect()
            logging.info(f"Résultat de l'insertion du bookmark : {result}")
            return True
        except Exception as e:
            logging.error(f"Erreur lors de l'ajout du bookmark: {str(e)}")
            return False

    def add_bookmark_button(self, question, lang, message_index):
        logging.info(f"add_bookmark_button")
        question_hash = hashlib.md5(question.encode()).hexdigest()
        bookmark_button_key = f"add_bookmark_{message_index}_{question_hash}"
        
        bookmark_clicked = st.button("🔖", key=bookmark_button_key)
        
        if bookmark_clicked:
            logging.info(f"Button clicked")
            success = self.insert_bookmark_data(question, lang)
            if success:
                st.success("Question enregistrée dans vos favoris !")
                st.rerun() 
            else:
                st.error("Erreur lors de l'enregistrement du favori.")

    def display_user_bookmarks_and_popular_questions(self):
        st.sidebar.markdown("## Mes favoris ❤️")
        bookmarks = self.fetch_user_bookmarks()
        if not bookmarks:
            st.sidebar.info("Vous n'avez pas encore de favoris.")
        else:
            for index, bookmark in enumerate(bookmarks):
                col1, col2, col3 = st.sidebar.columns([3, 1, 1])
                with col1:
                    if st.button(bookmark, key=f"bookmark_button_{index}_{hash(bookmark)}"):
                        st.session_state.active_suggestion = bookmark
                with col2:
                    if st.button("✏️", key=f"edit_bookmark_{index}_{hash(bookmark)}"):
                        st.session_state.editing_bookmark = bookmark
                        st.session_state.editing_bookmark_index = index
                with col3:
                    if st.button("🗑️", key=f"delete_bookmark_{index}_{hash(bookmark)}"):
                        self.delete_bookmark(bookmark)
                        st.rerun()

        if 'editing_bookmark' in st.session_state:
            self.edit_bookmark_ui()

        st.sidebar.markdown("## Questions populaires ✨")
        popular_questions = self.fetch_popular_questions()
        for index, question in enumerate(popular_questions):
            if st.sidebar.button(question, key=f"popular_question_{index}_{hash(question)}"):
                st.session_state.active_suggestion = question

    def edit_bookmark_ui(self):
        st.sidebar.markdown("### Modifier le favori")
        new_bookmark = st.sidebar.text_input("Nouveau texte", value=st.session_state.editing_bookmark)
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("Enregistrer", key="save_edit_bookmark"):
                self.update_bookmark(st.session_state.editing_bookmark, new_bookmark)
                del st.session_state.editing_bookmark
                del st.session_state.editing_bookmark_index
                st.rerun()
        with col2:
            if st.button("Annuler", key="cancel_edit_bookmark"):
                del st.session_state.editing_bookmark
                del st.session_state.editing_bookmark_index
                st.rerun()

    def update_bookmark(self, old_bookmark, new_bookmark):
        session = get_active_session()
        update_query = f"""
        UPDATE CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
        SET BK_QUESTION = ?, BK_UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE APP_ID = {self.APP_ID}
        AND BK_USERNAME = CURRENT_USER()
        AND BK_QUESTION = ?
        """
        session.sql(update_query, (new_bookmark, old_bookmark)).collect()
        logging.info(f"Favori mis à jour : '{old_bookmark}' -> '{new_bookmark}'")

    def delete_bookmark(self, question):
        session = get_active_session()
        delete_query = f"""
        DELETE FROM CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
        WHERE APP_ID = {self.APP_ID}
        AND BK_USERNAME = CURRENT_USER()
        AND BK_QUESTION = ?
        """
        session.sql(delete_query, (question,)).collect()

    def fetch_user_bookmarks(self):
        logging.info(f"fetch_user_bookmarks called in {__class__.__name__}")
        session = get_active_session()
        user_bookmarks_query = f"""
        SELECT BK_QUESTION 
        FROM CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS
        WHERE APP_ID = {self.APP_ID}
        AND BK_USERNAME = CURRENT_USER()
        ORDER BY BK_UPDATED_AT DESC
        """
        user_bookmarks = session.sql(user_bookmarks_query).collect()
        return [row['BK_QUESTION'] for row in user_bookmarks]

    def insert_vote_data(self, question, yaml_file, vote_value):
        logging.info(f"Tentative d'ajout d'un vote : app_id={self.APP_ID}, question={question}, vote_value={vote_value}")
        session = get_active_session()
        try:
            query = """
            INSERT INTO CORTEX_DB.PUBLIC.CORTEX_VOTES 
            (VOTE_USERNAME, QUESTION_TEXT, YAML_FILE, VOTE_VALUE)
            VALUES (CURRENT_USER(), ?, ?, ?)
            """
            session.sql(query, (question, yaml_file, vote_value)).collect()
            logging.info("Vote inséré avec succès")
            return True
        except Exception as e:
            logging.error(f"Erreur lors de l'ajout du vote: {str(e)}")
            return False

    def add_vote_button_up(self, question, yaml_file, message_index):
        logging.info(f"add_vote_buttons")
        question_hash = hashlib.md5(question.encode()).hexdigest()
        like_button_key = f"like_{message_index}_{question_hash}"
        if st.button("👍", key=like_button_key):
            success = self.insert_vote_data(question, yaml_file, 1)
            if success:
                st.success("Vous avez aimé cette réponse !")
            else:
                st.error("Erreur lors de l'enregistrement du vote positif.")

    def add_vote_button_down(self, question, yaml_file, message_index):
        logging.info(f"add_vote_buttons")
        question_hash = hashlib.md5(question.encode()).hexdigest()
        dislike_button_key = f"dislike_{message_index}_{question_hash}"
        if st.button("👎", key=dislike_button_key):
            success = self.insert_vote_data(question, yaml_file, -1)
            if success:
                st.success("Vous n'avez pas aimé cette réponse. Merci pour votre feedback !")
            else:
                st.error("Erreur lors de l'enregistrement du vote négatif.")

    def add_feedback_buttons(self, question, lang, yaml_file, message_index):
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            self.add_bookmark_button(question, lang, message_index)
        with col2:
            self.add_vote_button_up(question, yaml_file, message_index)
        with col3:
            self.add_vote_button_down(question, yaml_file, message_index)

    def display_content(self, content: list, message_index: int = None, prompt: str = None, yaml_file: str = None):
        message_index = message_index or len(st.session_state.messages)
        for item in content:
            if item["type"] == "text":
                st.markdown(item["text"])
                self.add_feedback_buttons(prompt, "FR", yaml_file, message_index)
            elif item["type"] == "suggestions":
                with st.expander("Suggestions", expanded=True):
                    for suggestion_index, suggestion in enumerate(item["suggestions"]):
                        unique_key = f"suggestion_{message_index}_{suggestion_index}_{hash(suggestion)}"
                        if st.button(suggestion, key=unique_key):
                            st.session_state.active_suggestion = suggestion
                            st.experimental_rerun()
            elif item["type"] == "sql":
                with st.expander("Requête SQL", expanded=False):
                    st.code(item["statement"], language="sql")
                with st.expander("Résultats", expanded=True):
                    with st.spinner("Exécution de la requête SQL..."):
                        session = get_active_session()
                        df = session.sql(item["statement"]).to_pandas()
                        if not df.empty:
                            data_tab, line_tab, bar_tab = st.tabs(
                                ["Données", "Graphique en ligne", "Graphique en barres"]
                            )
                            data_tab.dataframe(df)
                            if len(df.columns) > 1:
                                df = df.set_index(df.columns[0])
                                df_numeric = df.apply(pd.to_numeric, errors='coerce')
                                df_numeric = df_numeric.dropna(axis=1, how='all')
                                with line_tab:
                                    st.line_chart(df_numeric)
                                with bar_tab:
                                    st.bar_chart(df_numeric)
                            else:
                                st.info("Le DataFrame n'a pas assez de colonnes pour générer un graphique.")
                            col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                            with col1:
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    label="Télécharger les résultats en CSV",
                                    data=csv,
                                    file_name="resultats_requete.csv",
                                    mime="text/csv",
                                    key=f"download_{message_index}_{hash(df.to_string())}_{time.time()}"
                                )
                        else:
                            st.info("Aucun résultat trouvé pour cette requête.")

    def process_message(self, prompt: str):
        yaml_file = self.FILES[st.session_state.selected_model]

        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Génération de la réponse..."):
                response = self.send_message(prompt=prompt, yaml_file=yaml_file)
                if response:
                    content = response["message"]["content"]
                    self.display_content(content=content, prompt=prompt, yaml_file=yaml_file)
                    st.session_state.messages.append({"role": "assistant", "content": content})

    def send_message(self, prompt: str, yaml_file: str):
        session = get_active_session()
        start_time = time.time()
        request_body = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "semantic_model_file": f"@{self.DATABASE}.{self.SCHEMA}.{self.STAGE}/{yaml_file}",
        }
        try:
            resp = _snowflake.send_snow_api_request(
                "POST",
                f"/api/v2/cortex/analyst/message",
                {},
                {},
                request_body,
                {},
                30000,
            )
            elapsed_time = int((time.time() - start_time) * 1000)
            if resp["status"] < 400:
                output_json = json.loads(resp["content"])
                resolution_time = self.calculate_resolution_time(elapsed_time)
                self.log_to_snowflake(
                    username="",
                    input_text=prompt,
                    output_json=output_json,
                    elapsed_time=elapsed_time,
                    resolution_time=resolution_time, 
                    yaml_file=yaml_file
                )
                return output_json
            else:
                st.error(f"Erreur de l'API : {resp['status']} - {resp.get('content', 'Pas de détails')}")
                return None
        except Exception as e:
            st.error(f"Une erreur est survenue : {str(e)}")
            return None 

    def clear_chat_history(self):
        st.session_state.messages = []

    def run(self):
        if 'selected_model' not in st.session_state:
            st.session_state.selected_model = list(self.FILES.keys())[0] if self.FILES else None

        if 'messages' not in st.session_state:
            st.session_state.messages = []
        if 'suggestions' not in st.session_state:
            st.session_state.suggestions = []
        if 'active_suggestion' not in st.session_state:
            st.session_state.active_suggestion = None

        if 'editing_bookmark' not in st.session_state:
            st.session_state.editing_bookmark = None
        if 'editing_bookmark_index' not in st.session_state:
            st.session_state.editing_bookmark_index = None                    

        st.sidebar.button("Effacer l'historique du chat", on_click=self.clear_chat_history, key="clear_history_button")
        self.display_user_bookmarks_and_popular_questions()

        if self.FILES:
            # Sélection du modèle
            previous_model = st.session_state.selected_model
            st.session_state.selected_model = st.selectbox(
                "Choisissez un modèle sémantique",
                options=list(self.FILES.keys()),
                key="model_selector",
                index=list(self.FILES.keys()).index(st.session_state.selected_model) if st.session_state.selected_model in self.FILES else 0
            )

            if previous_model != st.session_state.selected_model:
                st.session_state.messages = []
                st.session_state.suggestions = []
                st.session_state.active_suggestion = None

            st.markdown(f"<h1 style='text-align: center; color: #FFFFFF;'>{st.session_state.selected_model} Model</h1>", unsafe_allow_html=True)
            
            self.load_and_display_image()
            self.display_key_questions()

            for message_index, message in enumerate(st.session_state.messages):
                with st.chat_message(message["role"]):
                    if message["role"] == "user":
                        st.markdown(message["content"][0]["text"])
                    else:
                        self.display_content(
                            content=message["content"],
                            message_index=message_index,
                            prompt=st.session_state.messages[message_index-1]["content"][0]["text"],
                            yaml_file=self.FILES[st.session_state.selected_model]
                        )

            if user_input := st.chat_input("Quelle est votre question ?"):
                self.process_message(prompt=user_input)

            if st.session_state.active_suggestion:
                self.process_message(prompt=st.session_state.active_suggestion)
                st.session_state.active_suggestion = None
        else:
            st.error("Aucun modèle sémantique disponible pour cette application.")

        logging.info(f"Fin de l'exécution. Modèle sélectionné: {st.session_state.selected_model}")
