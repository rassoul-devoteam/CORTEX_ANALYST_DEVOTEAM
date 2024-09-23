import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

def main():
        # CSS personnalisé pour les ombres et autres styles
    st.markdown("""
    <style>
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
            font-size: 24px;
            font-weight: bold;
        }
        .custom-shadow {
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
            padding: 20px;
            margin-bottom: 20px;
            border-radius: 10px;
        }
        .kpi-box {
            background-color: #f0f2f6;
            border-radius: 15px;
            padding: 20px;
            text-align: center;
            margin-bottom: 20px;
        }
        .kpi-title {
            font-size: 24px;
            font-weight: 600;
            color: #6c757d;
        }
        .kpi-value {
            font-size: 48px;
            font-weight: bold;
            color: #212529;
        }
        .main-title {
            font-size: 36px;
            font-weight: 700;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 40px;
        }
    </style>
    """, unsafe_allow_html=True)

    # Fonction pour charger les données des logs depuis Snowflake
    @st.cache_data
    def load_log_data():
        session = get_active_session()
        df = session.sql("SELECT * FROM CORTEX_DB.PUBLIC.CORTEX_LOGS").to_pandas()
        df['output_parsed'] = df['OUTPUT_JSON'].apply(lambda x: json.loads(x) if pd.notnull(x) else {})
        json_df = pd.json_normalize(df['output_parsed'])
        json_df.columns = ['output_' + col for col in json_df.columns]
        df = pd.concat([df, json_df], axis=1)
        df['DATETIME'] = pd.to_datetime(df['DATETIME'])
        return df

    # Fonction pour charger les données des votes depuis Snowflake
    @st.cache_data
    def load_vote_data():
        session = get_active_session()
        vote_df = session.sql("SELECT * FROM CORTEX_DB.PUBLIC.CORTEX_VOTES").to_pandas()
        vote_df['VOTE_VALUE'] = vote_df['VOTE_VALUE'].astype(int)
        return vote_df

    # Fonction pour ajouter un nouveau bookmark
    def add_bookmark(app_id, question, lang="fr"):
        session = get_active_session()
        session.sql(f"""
            INSERT INTO CORTEX_DB.PUBLIC.CORTEX_BOOKMARKS (APP_ID, BK_USERNAME, BK_QUESTION, BK_LANG)
            VALUES ({app_id}, 'ALL', '{question}', '{lang}')
        """)

    # Charger les données
    df = load_log_data()
    vote_df = load_vote_data()

    # Récupérer les noms des applications
    apps = sorted(df['APP_NAME'].unique().tolist())

    # Onglets pour chaque application avec icône
    app_tabs = st.tabs([f"🤖 {app}" for app in apps])

    # Boucle sur chaque application pour afficher les logs et les votes
    for i, app in enumerate(apps):
        with app_tabs[i]:
            # Ajouter une section avec une ombre pour les logs
            st.markdown(f"<div class='custom-shadow'><h2>Logs pour {app}</h2></div>", unsafe_allow_html=True)
            
            # Filtrer les logs pour l'application sélectionnée
            app_df = df[df['APP_NAME'] == app]

            # Filtres pour les logs
            col1, col2 = st.columns(2)
            with col1:
                users = ['Tous'] + sorted(app_df['USERNAME'].unique().tolist())
                selected_user = st.selectbox('Filtrer par utilisateur', users, key=f'user_{app}')
            with col2:
                keyword = st.text_input('Rechercher un mot-clé', key=f'keyword_{app}')

            # Appliquer les filtres aux logs
            filtered_df = app_df.copy()
            if selected_user != 'Tous':
                filtered_df = filtered_df[filtered_df['USERNAME'] == selected_user]
            if keyword:
                columns_to_search = ['INPUT_TEXT', 'OUTPUT_JSON'] + [col for col in filtered_df.columns if col.startswith('output_')]
                filtered_df = filtered_df[columns_to_search].astype(str).apply(lambda x: x.str.contains(keyword, case=False)).any(axis=1)

            # Afficher les logs filtrés
            all_columns = filtered_df.columns.tolist()
            default_columns = ['DATETIME', 'USERNAME', 'APP_NAME', 'INPUT_TEXT', 'ELAPSED_TIME']
            output_columns = [col for col in all_columns if col.startswith('output_')]
            selected_columns = st.multiselect('Sélectionner les colonnes à afficher', all_columns, default=default_columns + output_columns[:5], key=f'columns_{app}')

            st.write(f"Nombre d'entrées : {len(filtered_df)}")
            st.dataframe(filtered_df[selected_columns])

            # Calculer le nombre de requêtes par utilisateur
            user_requests = filtered_df['USERNAME'].value_counts().reset_index()
            user_requests.columns = ['USERNAME', 'count']  # Renommer les colonnes correctement

            # Visualisation pour les logs
            st.subheader("Visualisations des Logs")
            
            # Trois indicateurs côte à côte
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Indicateur pour le total des requêtes
                total_requests = user_requests['count'].sum()  # Calculer le total des requêtes
                st.markdown(f"""
                <div class='kpi-box'>
                    <div class='kpi-value'>{total_requests}</div>
                    <div class='kpi-title'>Total Requêtes</div>
                </div>
                """, unsafe_allow_html=True)
                
            with col2:
                # Indicateur pour le temps d'exécution moyen (converti en secondes)
                avg_execution_time_sec = filtered_df['ELAPSED_TIME'].mean() / 1000  # Convertir de ms en secondes
                st.markdown(f"""
                <div class='kpi-box'>
                    <div class='kpi-value'>{avg_execution_time_sec:.2f} s</div>
                    <div class='kpi-title'>Temps d'exécution moyen</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                # Gérer les valeurs de RESOLUTION_TIME sans null
                if 'RESOLUTION_TIME' in filtered_df.columns:
                    avg_resolution_time = filtered_df['RESOLUTION_TIME'].mean() / 1000  # Convertir en secondes
                else:
                    avg_resolution_time = 0
            
                st.markdown(f"""
                <div class='kpi-box'>
                    <div class='kpi-value'>{avg_resolution_time:.2f} s</div>
                    <div class='kpi-title'>Temps de résolution moyen</div>
                </div>
                """, unsafe_allow_html=True)

            
            # Graphique du nombre de requêtes par utilisateur en bas
            st.subheader("Nombre de requêtes par utilisateur")
            fig_users = px.bar(user_requests, 
                            x='count', y='USERNAME', 
                            orientation='h',
                            labels={'USERNAME':'Utilisateur', 'count':'Nombre de requêtes'})
            st.plotly_chart(fig_users)

            # Graphique 3 : Évolution du nombre de requêtes dans le temps (courbe avec zone d'ombre)
            filtered_df['Date'] = filtered_df['DATETIME'].dt.date
            requests_over_time = filtered_df.groupby('Date').size().reset_index(name='count')
            fig_timeline = px.area(requests_over_time, 
                                x='Date', y='count', 
                                title="Évolution du nombre de requêtes dans le temps",
                                labels={'Date':'Date', 'count':'Nombre de requêtes'})
            st.plotly_chart(fig_timeline)

            # Option pour télécharger les logs filtrés
            if not filtered_df.empty:
                csv = filtered_df[selected_columns].to_csv(index=False)
                st.download_button(
                    label="Télécharger les données filtrées (CSV)",
                    data=csv,
                    file_name=f"cortex_logs_{app}.csv",
                    mime="text/csv",
                )

            # Ajouter une section avec une ombre pour les votes
            st.markdown(f"<div class='custom-shadow'><h2>Votes pour {app}</h2></div>", unsafe_allow_html=True)

            # Afficher les votes sans lien avec l'application pour l'instant
            filtered_vote_df = vote_df.copy()

            # Filtres pour les votes
            vote_users = ['Tous'] + sorted(filtered_vote_df['VOTE_USERNAME'].unique().tolist())
            selected_vote_user = st.selectbox('Filtrer par utilisateur (votes)', vote_users, key=f'vote_user_{app}')
            vote_keyword = st.text_input('Rechercher un mot-clé dans les questions', key=f'vote_keyword_{app}')

            # Appliquer les filtres aux votes
            if selected_vote_user != 'Tous':
                filtered_vote_df = filtered_vote_df[filtered_vote_df['VOTE_USERNAME'] == selected_vote_user]
            if vote_keyword:
                filtered_vote_df = filtered_vote_df[filtered_vote_df['QUESTION_TEXT'].str.contains(vote_keyword, case=False)]

            # Afficher les votes filtrés
            st.write(f"Nombre de votes : {len(filtered_vote_df)}")
            st.dataframe(filtered_vote_df)

            # Visualisations pour les votes
            # Graphique 1 : Répartition des votes positifs et négatifs (Graphique en anneau)
            vote_counts = filtered_vote_df['VOTE_VALUE'].value_counts().reset_index()
            vote_counts.columns = ['Vote', 'Count']
            
            # Utiliser un pie chart avec un trou au centre pour créer un graphique en anneau
            fig_votes = px.pie(vote_counts, 
                            values='Count', 
                            names='Vote', 
                            title="Répartition des votes 👍 et 👎",
                            hole=0.4,  # Ceci ajoute le trou pour créer l'effet 'donut'
                            color='Vote', 
                            color_discrete_map={1: 'lightblue', -1: 'darkred'})
            
            st.plotly_chart(fig_votes)

            # Graphique 2 : Top 10 des questions les plus votées (barres avec couleurs progressives)
            top_questions = filtered_vote_df.groupby('QUESTION_TEXT')['VOTE_VALUE'].count().sort_values(ascending=False).head(10)
            fig_top_questions = px.bar(top_questions, x=top_questions.index, y=top_questions.values,
                                    title="Top 10 des questions les plus votées",
                                    color=top_questions.values,
                                    color_continuous_scale='Blues')
            fig_top_questions.update_xaxes(tickangle=45, title_text="Question")
            fig_top_questions.update_yaxes(title_text="Nombre de votes")
            st.plotly_chart(fig_top_questions)

            # Graphique 3 : Évolution des votes dans le temps (courbe avec zone d'ombre)
            if 'VOTE_ID' in filtered_vote_df.columns:
                filtered_vote_df['Date'] = pd.to_datetime(filtered_vote_df['VOTE_ID']).dt.date
                votes_over_time = filtered_vote_df.groupby('Date').size().reset_index(name='count')
                fig_vote_timeline = px.area(votes_over_time, 
                                            x='Date', y='count', 
                                            title="Évolution du nombre de votes dans le temps",
                                            labels={'Date':'Date', 'count':'Nombre de votes'})
                st.plotly_chart(fig_vote_timeline)

            # Option pour télécharger les votes filtrés
            if not filtered_vote_df.empty:
                vote_csv = filtered_vote_df.to_csv(index=False)
                st.download_button(
                    label=f"Télécharger les données de votes filtrées (CSV)",
                    data=vote_csv,
                    file_name=f"cortex_votes_{app}.csv",
                    mime="text/csv",
                )

            # Bouton pour ajouter un bookmark
            question = st.text_input("Entrer la question pour le bookmark", key=f'bk_question_{app}')
            if st.button("Ajouter un bookmark", key=f'add_bookmark_{app}'):
                app_id = 1  # You can adjust this to pull from a variable or external data source
                add_bookmark(app_id, question)
                st.success(f"Bookmark ajouté pour {app} avec la question : {question}")
if __name__ == "__main__":
    main()