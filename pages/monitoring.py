import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

def main():
    # CSS personnalisé pour agrandir la taille de la police des onglets
    st.markdown("""
    <style>
        .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
            font-size: 24px;
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

    # Fonction pour charger les données des votes
    @st.cache_data
    def load_vote_data():
        session = get_active_session()
        vote_df = session.sql("SELECT * FROM CORTEX_DB.PUBLIC.CORTEX_VOTES").to_pandas()
        vote_df['VOTE_VALUE'] = vote_df['VOTE_VALUE'].astype(int)
        return vote_df

    # Chargement des données
    df = load_log_data()
    vote_df = load_vote_data()

    # Titre de l'application
    st.title('Monitoring CORTEX')

    # Onglets pour séparer les logs et les votes
    tab1, tab2 = st.tabs(["💹Logs", "👍👎Votes"])

    with tab1:
        st.header('Monitoring CORTEX_LOGS')
        
        # Filtres
        col1, col2, col3 = st.columns(3)
        with col1:
            users = ['Tous'] + sorted(df['USERNAME'].unique().tolist())
            selected_user = st.selectbox('Filtrer par utilisateur', users)
        with col2:
            apps = ['Toutes'] + sorted(df['APP_NAME'].unique().tolist())
            selected_app = st.selectbox('Filtrer par application', apps)
        with col3:
            keyword = st.text_input('Rechercher un mot-clé')

        # Application des filtres
        filtered_df = df.copy()
        if selected_user != 'Tous':
            filtered_df = filtered_df[filtered_df['USERNAME'] == selected_user]
        if selected_app != 'Toutes':
            filtered_df = filtered_df[filtered_df['APP_NAME'] == selected_app]
        if keyword:
            columns_to_search = ['INPUT_TEXT', 'OUTPUT_JSON'] + [col for col in filtered_df.columns if col.startswith('output_')]
            filtered_df = filtered_df[filtered_df[columns_to_search].astype(str).apply(lambda x: x.str.contains(keyword, case=False)).any(axis=1)]

        # Sélection des colonnes à afficher
        all_columns = filtered_df.columns.tolist()
        default_columns = ['DATETIME', 'USERNAME', 'APP_NAME', 'INPUT_TEXT', 'ELAPSED_TIME']
        output_columns = [col for col in all_columns if col.startswith('output_')]
        selected_columns = st.multiselect('Sélectionner les colonnes à afficher', all_columns, default=default_columns + output_columns[:5])

        # Affichage des données filtrées
        st.write(f"Nombre d'entrées : {len(filtered_df)}")
        st.dataframe(filtered_df[selected_columns])

        # Graphiques
        st.subheader("Visualisations")
        
        # Graphique 1: Nombre de requêtes par utilisateur
        fig_users = px.bar(filtered_df['USERNAME'].value_counts().reset_index(), x='USERNAME', y='count', title="Nombre de requêtes par utilisateur")
        st.plotly_chart(fig_users)

        # Graphique 2: Temps d'exécution moyen par application
        avg_time = filtered_df.groupby('APP_NAME')['ELAPSED_TIME'].mean().reset_index()
        fig_time = px.bar(avg_time, x='APP_NAME', y='ELAPSED_TIME', title="Temps d'exécution moyen par application")
        st.plotly_chart(fig_time)

        # Graphique 3: Évolution du nombre de requêtes dans le temps
        filtered_df['Date'] = filtered_df['DATETIME'].dt.date
        requests_over_time = filtered_df.groupby('Date').size().reset_index(name='count')
        fig_timeline = px.line(requests_over_time, x='Date', y='count', title="Évolution du nombre de requêtes dans le temps")
        st.plotly_chart(fig_timeline)

        # Option pour télécharger les données filtrées
        if not filtered_df.empty:
            csv = filtered_df[selected_columns].to_csv(index=False)
            st.download_button(
                label="Télécharger les données filtrées (CSV)",
                data=csv,
                file_name="cortex_logs_filtered.csv",
                mime="text/csv",
            )

    with tab2:
        st.header('Monitoring des Votes')
        
        # Filtres pour les votes
        col1, col2 = st.columns(2)
        with col1:
            vote_users = ['Tous'] + sorted(vote_df['VOTE_USERNAME'].unique().tolist())
            selected_vote_user = st.selectbox('Filtrer par utilisateur (votes)', vote_users)
        with col2:
            vote_keyword = st.text_input('Rechercher un mot-clé dans les questions')

        # Application des filtres
        filtered_vote_df = vote_df.copy()
        if selected_vote_user != 'Tous':
            filtered_vote_df = filtered_vote_df[filtered_vote_df['VOTE_USERNAME'] == selected_vote_user]
        if vote_keyword:
            filtered_vote_df = filtered_vote_df[filtered_vote_df['QUESTION_TEXT'].str.contains(vote_keyword, case=False)]

        # Affichage des données filtrées
        st.write(f"Nombre de votes : {len(filtered_vote_df)}")
        st.dataframe(filtered_vote_df)

        # Visualisations pour les votes
        st.subheader("Visualisations des Votes")

        # Graphique 1: Répartition des votes positifs et négatifs
        vote_counts = filtered_vote_df['VOTE_VALUE'].value_counts().reset_index()
        vote_counts.columns = ['Vote', 'Count']
        fig_votes = px.pie(vote_counts, values='Count', names='Vote', 
                        title="Répartition des votes positifs et négatifs",
                        color='Vote', color_discrete_map={1: 'green', -1: 'red'})
        st.plotly_chart(fig_votes)

        # Graphique 2: Top 10 des questions les plus votées
        top_questions = filtered_vote_df.groupby('QUESTION_TEXT')['VOTE_VALUE'].count().sort_values(ascending=False).head(10)
        fig_top_questions = px.bar(top_questions, x=top_questions.index, y=top_questions.values,
                                title="Top 10 des questions les plus votées")
        fig_top_questions.update_xaxes(tickangle=45, title_text="Question")
        fig_top_questions.update_yaxes(title_text="Nombre de votes")
        st.plotly_chart(fig_top_questions)

        # Graphique 3: Évolution des votes dans le temps
        if 'DATETIME' in filtered_vote_df.columns:
            filtered_vote_df['Date'] = pd.to_datetime(filtered_vote_df['DATETIME']).dt.date
            votes_over_time = filtered_vote_df.groupby('Date').size().reset_index(name='count')
            fig_vote_timeline = px.line(votes_over_time, x='Date', y='count', title="Évolution du nombre de votes dans le temps")
            st.plotly_chart(fig_vote_timeline)

        # Option pour télécharger les données filtrées des votes
        if not filtered_vote_df.empty:
            vote_csv = filtered_vote_df.to_csv(index=False)
            st.download_button(
                label="Télécharger les données de votes filtrées (CSV)",
                data=vote_csv,
                file_name="cortex_votes_filtered.csv",
                mime="text/csv",
            )
if __name__ == "__main__":
    main()