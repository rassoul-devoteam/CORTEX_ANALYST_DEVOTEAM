from apps.base_analyst_app import BaseAnalystApp
class AnalystWinterGames(BaseAnalystApp):
    def __init__(self):
        super().__init__(app_id=3)

def main():
    app = AnalystWinterGames()
    app.run()

if __name__ == "__main__":
    main()