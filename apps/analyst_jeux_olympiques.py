from apps.base_analyst_app import BaseAnalystApp
class AnalystJeuxOlympiques(BaseAnalystApp):
    def __init__(self):
        super().__init__(app_id=1)

def main():
    app = AnalystJeuxOlympiques()
    app.run()

if __name__ == "__main__":
    main()