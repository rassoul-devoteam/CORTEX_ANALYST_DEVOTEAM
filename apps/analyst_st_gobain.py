from apps.base_analyst_app import BaseAnalystApp
class AnalystSaintGobain(BaseAnalystApp):
    def __init__(self):
        super().__init__(app_id=2)

def main():
    app = AnalystSaintGobain()
    app.run()

if __name__ == "__main__":
    main()