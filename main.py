from app.watcherdog import start_watcher

if __name__ == "__main__":

    folder = r"\\10.101.18.251\quality_assurance_department\CMM"

    start_watcher(folder)