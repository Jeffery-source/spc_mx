import time
import threading
from queue import Queue
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class ExcelHandler(FileSystemEventHandler):

    def __init__(self, queue):
        self.queue = queue


    def add_file(self, file_path):

        if file_path.lower().endswith((".xlsx", ".xls")):
            self.queue.put(file_path)


    def on_created(self, event):

        if event.is_directory:
            return

        self.add_file(event.src_path)


    def on_modified(self, event):

        if event.is_directory:
            return

        self.add_file(event.src_path)



class FileProcessor(threading.Thread):

    def __init__(self, queue, callback, delay=3):
        super().__init__()

        self.queue = queue
        self.callback = callback
        self.delay = delay

        # 记录待处理文件时间
        self.pending = {}


    def run(self):

        while True:

            file_path = self.queue.get()

            # 更新最后修改时间
            self.pending[file_path] = time.time()


            # 延迟检查
            threading.Thread(
                target=self.wait_and_process,
                args=(file_path,)
            ).start()


    def wait_and_process(self, file_path):

        time.sleep(self.delay)


        # 如果3秒内没有新的修改
        if (
            time.time() - self.pending[file_path]
            >= self.delay
        ):

            try:
                self.callback(file_path)

            finally:
                del self.pending[file_path]



def start_file_watcher(folder, callback):

    queue = Queue()


    # 启动处理线程
    processor = FileProcessor(
        queue,
        callback,
        delay=3
    )

    processor.daemon = True
    processor.start()


    # 启动监听
    observer = Observer()

    handler = ExcelHandler(queue)


    observer.schedule(
        handler,
        folder,
        recursive=True
    )


    observer.start()


    print("开始监听:", folder)


    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        observer.stop()


    observer.join()